<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# 🔬 Lean Squad — Formal Verification Research for Apache Iceberg

## Project Overview

**Repository**: Apache Iceberg  
**Language**: Java (Lean 4 models)  
**Purpose**: High-performance table format for analytical databases with engine compatibility (Spark, Flink, Hive, Trino, Presto, Impala)

Apache Iceberg is a mature, well-tested project with clear architectural boundaries (api/, core/, data/, Spark, Flink, cloud-specific modules). The codebase synthesizes 58,000+ review comments across 4,300+ merged PRs, establishing strong patterns for type safety, immutability, and invariant preservation.

## Formal Verification Approach

- **Tool**: Lean 4 + Mathlib
- **Strategy**: Identify pure functions, data structure invariants, and mathematical properties that are high-value targets for verification
- **Focus areas**:
  1. **Partition transforms**: Pure functions with well-defined semantics (Bucket, Identity, Truncate, date/time transforms)
  2. **Type system and schema operations**: Type equivalence, schema evolution, field ID uniqueness
  3. **Comparison and sorting**: Sort order semantics, comparator correctness, null ordering
  4. **Data structure invariants**: PartitionSpec, Schema, SortOrder internal consistency

## Identified FV Targets

### Priority 1: Partition Transform Functions

**Rationale**: Partition transforms are pure, deterministic functions critical to data partitioning correctness. They have well-defined semantics and mathematical properties. Bugs in bucketing or date truncation could cause data misplacement or correctness issues across engines.

#### Target 1a: Bucket Transform
- **File**: `api/src/main/java/org/apache/iceberg/transforms/Bucket.java`
- **Function**: `public Integer apply(T value)` → `(hash(value) & Integer.MAX_VALUE) % numBuckets`
- **Properties to verify**:
  - **Determinism**: Same input always produces same output
  - **Boundedness**: Result is always in range `[0, numBuckets)`
  - **Hash distribution**: Hash values are well-distributed (modulo modelling constraints)
  - **Null handling**: `null` input maps to `null` output
  - **Width correctness**: Hash masking with `Integer.MAX_VALUE` preserves distribution
  
- **Benefit**: Bugs in bucketing can cause incorrect data placement, leading to data loss or access failures
- **Spec size**: ~200 Lean lines (type system, hash properties, modulo arithmetic)
- **Proof tractability**: Routine arithmetic (`omega`, `decide`) for core properties; hash distribution requires abstraction
- **Approximations**: 
  - Model hash functions as abstract functions satisfying basic properties (finite output range, deterministic)
  - Don't model actual FNV/SHA hash algorithms; verify against hash abstraction instead
  - Model `Integer.MAX_VALUE` masking as bitwise AND operation

#### Target 1b: Identity Transform
- **File**: `api/src/main/java/org/apache/iceberg/transforms/Identity.java`
- **Function**: `public T apply(T value)` → `value`
- **Properties to verify**:
  - **Identity law**: `apply(x) = x` for all `x`
  - **Order preservation**: `satisfiesOrderOf(other)` iff `other.preservesOrder()`
  - **Result type invariant**: `getResultType(sourceType) = sourceType`
  
- **Benefit**: Simple but foundational; easy to verify and provides confidence in proof methodology
- **Spec size**: ~100 Lean lines
- **Proof tractability**: Trivial (by reflexivity, `rfl`)
- **Approximations**: None; this is pure functional identity

#### Target 1c: Truncate Transform (Strings)
- **File**: `api/src/main/java/org/apache/iceberg/transforms/Truncate.java` (String variant)
- **Function**: `public String apply(String value)` → `value.substring(0, length)`
- **Properties to verify**:
  - **Idempotence**: `truncate(truncate(x, L), L) = truncate(x, L)`
  - **Boundedness**: Result length ≤ input length
  - **Empty safety**: `truncate("", L) = ""`
  - **Null safety**: `truncate(null, L) = null`
  
- **Benefit**: String operations are ubiquitous; truncation is a common partitioning strategy
- **Spec size**: ~150 Lean lines
- **Proof tractability**: Routine string manipulation with `omega` for length bounds
- **Approximations**: Model strings as abstract sequences; don't verify actual Java string encoding

---

### Priority 2: Comparison and Sorting Correctness

**Rationale**: Comparator and sort order logic must be correct to guarantee sorted output. Bugs here could break range queries, duplicate detection, and data integrity across all query engines.

#### Target 2a: SortOrder Comparator
- **File**: `api/src/main/java/org/apache/iceberg/SortOrderComparators.java`
- **Function**: `public int compare(StructLike left, StructLike right)` (lexicographic comparison with transform)
- **Properties to verify**:
  - **Transitivity**: If `compare(a,b) < 0` and `compare(b,c) < 0`, then `compare(a,c) < 0`
  - **Antisymmetry**: If `compare(a,b) < 0`, then `compare(b,a) > 0`
  - **Reflexivity**: `compare(x, x) = 0`
  - **Total order**: For any a, b: exactly one of `a < b`, `a = b`, `a > b`
  
- **Benefit**: Sorting correctness is foundational for data reliability across engines
- **Spec size**: ~300 Lean lines (lexicographic product of comparators, transform composition)
- **Proof tractability**: Inductive on struct fields; requires showing composition of transitive relations is transitive
- **Approximations**: 
  - Model comparators abstractly (satisfy strict total order properties)
  - Model transform composition abstractly (function application)
  - Don't verify actual type-specific comparisons (handled as black boxes)

#### Target 2b: Null Ordering Semantics
- **File**: `api/src/main/java/org/apache/iceberg/types/Comparators.java`
- **Function**: `nullsFirst()` and `nullsLast()` adapter logic
- **Properties to verify**:
  - **Null placement**: `nullsFirst()` ensures `null` compares less than any non-null value
  - **Order preservation within non-nulls**: Non-null comparisons unchanged
  
- **Benefit**: Null handling bugs can cause incorrect sort order and range query failures
- **Spec size**: ~120 Lean lines
- **Proof tractability**: Case analysis on null; `omega` for comparisons
- **Approximations**: Model comparators as abstract total orders

---

### Priority 3: Schema and Type System Invariants

**Rationale**: Schema mutations (field addition, removal, type changes) are critical operations. Invariants like field ID uniqueness, nested field path validity, and type evolution rules must hold to preserve data interpretation.

#### Target 3a: Schema Field ID Uniqueness
- **File**: `api/src/main/java/org/apache/iceberg/Schema.java`
- **Invariant**: All nested fields have unique IDs; IDs are never reused
- **Property to verify**: After any schema update (add/remove/modify field), the unique ID property holds
  
- **Benefit**: Field ID collisions can cause data misinterpretation across engine versions
- **Spec size**: ~250 Lean lines (tree traversal, set construction)
- **Proof tractability**: Structural induction over nested schema tree
- **Approximations**: Model schema updates abstractly; don't verify actual table metadata persistence logic

#### Target 3b: PartitionSpec Completeness
- **File**: `api/src/main/java/org/apache/iceberg/PartitionSpec.java`
- **Invariant**: All partition fields refer to valid schema fields; no cycles or orphaned fields
- **Property to verify**: PartitionSpec is well-formed relative to its target Schema
  
- **Benefit**: Malformed partition specs can cause silent data corruption or access anomalies
- **Spec size**: ~200 Lean lines
- **Proof tractability**: Structural validation with set membership checks (`decide`)
- **Approximations**: Model field references as abstract pointers; don't verify file I/O

---

## Approach and Methodology

1. **Phase 1 — Research** (current): Identify targets, document properties, assess tractability
2. **Phase 2 — Informal Specification**: For highest-priority target, extract precise informal spec from code and tests
3. **Phase 3 — Formal Lean Specification**: Translate informal spec to Lean 4, state key theorems (bodies as `sorry`)
4. **Phase 4 — Implementation Model**: Translate target Java function to Lean functional model
5. **Phase 5 — Proofs**: Attempt proofs; report any counterexamples as bugs
6. **Phase 6 — Correspondence Validation** (if applicable): Use Aeneas (Java → LLBC → Lean) or runnable tests to validate model fidelity

## Expected Outcomes

- **Short-term** (first 2–3 runs): Prove properties of 1–2 partition transforms; establish proof methodology
- **Medium-term** (runs 3–6): Expand to comparator correctness, basic schema invariants
- **Long-term** (runs 6+): Comprehensive coverage of critical data structure invariants; identify and fix bugs if any

## Known Constraints and Modelling Decisions

- **Java-to-Lean translation**: Use functional subset of Java; model mutable state as immutable snapshots
- **Null semantics**: Model Java `null` as Lean `Option`
- **Hash functions**: Abstract to properties (deterministic, bounded range); don't model actual hash algorithms
- **Concurrency**: Out of scope; focus on single-threaded correctness
- **I/O and persistence**: Out of scope; focus on pure function behavior

## Open Questions for Maintainers

1. **Hash function abstraction**: Are there known hash function properties Iceberg relies on beyond determinism? (e.g., avalanche effect, FNV vs. other hash schemes)
2. **Type evolution rules**: Are there documented rules for schema evolution, or should we infer them from tests?
3. **Performance requirements**: Are there performance-critical paths where formal verification should focus? (e.g., comparison hot paths)
4. **Existing test coverage**: Can we reuse existing test cases as specification examples?

---

## References and Relevant Mathlib Modules

- `Mathlib.Data.List.Basic`: List operations, idempotence
- `Mathlib.Order.RelClasses`: Transitive, antisymmetric, reflexive relations
- `Mathlib.Order.Defs`: Total order, strict total order definitions
- `Mathlib.Data.Fintype.Basic`: Finite types (for bucket indices)
- `Mathlib.Algebra.GroupWithZero.Order`: Integer modulo arithmetic
- `Mathlib.Data.Nat.Mod.Properties`: Modular arithmetic properties

---

**Last Updated**: 2026-04-24 UTC  
**Author**: 🔬 Lean Squad  
