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

# 🔬 Formal Verification Targets — Apache Iceberg

## Phase Legend
- **Phase 1**: Research & Target Identification ✅
- **Phase 2**: Informal Specification Extraction (in progress)
- **Phase 3**: Lean 4 Formal Spec Writing
- **Phase 4**: Implementation Model Extraction
- **Phase 5**: Proof Assistance
- **Phase 6+**: Correspondence Validation, Critiques, etc.

## Priority 1: Identity Transform

| Property | Value |
|----------|-------|
| **Codename** | `identity-transform` |
| **File** | `api/src/main/java/org/apache/iceberg/transforms/Identity.java` |
| **Phase** | ⬜ Phase 2: Informal Spec |
| **Rationale** | Foundational, trivial proof (establishes methodology); builds confidence |
| **Spec Size** | ~100 Lean lines |
| **Tractability** | Trivial (by reflexivity) |
| **Key Properties** |  `apply(x) = x`, `getResultType(T) = T`, preserves order |
| **Expected Theorems** | 3–4 |
| **Status** | Ready for Phase 2 |

---

## Priority 2: Bucket Transform

| Property | Value |
|----------|-------|
| **Codename** | `bucket-transform` |
| **File** | `api/src/main/java/org/apache/iceberg/transforms/Bucket.java` |
| **Phase** | ⬜ Phase 2: Informal Spec |
| **Rationale** | Pure function, high value (data partitioning correctness), well-tested |
| **Spec Size** | ~250 Lean lines |
| **Tractability** | Routine arithmetic (`omega`, `decide` for boundedness) |
| **Key Properties** | `0 ≤ apply(x) < numBuckets`, determinism, null safety, distribution |
| **Expected Theorems** | 4–5 |
| **Status** | Ready for Phase 2; hash abstraction needs clarification |

---

## Priority 3: Truncate Transform (Strings)

| Property | Value |
|----------|-------|
| **Codename** | `truncate-transform` |
| **File** | `api/src/main/java/org/apache/iceberg/transforms/Truncate.java` |
| **Phase** | ⬜ Phase 2: Informal Spec |
| **Rationale** | Pure function, string operations, common partitioning strategy |
| **Spec Size** | ~150 Lean lines |
| **Tractability** | String manipulation + length bounds (`omega`) |
| **Key Properties** | `len(apply(x, L)) ≤ L`, idempotence, null safety |
| **Expected Theorems** | 3–4 |
| **Status** | Ready for Phase 2 |

---

## Priority 4: SortOrder Comparator

| Property | Value |
|----------|-------|
| **Codename** | `sortorder-comparator` |
| **File** | `api/src/main/java/org/apache/iceberg/SortOrderComparators.java` |
| **Phase** | ⬜ Phase 2: Informal Spec |
| **Rationale** | Sorting correctness is critical; total order property must hold |
| **Spec Size** | ~300 Lean lines |
| **Tractability** | Inductive + comparator abstraction; moderate complexity |
| **Key Properties** | Transitivity, antisymmetry, reflexivity, totality |
| **Expected Theorems** | 5–6 |
| **Depends On** | Null ordering semantics (Priority 5) |
| **Status** | Ready for Phase 2 after Priority 5 |

---

## Priority 5: Null Ordering Semantics

| Property | Value |
|----------|-------|
| **Codename** | `null-ordering` |
| **File** | `api/src/main/java/org/apache/iceberg/types/Comparators.java` |
| **Phase** | ⬜ Phase 2: Informal Spec |
| **Rationale** | Foundation for comparator correctness; relatively simple |
| **Spec Size** | ~120 Lean lines |
| **Tractability** | Case analysis on null; routine `omega` |
| **Key Properties** | Null placement invariant, non-null order preservation |
| **Expected Theorems** | 2–3 |
| **Status** | Ready for Phase 2; blocks Priority 4 |

---

## Priority 6: Schema Field ID Uniqueness

| Property | Value |
|----------|-------|
| **Codename** | `schema-field-id-uniqueness` |
| **File** | `api/src/main/java/org/apache/iceberg/Schema.java` |
| **Phase** | ⬜ Phase 2: Informal Spec |
| **Rationale** | Data structure invariant; ID collisions cause data misinterpretation |
| **Spec Size** | ~250 Lean lines |
| **Tractability** | Structural induction over nested schema tree |
| **Key Properties** | All nested fields have unique IDs; IDs never reused across mutations |
| **Expected Theorems** | 3–4 |
| **Dependencies** | Schema evolution operations (AddField, RemoveField, RenameField) |
| **Status** | Blocked pending schema mutation spec extraction |

---

## Priority 7: PartitionSpec Completeness

| Property | Value |
|----------|-------|
| **Codename** | `partitionspec-completeness` |
| **File** | `api/src/main/java/org/apache/iceberg/PartitionSpec.java` |
| **Phase** | ⬜ Phase 2: Informal Spec |
| **Rationale** | Data structure invariant; malformed specs cause data corruption |
| **Spec Size** | ~200 Lean lines |
| **Tractability** | Structural validation; decision procedures for membership |
| **Key Properties** | All partition fields refer to valid schema fields; no orphans |
| **Expected Theorems** | 2–3 |
| **Dependencies** | PartitionField validation, Schema reference integrity |
| **Status** | Blocked pending schema/partition field interaction spec |

---

## Execution Order (Recommended)

1. **Run 1** (current): Complete Research phase ✅; start Phase 2 on **Priority 1** (Identity) — trivial to establish confidence
2. **Run 2**: Complete Phase 2–4 on Identity; start Phase 2 on **Priority 2** (Bucket)
3. **Run 3**: Complete Phase 2–4 on Bucket; start Phase 2 on **Priority 3** (Truncate)
4. **Run 4**: Complete Phase 2–4 on Truncate; attempt proofs (Phase 5) on Identity
5. **Run 5**: Attempt proofs on Bucket; start Phase 2 on **Priority 5** (Null Ordering)
6. **Run 6+**: Build out Priority 4–7 targets; establish correspondence validation (Phase 6)

---

**Last Updated**: 2026-04-24 UTC  
**Author**: 🔬 Lean Squad
