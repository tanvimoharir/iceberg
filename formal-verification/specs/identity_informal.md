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

# 🔬 Informal Specification: Identity Transform

**File**: `api/src/main/java/org/apache/iceberg/transforms/Identity.java`

**FV Target Codename**: `identity-transform`

**Phase**: 2 (Informal Spec Extraction)

---

## Purpose

The `Identity` transform is a partition function that returns its input unchanged. It is the simplest transform in Apache Iceberg's transform hierarchy, serving as a foundational partitioning strategy for columns where the raw value itself is the appropriate partition key.

This transform is semantically trivial: identity(x) = x for all supported x. However, it is critically important as the default partition transform and exhibits interesting implementation patterns (singleton design, generic type preservation, predicate projection) that merit formal verification to establish both correctness and robustness.

---

## Scope and Applicability

### Supported Types

The Identity transform can be applied to **any primitive type** except:
- `VARIANT` (JSON-like variant types)
- `GEOMETRY` (geospatial types)
- `GEOGRAPHY` (geospatial types)

Validation occurs in:
- `canTransform(Type)` — checks if a type is primitive and not in the unsupported set
- `bind(Type)` — validates and returns a SerializableFunction for the given type

### Binding Model

The transform uses a two-phase binding model:
1. **Type checking** via `canTransform(type)` — returns true/false
2. **Function extraction** via `bind(type)` — returns a `SerializableFunction<T, T>` that implements the actual transformation

The bound function is a singleton (`Apply.get()`) that delegates to `apply(T t) → t`.

---

## Preconditions

For a given Transform instance and type:

1. **Valid Type Precondition**: 
   - The type must be a primitive type (checked via `Type.isPrimitiveType()`)
   - The type's ID must not be in `{VARIANT, GEOMETRY, GEOGRAPHY}`

2. **Calling Sequence**:
   - `canTransform(type)` should be called **before** `bind(type)` or `apply(value)`
   - Calling `bind(type)` with an unsupported type will raise `IllegalArgumentException` (via Preconditions.checkArgument)

---

## Postconditions

### Core Transformation Property

For any value `x` of a supported primitive type `T`:

```
apply(x) = x
```

That is, the identity transform is **reflexive**: it returns the input unchanged.

### Type Preservation

For any type `sourceType`:

```
getResultType(sourceType) = sourceType
```

The result type is identical to the source type. The transform does not change type in any way.

### Order Preservation

```
preservesOrder() = true
```

The identity transform is **monotonic**: for all values `a < b` of the same type, `apply(a) ≤ apply(b)` because `a ≤ a` and `b ≤ b` (reflexivity). More formally, the identity relation is always a partial order that preserves strictly the ordering of the domain.

### Null Safety

Per the JavaDoc of `Transform.toHumanString(T value)`:

```
toHumanString(null) = "null"  (string representation)
apply(null) = null            (semantic identity)
```

When a null value is passed to `apply()`, it returns null unchanged. When `toHumanString()` is called on null, it is handled by a default implementation that returns the string "null".

### Idempotence of Binding

The `bind(type)` method returns the same singleton instance (`Apply.get()`) for all valid types. This means:

```
bind(t1) ≡ bind(t2)  (same object reference for all valid types)
```

The bound function is **type-erased**: the singleton `Apply` instance is parameterized with a type argument but does not actually use that information at runtime — all applications of the bound function simply return their input.

### Equality Semantics

Two Identity instances are equal if and only if they are both Identity instances:

```
identity1.equals(identity2) = true   (for any two Identity instances)
identity1.equals(other_transform) = false
```

The equality check is **type-insensitive** due to type erasure. Note: This behavior is flagged as "Can be removed with 2.0.0 deprecation of get(Type)", suggesting a planned simplification.

### Hash Code

```
identity.hashCode() = 0  (constant)
```

All Identity instances have the same hash code (0), consistent with the equality semantics where all instances are considered equal.

### String Representation

```
identity.toString() = "identity"
```

The string representation is the constant "identity".

---

## Invariants

### Singleton Pattern (Deprecated Path)

For the non-deprecated `get()` static method (no-argument version):

```
Identity.get() == Identity.get()  (reference equality)
```

The static factory always returns the same singleton instance. This ensures memory efficiency and enables identity-based comparisons.

### Type Field Semantics

The `type` field (stored by the deprecated `get(Type)` constructor) is **not used** in the core transformation logic:

- `apply(value)` does not consult the `type` field
- `bind(type)` does not consult the instance `type` field
- `getResultType(type)` does not consult the instance `type` field

The field is legacy, retained for backward compatibility with deprecated APIs. The canonical `get()` method sets `type = null`.

### Serialization Invariant

During serialization/deserialization, the instance is replaced by `SerializationProxies.IdentityTransformProxy.get()`, ensuring that after deserialization, the result is the canonical singleton:

```
readObject(writeObject(identity)) == Identity.get()
```

### Predicate Projection

For any bound predicate `p: BoundPredicate<T>` and partition field name `name`:

- `project(name, p)` and `projectStrict(name, p)` both project the predicate to the identity — they reconstruct the predicate with the same operation and operands but rebound to the field name
- For unary predicates (e.g., `IS_NULL`): returns `Expressions.predicate(op, name)`
- For literal predicates (e.g., `EQ 5`): returns `Expressions.predicate(op, name, literal)`
- For set predicates (e.g., `IN {1,2,3}`): returns `Expressions.predicate(op, name, literalSet)`

The projection is **lossless** — the predicate semantics are preserved identically under the identity transform.

---

## Edge Cases

### Type Edge Cases

1. **Unsupported Primitive Types**:
   - VARIANT, GEOMETRY, GEOGRAPHY → `canTransform()` returns false; `bind()` raises IllegalArgumentException
   
2. **Non-Primitive Types** (e.g., Struct, List):
   - `canTransform(complexType)` returns false
   - `bind()` raises IllegalArgumentException

3. **Null Type**:
   - Behavior undefined in the current implementation; Preconditions.checkArgument relies on JVM null-handling

### Value Edge Cases

1. **Null Value**:
   - `apply(null)` returns null
   - `toHumanString(null)` → "null" (via default Transform implementation or instance-level handling)

2. **Edge Numeric Values**:
   - Minimum/maximum values for integer types: returned unchanged
   - Positive/negative infinity for floating-point types: returned unchanged
   - NaN for floating-point types: returned unchanged

3. **String Edge Cases**:
   - Empty string "": returned as "" (identity)
   - Unicode and multi-byte strings: returned unchanged

### Predicate Edge Cases

1. **Null Predicate**:
   - `projectStrict()` returns null if the predicate is not unary, literal, or set (line 156)

2. **Complex Predicates**:
   - Binary/ternary predicates or other unknown types: returns null

---

## Comparison with Other Transforms

The Identity transform is distinguished from other Iceberg transforms by:

- **Bucket(n)**: `bucket(x) mod n`, changes the value; differs in `getResultType` (may narrow precision)
- **Truncate(width)**: `trunc(x, width)`, changes the value; loses lower bits
- **Year/Month/Day**: `year(ts)` extracts time components; narrowing transform
- **Hour/Minute/Second**: `hour(ts)` extracts time components; narrowing transform

Identity is the only transform that is **semantically transparent**: its output type is identical to its input type, and its value is identical to its input value.

---

## Deprecated APIs

The following methods are marked `@Deprecated` and scheduled for removal in version 2.0.0:

1. **`get(Type type)`** — static factory
   - Replaced by `get()` (no-argument)
   - The type parameter was unused even in this method

2. **`apply(T value)`** — instance method
   - Replaced by `bind(type).apply(value)` pattern
   - The new pattern is more composable and supports type introspection

3. **`toHumanString(T value)`** — single-argument instance method
   - Replaced by `toHumanString(Type, Object)` — two-argument version
   - The type parameter enables more sophisticated string representations for type-dependent rendering

These methods remain in the codebase for backward compatibility but are considered obsolete. New code should use the non-deprecated alternatives.

---

## Inferred Intent & Design Rationale

### Why Identity is Interesting to Verify

1. **Foundation**: Identity is the baseline partition transform. Proving it correct establishes confidence in the Transform abstraction.

2. **Singleton Pattern**: The use of a type-erased singleton instance for the Apply function is an optimization worth understanding and validating.

3. **Predicate Projection**: The transform's ability to project and rewrite predicates without changing their semantics is non-trivial and deserves proof.

4. **Type System Interaction**: The transform's generic type parameters `<T>` and the relationship between the generic type and the Iceberg Type system merit formal specification.

### Design Trade-offs Observed

1. **Type Erasure**: Java's generic type erasure means the Apply singleton cannot actually be type-safe in the traditional sense, yet the design leverages this to achieve memory efficiency (one object for all types).

2. **Deprecation Phase**: The migration from `get(Type)` to `get()` suggests a simplification trend — moving away from type-explicit APIs toward more implicit, handler-based patterns.

3. **Null Handling**: Null values are handled implicitly in `apply()` (returns null) and explicitly in `toHumanString()` (string "null"). This dual treatment is consistent but worth formalizing.

---

## Open Questions for Maintainer Clarification

1. **Type Erasure Soundness**: In a formal model, how should we represent the relationship between the Java generic type `T` and the Iceberg `Type` system? Should we assume they are always consistent, or should we model potential mismatches as precondition violations?

2. **Serialization Proxy Semantics**: The `writeReplace()` method delegates to a proxy. How should we model the guarantee that deserialization restores the singleton? Should we assume it always succeeds, or model potential failure cases?

3. **Predicate Projection Completeness**: The `projectStrict()` method returns `null` for unrecognized predicate types (line 156). Should this be treated as an error (precondition violation) or as an intended graceful fallback? What are the semantics of a null projection in the context of partition filtering?

4. **Future Behavior (2.0.0)**: Should the formal specification model Identity as it currently exists (with deprecated methods), or should it skip the deprecated surface and focus only on the modern API (`get()`, `bind()`, `getResultType()`, `preservesOrder()`, `satisfiesOrderOf()`, `projectStrict()`, `isIdentity()`)? Recommend focusing on the modern API to avoid chasing moving targets.

---

## Specification Summary

| Property | Value |
|----------|-------|
| **Transform Class** | `Identity<T> implements Transform<T, T>` |
| **Arity** | Unary (identity function) |
| **Core Law** | `apply(x) = x` (reflexive) |
| **Type Invariant** | `getResultType(T) = T` |
| **Order Preservation** | Yes (`preservesOrder() = true`) |
| **Null Safety** | `apply(null) = null` |
| **Type Erasure** | Singleton bind for all types |
| **Supported Types** | All primitives except VARIANT, GEOMETRY, GEOGRAPHY |
| **Deprecated APIs** | 3 methods (get(Type), apply(T), toHumanString(T)) |
| **Expected Proof Complexity** | Very Low (all properties are by reflexivity or trivial pattern matching) |

---

## Next Steps (Phase 3+)

1. **Phase 3** (Lean Formal Spec): Create `formal-verification/lean/FVSquad/Identity.lean`
   - Define Lean types mirroring or abstracting the Java types
   - State the key propositions with `sorry` bodies
   - Focus on: identity law, type preservation, order preservation, null safety

2. **Phase 4** (Implementation Extraction): Translate the apply function and bind logic to Lean
   - Preserve the functional semantics
   - Model type erasure appropriately (possibly with a Lean function over a type index)

3. **Phase 5** (Proof Assistance): Prove the stated propositions
   - `apply(x) = x` should be trivial by reflexivity
   - `getResultType(T) = T` should be immediate by definition
   - `preservesOrder() = true` should follow from order reflexivity
   - All proofs expected to be `rfl`, `trivial`, or one-liner `simp`

---

**🔬 Lean Squad Disclosure**: This specification was extracted during automated formal verification research. It documents inferred design intent and implementation semantics for the Identity transform.
