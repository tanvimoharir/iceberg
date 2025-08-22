/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg.util;


import static org.assertj.core.api.Assertions.assertThat;


import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.iceberg.StructLike;
import org.apache.iceberg.TestHelpers.CustomRow;
import org.apache.iceberg.TestHelpers.Row;

import org.mpi_sws.jmc.annotations.JmcCheckConfiguration;



import java.nio.ByteBuffer;
import java.util.*;
import org.apache.iceberg.StructLike;

final class TestPair<A, B> {
  final A first;
  final B second;
  private TestPair(A first, B second) { this.first = first; this.second = second; }
  static <A, B> TestPair<A, B> of(A a, B b) { return new TestPair<>(a, b); }
  @Override public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof TestPair)) return false;
    TestPair<?, ?> that = (TestPair<?, ?>) o;
    return java.util.Objects.equals(first, that.first)
            && java.util.Objects.equals(second, that.second);
  }
  @Override public int hashCode() { return java.util.Objects.hash(first, second); }
  @Override public String toString() { return "TestPair(" + first + ", " + second + ")"; }
}

final class MinimalPartitionMap<V> extends AbstractMap<TestPair<Integer, StructLike>, V> {

  private static final class Key {
    final int specId;
    final StructLike structRef;        // keep original for entrySet/keySet projection
    final List<Object> valuesOrNull;   // normalized for equals/hashCode

    Key(int specId, StructLike struct) {
      this.specId = specId;
      this.structRef = struct;
      this.valuesOrNull = normalize(struct);
    }

    private static List<Object> normalize(StructLike struct) {
      if (struct == null) return null;
      int size = struct.size();
      List<Object> out = new ArrayList<>(size);
      for (int i = 0; i < size; i++) {
        Object v = struct.get(i, Object.class);
        out.add(canonical(v));
      }
      return out;
    }

    private static Object canonical(Object v) {
      if (v == null) return null;
      if (v instanceof CharSequence) return v.toString();
      if (v instanceof ByteBuffer) {
        ByteBuffer buf = ((ByteBuffer) v).asReadOnlyBuffer();
        byte[] arr = new byte[buf.remaining()];
        buf.get(arr);
        return Arrays.hashCode(arr);
      }
      if (v.getClass().isArray()) {
        if (v instanceof Object[])   return Arrays.deepHashCode((Object[]) v);
        if (v instanceof byte[])     return Arrays.hashCode((byte[]) v);
        if (v instanceof int[])      return Arrays.hashCode((int[]) v);
        if (v instanceof long[])     return Arrays.hashCode((long[]) v);
        if (v instanceof short[])    return Arrays.hashCode((short[]) v);
        if (v instanceof char[])     return Arrays.hashCode((char[]) v);
        if (v instanceof boolean[])  return Arrays.hashCode((boolean[]) v);
        if (v instanceof float[])    return Arrays.hashCode((float[]) v);
        if (v instanceof double[])   return Arrays.hashCode((double[]) v);
      }
      return v;
    }

    @Override public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof Key)) return false;
      Key key = (Key) o;
      return specId == key.specId && Objects.equals(valuesOrNull, key.valuesOrNull);
    }
    @Override public int hashCode() { return 31 * specId + (valuesOrNull == null ? 0 : valuesOrNull.hashCode()); }
  }

  private final Map<Key, V> delegate = new HashMap<>();

  public static <T> MinimalPartitionMap<T> create() { return new MinimalPartitionMap<>(); }

  // convenience API used by your tests
  public V get(int specId, StructLike struct) { return delegate.get(new Key(specId, struct)); }
  public V put(int specId, StructLike struct, V value) { return delegate.put(new Key(specId, struct), value); }
  public V removeKey(int specId, StructLike struct) { return delegate.remove(new Key(specId, struct)); }

  // Map<K,V> overrides for K = TestPair<Integer, StructLike>
  @Override
  public V put(TestPair<Integer, StructLike> key, V value) {
    if (key == null) throw new NullPointerException("key must not be null");
    return put(key.first, key.second, value);
  }

  @Override
  public V get(Object key) {
    if (!(key instanceof TestPair)) return null;
    @SuppressWarnings("unchecked") TestPair<Integer, StructLike> p = (TestPair<Integer, StructLike>) key;
    return get(p.first, p.second);
  }

  @Override
  public V remove(Object key) {
    if (!(key instanceof TestPair)) return null;
    @SuppressWarnings("unchecked") TestPair<Integer, StructLike> p = (TestPair<Integer, StructLike>) key;
    return removeKey(p.first, p.second);
  }

  @Override
  public boolean containsKey(Object key) {
    if (!(key instanceof TestPair)) return false;
    @SuppressWarnings("unchecked") TestPair<Integer, StructLike> p = (TestPair<Integer, StructLike>) key;
    return delegate.containsKey(new Key(p.first, p.second));
  }

  @Override public int size() { return delegate.size(); }
  @Override public boolean isEmpty() { return delegate.isEmpty(); }
  @Override public void clear() { delegate.clear(); }
  @Override public boolean containsValue(Object value) { return delegate.containsValue(value); }

  @Override
  public Set<Entry<TestPair<Integer, StructLike>, V>> entrySet() {
    Set<Entry<TestPair<Integer, StructLike>, V>> out = new HashSet<>();
    for (Entry<Key, V> e : delegate.entrySet()) {
      Key k = e.getKey();
      TestPair<Integer, StructLike> pubKey = TestPair.of(k.specId, k.structRef);
      out.add(new SimpleEntry<>(pubKey, e.getValue()));
    }
    return Collections.unmodifiableSet(out);
  }

  @Override
  public Set<TestPair<Integer, StructLike>> keySet() {
    Set<TestPair<Integer, StructLike>> out = new HashSet<>();
    for (Key k : delegate.keySet()) {
      out.add(TestPair.of(k.specId, k.structRef));
    }
    return Collections.unmodifiableSet(out);
  }

  @Override
  public Collection<V> values() {
    return Collections.unmodifiableCollection(delegate.values());
  }

  @Override
  public String toString() {
    if (delegate.isEmpty()) return "{}";
    StringBuilder sb = new StringBuilder("{");
    boolean first = true;
    for (Entry<Key, V> e : delegate.entrySet()) {
      if (!first) sb.append(", ");
      first = false;
      Key k = e.getKey();
      sb.append("specId=").append(k.specId).append(" ").append(k.valuesOrNull).append(" -> ").append(e.getValue());
    }
    sb.append("}");
    return sb.toString();
  }
}


public class TestPartitionMap {

//  private static final Schema SCHEMA = new Schema(
//          required(1, "id", Types.IntegerType.get()),
//          required(2, "data", Types.StringType.get()),
//          required(3, "category", Types.StringType.get())); ;
////  static {
//
//    Schema temp = new Schema(
//            required(1, "id", Types.IntegerType.get()),
//            required(2, "data", Types.StringType.get()),
//            required(3, "category", Types.StringType.get()));
//    JmcRuntimeUtils.writeEventWithoutYield(null, temp, "","","");
//    SCHEMA = temp;
//    JmcRuntime.yield();
//  }

//  private static final PartitionSpec UNPARTITIONED_SPEC = PartitionSpec.unpartitioned();
//  private static final PartitionSpec BY_DATA_SPEC =
//      PartitionSpec.builderFor(SCHEMA).identity("data").withSpecId(1).build();
//  private static final PartitionSpec BY_DATA_CATEGORY_BUCKET_SPEC =
//      PartitionSpec.builderFor(SCHEMA).identity("data").bucket("category", 8).withSpecId(3).build();
//  private static final Map<Integer, PartitionSpec> SPECS =
//
//      ImmutableMap.of(
//          UNPARTITIONED_SPEC.specId(),
//          UNPARTITIONED_SPEC,
//          BY_DATA_SPEC.specId(),
//          BY_DATA_SPEC,
//          BY_DATA_CATEGORY_BUCKET_SPEC.specId(),
//          BY_DATA_CATEGORY_BUCKET_SPEC);
  private static final int UNPARTITIONED_SPEC_ID = 0;
  private static final int BY_DATA_SPEC_ID = 1;
  private static final int BY_DATA_CATEGORY_BUCKET_SPEC_ID = 3;


    @JmcCheckConfiguration(
            numIterations = 10,
            debug = true
    )
  public void testConcurrentReadAccess() throws InterruptedException {
//    PartitionMap<String> map = PartitionMap.create(SPECS);
//    map.put(BY_DATA_SPEC.specId(), Row.of("aaa"), "v1");
//    map.put(BY_DATA_SPEC.specId(), Row.of("bbb"), "v2");
//    System.out.println("SPECS is "+ map);
//    map.put(UNPARTITIONED_SPEC.specId(), null, "v3");
//    map.put(BY_DATA_SPEC.specId(), CustomRow.of("ccc"), "v4");

      MinimalPartitionMap<String> map = MinimalPartitionMap.create();
      map.put(BY_DATA_SPEC_ID, Row.of("aaa"), "v1");
      map.put(BY_DATA_SPEC_ID, Row.of("bbb"), "v2");
      System.out.println("map is " + map);
      map.put(UNPARTITIONED_SPEC_ID, null, "v3");
      map.put(BY_DATA_SPEC_ID, CustomRow.of("ccc"), "v4");

    int numThreads = 10;
    ExecutorService executorService = Executors.newFixedThreadPool(numThreads);

    // read the map from multiple threads to ensure thread-local wrappers are used
    for (int i = 0; i < numThreads; i++) {
      executorService.submit(
          () -> {
            assertThat(map.get(BY_DATA_SPEC_ID, Row.of("aaa"))).isEqualTo("v1");
            assertThat(map.get(BY_DATA_SPEC_ID, Row.of("bbb"))).isEqualTo("v2");
            assertThat(map.get(UNPARTITIONED_SPEC_ID, null)).isEqualTo("v3");
            assertThat(map.get(BY_DATA_SPEC_ID, Row.of("ccc"))).isEqualTo("v4");
          });
    }

    executorService.shutdown();
    assertThat(executorService.awaitTermination(1, TimeUnit.MINUTES)).isTrue();
  }

}
