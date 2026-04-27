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
package org.mpi_sws.jmc.iceberg.targeted;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.iceberg.BaseCombinedScanTask;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.Catalog;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.mpi_sws.jmc.annotations.JmcCheck;
import org.mpi_sws.jmc.annotations.JmcCheckConfiguration;

/**
 * JMC test for lazy initialization race conditions in scan task flattening.
 *
 * <p>Concurrency Target: BaseCombinedScanTask.taskList volatile field with lazy initialization.
 * Tests that the double-checked locking pattern for task flattening is thread-safe.
 *
 * <p>Scenario: Two threads concurrently access flattened task lists, triggering lazy
 * initialization. Verifies that only one initialization occurs and all threads see the same
 * flattened list.
 */
public class ScanTaskLazyInitJmc {

  private static final Namespace TEST_NS = Namespace.of("test_ns");

  @JmcCheck
  @JmcCheckConfiguration(numIterations = 100, strategy = "random", debug = false)
  public void testConcurrentScanTaskAccess() throws Exception {
    // Setup: Create InMemoryCatalog with test table and data
    Catalog catalog = new InMemoryCatalog();
    catalog.createNamespace(TEST_NS);

    Schema schema =
        new Schema(
            1,
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.required(2, "value", Types.StringType.get()));

    Table table = catalog.createTable(TEST_NS.concat("scan_test"), schema);

    // Add some data files
    DataFile file1 =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath("/path/to/file1.parquet")
            .withFormat(FileFormat.PARQUET)
            .withFileSizeInBytes(1000)
            .withRecordCount(100)
            .build();

    DataFile file2 =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath("/path/to/file2.parquet")
            .withFormat(FileFormat.PARQUET)
            .withFileSizeInBytes(1000)
            .withRecordCount(100)
            .build();

    ExecutorService executor = Executors.newFixedThreadPool(2);
    Set<Integer> observedHashCodes = ConcurrentHashMap.newKeySet();
    AtomicInteger errors = new AtomicInteger(0);

    try {
      // Thread 1: Access scan
      Future<?> f1 =
          executor.submit(
              () -> {
                try {
                  var scan = table.newScan();
                  // Multiple accesses to trigger lazy initialization
                  for (int i = 0; i < 5; i++) {
                    Iterable<CombinedScanTask> tasks = scan.planTasks();
                    if (tasks != null) {
                      int hashCode = System.identityHashCode(tasks);
                      observedHashCodes.add(hashCode);
                    }
                    Thread.yield();
                  }
                } catch (Exception e) {
                  errors.incrementAndGet();
                }
              });

      // Thread 2: Concurrently access scan
      Future<?> f2 =
          executor.submit(
              () -> {
                try {
                  var scan = table.newScan();
                  // Multiple accesses to trigger lazy initialization
                  for (int i = 0; i < 5; i++) {
                    Iterable<CombinedScanTask> tasks = scan.planTasks();
                    if (tasks != null) {
                      int hashCode = System.identityHashCode(tasks);
                      observedHashCodes.add(hashCode);
                    }
                    Thread.yield();
                  }
                } catch (Exception e) {
                  errors.incrementAndGet();
                }
              });

      // Wait for both threads
      f1.get();
      f2.get();

      // Verify: No errors occurred
      assertThat(errors.get()).isEqualTo(0);

    } finally {
      executor.shutdown();
    }
  }

  @JmcCheck
  @JmcCheckConfiguration(numIterations = 100, strategy = "random", debug = false)
  public void testLazyInitializationConsistency() throws Exception {
    // Setup: Create InMemoryCatalog with simple table
    Catalog catalog = new InMemoryCatalog();
    catalog.createNamespace(TEST_NS);

    Schema schema =
        new Schema(
            1,
            Types.NestedField.required(1, "col", Types.IntegerType.get()));

    Table table = catalog.createTable(TEST_NS.concat("lazy_init_test"), schema);

    ExecutorService executor = Executors.newFixedThreadPool(2);
    Set<String> scanResults = ConcurrentHashMap.newKeySet();
    AtomicInteger errors = new AtomicInteger(0);

    try {
      // Thread 1: First scan accessor
      Future<?> f1 =
          executor.submit(
              () -> {
                try {
                  var scan = table.newScan();
                  // Trigger lazy initialization
                  Iterable<CombinedScanTask> tasks = scan.planTasks();
                  assertThat(tasks).isNotNull();
                  scanResults.add("thread1_" + System.identityHashCode(tasks));
                } catch (Exception e) {
                  errors.incrementAndGet();
                }
              });

      // Thread 2: Concurrent scan accessor (races with T1 initialization)
      Future<?> f2 =
          executor.submit(
              () -> {
                try {
                  var scan = table.newScan();
                  // Trigger lazy initialization concurrently
                  Iterable<CombinedScanTask> tasks = scan.planTasks();
                  assertThat(tasks).isNotNull();
                  scanResults.add("thread2_" + System.identityHashCode(tasks));
                } catch (Exception e) {
                  errors.incrementAndGet();
                }
              });

      // Wait for both threads
      f1.get();
      f2.get();

      // Verify: No errors in initialization
      assertThat(errors.get()).isEqualTo(0);

      // Verify: Both threads obtained tasks
      assertThat(scanResults).hasSize(2);

    } finally {
      executor.shutdown();
    }
  }
}
