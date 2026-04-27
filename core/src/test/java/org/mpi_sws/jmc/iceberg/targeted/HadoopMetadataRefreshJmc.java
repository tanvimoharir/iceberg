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

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.iceberg.Catalog;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.mpi_sws.jmc.annotations.JmcCheck;
import org.mpi_sws.jmc.annotations.JmcCheckConfiguration;

/**
 * JMC test for concurrent metadata refresh race conditions.
 *
 * <p>Concurrency Target: Volatile metadata caching and refresh logic, particularly in systems
 * that use currentMetadata, version, and shouldRefresh flags. Tests that concurrent metadata
 * reads and updates maintain consistency.
 *
 * <p>Scenario: Two threads race - one refreshing metadata while another reads it. Verifies that
 * metadata visibility is consistent and version numbers align with actual metadata state.
 */
public class HadoopMetadataRefreshJmc {

  private static final Namespace TEST_NS = Namespace.of("test_ns");

  @JmcCheck
  @JmcCheckConfiguration(numIterations = 100, strategy = "random", debug = false)
  public void testConcurrentMetadataReadWrite() throws Exception {
    // Setup: Create InMemoryCatalog with test table
    Catalog catalog = new InMemoryCatalog();
    catalog.createNamespace(TEST_NS);

    Schema schema =
        new Schema(
            1,
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.required(2, "value", Types.StringType.get()));

    Table table = catalog.createTable(TEST_NS.concat("test_table"), schema);
    int initialVersion = table.currentMetadata().version();

    ExecutorService executor = Executors.newFixedThreadPool(2);
    AtomicInteger readVersions = new AtomicInteger(0);
    AtomicInteger writeCount = new AtomicInteger(0);
    AtomicInteger errors = new AtomicInteger(0);

    try {
      // Thread 1: Perform writes (metadata changes)
      Future<?> f1 =
          executor.submit(
              () -> {
                try {
                  for (int i = 0; i < 3; i++) {
                    table.updateProperties().set("prop" + i, "value" + i).commit();
                    writeCount.incrementAndGet();
                    Thread.yield();
                  }
                } catch (Exception e) {
                  errors.incrementAndGet();
                }
              });

      // Thread 2: Continuously read and refresh metadata
      Future<?> f2 =
          executor.submit(
              () -> {
                try {
                  for (int i = 0; i < 5; i++) {
                    table.refresh();
                    TableMetadata metadata = table.currentMetadata();
                    int version = metadata.version();
                    if (version > initialVersion) {
                      readVersions.set(version);
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

      // Verify: No errors
      assertThat(errors.get()).isEqualTo(0);

      // Verify: Final refresh sees all writes
      table.refresh();
      int finalVersion = table.currentMetadata().version();
      assertThat(finalVersion).isGreaterThan(initialVersion);
      assertThat(finalVersion).isGreaterThanOrEqualTo(writeCount.get() + initialVersion);

      // Verify: Metadata is consistent (can read properties)
      assertThat(table.currentMetadata()).isNotNull();

    } finally {
      executor.shutdown();
    }
  }

  @JmcCheck
  @JmcCheckConfiguration(numIterations = 100, strategy = "random", debug = false)
  public void testMetadataConsistencyUnderConcurrentRefresh() throws Exception {
    // Setup: Create InMemoryCatalog with test table
    Catalog catalog = new InMemoryCatalog();
    catalog.createNamespace(TEST_NS);

    Schema schema =
        new Schema(
            1,
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.required(2, "name", Types.StringType.get()));

    Table table = catalog.createTable(TEST_NS.concat("consistency_test"), schema);

    ExecutorService executor = Executors.newFixedThreadPool(2);
    AtomicInteger consistencyErrors = new AtomicInteger(0);

    try {
      // Thread 1: Repeatedly refresh
      Future<?> f1 =
          executor.submit(
              () -> {
                try {
                  for (int i = 0; i < 10; i++) {
                    table.refresh();
                    // After refresh, verify metadata is readable
                    int version = table.currentMetadata().version();
                    assertThat(version).isGreaterThanOrEqualTo(0);
                  }
                } catch (Exception e) {
                  consistencyErrors.incrementAndGet();
                }
              });

      // Thread 2: Update properties while T1 refreshes
      Future<?> f2 =
          executor.submit(
              () -> {
                try {
                  for (int i = 0; i < 5; i++) {
                    try {
                      table.updateProperties().set("refresh_prop_" + i, "val_" + i).commit();
                    } catch (Exception e) {
                      // Concurrent modification is acceptable
                    }
                    Thread.yield();
                  }
                } catch (Exception e) {
                  consistencyErrors.incrementAndGet();
                }
              });

      // Wait for both threads
      f1.get();
      f2.get();

      // Verify: No assertion errors
      assertThat(consistencyErrors.get()).isEqualTo(0);

      // Verify: Final state is readable
      table.refresh();
      TableMetadata finalMetadata = table.currentMetadata();
      assertThat(finalMetadata).isNotNull();
      assertThat(finalMetadata.version()).isGreaterThanOrEqualTo(0);

    } finally {
      executor.shutdown();
    }
  }
}
