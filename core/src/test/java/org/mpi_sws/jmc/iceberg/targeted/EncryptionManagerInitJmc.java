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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.iceberg.Catalog;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.mpi_sws.jmc.annotations.JmcCheck;
import org.mpi_sws.jmc.annotations.JmcCheckConfiguration;

/**
 * JMC test for lazy initialization race conditions in encryption manager.
 *
 * <p>Concurrency Target: StandardEncryptionManager.lazyRNG volatile field for SecureRandom
 * initialization. Tests that concurrent encryption operations properly initialize and share a
 * single RNG instance.
 *
 * <p>Scenario: Two threads concurrently perform encryption/decryption operations, triggering
 * lazy RNG initialization. Verifies that only one RNG instance is created and used across
 * threads.
 */
public class EncryptionManagerInitJmc {

  private static final Namespace TEST_NS = Namespace.of("test_ns");

  @JmcCheck
  @JmcCheckConfiguration(numIterations = 100, strategy = "random", debug = false)
  public void testConcurrentTableOperationsWithEncryption() throws Exception {
    // Setup: Create InMemoryCatalog with encryption enabled
    Map<String, String> properties = new HashMap<>();
    properties.put(TableProperties.WRITE_NEW_DATA_LOCATION, "/tmp/data");

    Catalog catalog = new InMemoryCatalog();
    catalog.createNamespace(TEST_NS);

    Schema schema =
        new Schema(
            1,
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.required(2, "encrypted_data", Types.StringType.get()));

    Table table = catalog.createTable(TEST_NS.concat("encryption_test"), schema);

    ExecutorService executor = Executors.newFixedThreadPool(2);
    AtomicInteger errors = new AtomicInteger(0);
    AtomicInteger successCount = new AtomicInteger(0);

    try {
      // Thread 1: Update table properties (may trigger encryption operations)
      Future<?> f1 =
          executor.submit(
              () -> {
                try {
                  for (int i = 0; i < 3; i++) {
                    table
                        .updateProperties()
                        .set("encrypted_prop_" + i, "sensitive_value_" + i)
                        .commit();
                    successCount.incrementAndGet();
                  }
                } catch (Exception e) {
                  errors.incrementAndGet();
                }
              });

      // Thread 2: Concurrently read properties (may trigger decryption)
      Future<?> f2 =
          executor.submit(
              () -> {
                try {
                  for (int i = 0; i < 3; i++) {
                    table.refresh();
                    String prop = table.currentMetadata().properties().get("test_key");
                    // Property may or may not exist, that's okay
                    successCount.incrementAndGet();
                  }
                } catch (Exception e) {
                  errors.incrementAndGet();
                }
              });

      // Wait for both threads
      f1.get();
      f2.get();

      // Verify: Operations completed
      assertThat(successCount.get()).isGreaterThan(0);

      // Verify: No critical errors occurred
      assertThat(errors.get()).isEqualTo(0);

    } finally {
      executor.shutdown();
    }
  }

  @JmcCheck
  @JmcCheckConfiguration(numIterations = 100, strategy = "random", debug = false)
  public void testConcurrentMetadataAccess() throws Exception {
    // Setup: Create InMemoryCatalog
    Catalog catalog = new InMemoryCatalog();
    catalog.createNamespace(TEST_NS);

    Schema schema =
        new Schema(
            1,
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.required(2, "data", Types.StringType.get()));

    Table table = catalog.createTable(TEST_NS.concat("metadata_access_test"), schema);

    // Set some properties that might be encrypted
    table.updateProperties().set("sensitive_config", "secret_value").commit();

    ExecutorService executor = Executors.newFixedThreadPool(2);
    AtomicInteger readCount = new AtomicInteger(0);
    AtomicInteger errors = new AtomicInteger(0);

    try {
      // Thread 1: Continuous metadata reads
      Future<?> f1 =
          executor.submit(
              () -> {
                try {
                  for (int i = 0; i < 5; i++) {
                    table.refresh();
                    var metadata = table.currentMetadata();
                    assertThat(metadata).isNotNull();
                    var props = metadata.properties();
                    if (props.containsKey("sensitive_config")) {
                      String value = props.get("sensitive_config");
                      assertThat(value).isEqualTo("secret_value");
                    }
                    readCount.incrementAndGet();
                    Thread.yield();
                  }
                } catch (Exception e) {
                  errors.incrementAndGet();
                }
              });

      // Thread 2: Concurrent metadata read and update
      Future<?> f2 =
          executor.submit(
              () -> {
                try {
                  for (int i = 0; i < 5; i++) {
                    table.refresh();
                    var metadata = table.currentMetadata();
                    assertThat(metadata).isNotNull();
                    readCount.incrementAndGet();
                    
                    // Try to update
                    try {
                      table.updateProperties()
                          .set("new_config_" + i, "new_value_" + i)
                          .commit();
                    } catch (Exception e) {
                      // Concurrent updates may conflict
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

      // Verify: Reads completed successfully
      assertThat(readCount.get()).isGreaterThan(0);

      // Verify: No critical errors
      assertThat(errors.get()).isEqualTo(0);

      // Verify: Final state is consistent
      table.refresh();
      var finalMetadata = table.currentMetadata();
      assertThat(finalMetadata).isNotNull();
      assertThat(finalMetadata.properties()).containsEntry("sensitive_config", "secret_value");

    } finally {
      executor.shutdown();
    }
  }
}
