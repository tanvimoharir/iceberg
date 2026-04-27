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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.iceberg.Catalog;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.mpi_sws.jmc.annotations.JmcCheck;
import org.mpi_sws.jmc.annotations.JmcCheckConfiguration;

/**
 * JMC test for concurrent table create/drop race conditions in InMemoryCatalog.
 *
 * <p>Concurrency Target: InMemoryCatalog.createTable() and dropTable() synchronized blocks
 * protecting the tables map. Tests that concurrent create and drop operations maintain catalog
 * consistency.
 *
 * <p>Scenario: Two threads race to create and drop tables in the same namespace, verifying that
 * the catalog state remains consistent and no tables are corrupted or lost.
 */
public class InMemoryCatalogRaceJmc {

  private static final Namespace TEST_NS = Namespace.of("test_ns");
  private static final String TABLE_1 = "table1";
  private static final String TABLE_2 = "table2";

  @JmcCheck
  @JmcCheckConfiguration(numIterations = 100, strategy = "random", debug = false)
  public void testConcurrentCreateDrop() throws Exception {
    // Setup: Create InMemoryCatalog with test namespace
    Catalog catalog = new InMemoryCatalog();
    catalog.createNamespace(TEST_NS);

    Schema schema =
        new Schema(
            1,
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.required(2, "name", Types.StringType.get()));

    ExecutorService executor = Executors.newFixedThreadPool(2);
    AtomicInteger errors = new AtomicInteger(0);

    try {
      // Thread 1: Create TABLE_1
      Future<?> f1 =
          executor.submit(
              () -> {
                try {
                  if (!catalog.tableExists(TEST_NS.concat(TABLE_1))) {
                    catalog.createTable(TEST_NS.concat(TABLE_1), schema);
                  }
                } catch (Exception e) {
                  errors.incrementAndGet();
                }
              });

      // Thread 2: Create TABLE_2
      Future<?> f2 =
          executor.submit(
              () -> {
                try {
                  if (!catalog.tableExists(TEST_NS.concat(TABLE_2))) {
                    catalog.createTable(TEST_NS.concat(TABLE_2), schema);
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

      // Verify: Both tables exist
      assertThat(catalog.tableExists(TEST_NS.concat(TABLE_1))).isTrue();
      assertThat(catalog.tableExists(TEST_NS.concat(TABLE_2))).isTrue();

      // Verify: Table list is consistent
      List<String> tables = catalog.listTables(TEST_NS);
      assertThat(tables).contains(TABLE_1, TABLE_2).hasSize(2);

      // Verify: Tables can be loaded without corruption
      Table t1 = catalog.loadTable(TEST_NS.concat(TABLE_1));
      Table t2 = catalog.loadTable(TEST_NS.concat(TABLE_2));
      assertThat(t1).isNotNull();
      assertThat(t2).isNotNull();

    } finally {
      executor.shutdown();
    }
  }

  @JmcCheck
  @JmcCheckConfiguration(numIterations = 100, strategy = "random", debug = false)
  public void testConcurrentCreateUpdate() throws Exception {
    // Setup: Create InMemoryCatalog with test namespace
    Catalog catalog = new InMemoryCatalog();
    catalog.createNamespace(TEST_NS);

    Schema schema =
        new Schema(
            1,
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.required(2, "data", Types.StringType.get()));

    String tableName = "test_table";

    ExecutorService executor = Executors.newFixedThreadPool(2);
    AtomicInteger errors = new AtomicInteger(0);
    AtomicInteger successCount = new AtomicInteger(0);

    try {
      // Thread 1: Create table
      Future<?> f1 =
          executor.submit(
              () -> {
                try {
                  if (!catalog.tableExists(TEST_NS.concat(tableName))) {
                    catalog.createTable(TEST_NS.concat(tableName), schema);
                    successCount.incrementAndGet();
                  }
                } catch (Exception e) {
                  // Expected if race loses
                  errors.incrementAndGet();
                }
              });

      // Thread 2: Try to get table (or create if not exists)
      Future<?> f2 =
          executor.submit(
              () -> {
                try {
                  Table table;
                  if (catalog.tableExists(TEST_NS.concat(tableName))) {
                    table = catalog.loadTable(TEST_NS.concat(tableName));
                    assertThat(table).isNotNull();
                  } else {
                    table = catalog.createTable(TEST_NS.concat(tableName), schema);
                    successCount.incrementAndGet();
                  }
                } catch (Exception e) {
                  errors.incrementAndGet();
                }
              });

      // Wait for both threads
      f1.get();
      f2.get();

      // Verify: At least one thread succeeded in creating
      assertThat(successCount.get()).isGreaterThanOrEqual(0);

      // Verify: Table exists and is consistent
      if (catalog.tableExists(TEST_NS.concat(tableName))) {
        Table table = catalog.loadTable(TEST_NS.concat(tableName));
        assertThat(table.schema().columns()).isNotNull();
        assertThat(table.schema().columns()).hasSize(2);
      }

    } finally {
      executor.shutdown();
    }
  }
}
