/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *   http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing,
 *  * software distributed under the License is distributed on an
 *  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  * KIND, either express or implied.  See the License for the
 *  * specific language governing permissions and limitations
 *  * under the License.
 *
 */

package org.apache.iceberg;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import java.util.stream.StreamSupport;
import org.apache.commons.io.FileUtils;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class TestSnapshotInvariants {

    @Test
    public void testBasicInvariants() throws IOException {
        //Load Iceberg table
        File tableDir = new File("target/iceberg-table-test");
        HadoopTables tables = new HadoopTables();
        Table table;

        //Clean up any old table
        FileUtils.deleteDirectory(new File(tableDir.getAbsolutePath()));

        Schema schema = new Schema(
                Types.NestedField.required(1, "id", Types.IntegerType.get()),
                Types.NestedField.optional(2, "currency", Types.StringType.get()
                )
        );

        PartitionSpec spec = PartitionSpec.unpartitioned();
        table = tables.create(schema, spec, tableDir.getAbsolutePath());

        // --- Snapshot 0: Table creation ---
        System.out.println("Initial snapshot: " + table.currentSnapshot());
        assertNull(table.currentSnapshot(), "No snapshot should exist right after table creation");


        // --- Append 1: add a data file ---
        DataFile file1 = DataFiles.builder(PartitionSpec.unpartitioned())
                .withInputFile(Files.localInput("src/test/resources/data1.parquet"))
                .withRecordCount(10)
                .withFileSizeInBytes(100)
                .build();

        table.newAppend().appendFile(file1).commit();

        // Refresh metadata to get updated view
        table.refresh();

        Snapshot s1 = table.currentSnapshot();
        assertNotNull(s1);
        System.out.println("After first append snapshot: " + s1.snapshotId());
        System.out.println("Manifest list: " + s1.manifestListLocation());

        // --- Append 2: add another file ---
        DataFile file2 = DataFiles.builder(PartitionSpec.unpartitioned())
                .withInputFile(Files.localInput("src/test/resources/data2.parquet"))
                .withRecordCount(5)
                .withFileSizeInBytes(80)
                .build();

        table.newAppend().appendFile(file2).commit();
        table.refresh();

        Snapshot s2 = table.currentSnapshot();
        assertNotNull(s2);
        System.out.println("After second append snapshot: " + s2.snapshotId());

        // --- Invariants ---
        List<Snapshot> snapshots = StreamSupport.stream(table.snapshots().spliterator(), false)
                .toList();

        System.out.println("All snapshots:");
        snapshots.forEach(s ->
                System.out.println("  id=" + s.snapshotId() + " timestamp=" + s.timestampMillis()));

        // Basic invariants
        assertEquals(2, snapshots.size(), "There should be exactly 2 snapshots after 2 commits");
        assertTrue(s2.timestampMillis() >= s1.timestampMillis(), "Snapshots should be monotonic in time");
        assertNotEquals(s1.snapshotId(), s2.snapshotId(), "Snapshot IDs should differ across commits");
        // to add snapshots exist in table.snapshots().
        assertTrue(snapshots.contains(s1));
        assertTrue(snapshots.contains(s2));


    }


    @Test
    public void testSnapshotParentCheck() throws IOException {
        HadoopTables tables = new HadoopTables();
        File tableDir = new File("target/iceberg-snapshot-parent-test");
        Table table;
        Schema schema = new Schema(
                Types.NestedField.required(1, "id", Types.IntegerType.get())
        );
        //Clean up any old table
        FileUtils.deleteDirectory(new File(tableDir.getAbsolutePath()));
        // Create a new table
        PartitionSpec spec = PartitionSpec.unpartitioned();
        table = tables.create(schema, spec, tableDir.getAbsolutePath());


        // --- Snapshot 0: Table creation ---
        System.out.println("Initial snapshot: " + table.currentSnapshot());
        assertNull(table.currentSnapshot(), "No snapshot should exist right after table creation");

        // --- Append 1: add a data file ---
        DataFile file1 = DataFiles.builder(PartitionSpec.unpartitioned())
                .withInputFile(Files.localInput("src/test/resources/data1.parquet"))
                .withRecordCount(10)
                .withFileSizeInBytes(100)
                .build();

        table.newAppend().appendFile(file1).commit();

        // Refresh metadata to get updated view
        table.refresh();

        Snapshot s1 = table.currentSnapshot();
        assertNotNull(s1);
        System.out.println("After first append snapshot: " + s1.snapshotId());
        System.out.println("Snapshot parentId: " + s1.parentId());

        // --- Append 2: add another file ---
        DataFile file2 = DataFiles.builder(PartitionSpec.unpartitioned())
                .withInputFile(Files.localInput("src/test/resources/data2.parquet"))
                .withRecordCount(5)
                .withFileSizeInBytes(80)
                .build();

        table.newAppend().appendFile(file2).commit();
        table.refresh();

        Snapshot s2 = table.currentSnapshot();
        assertNotNull(s2);
        System.out.println("After second append snapshot: " + s2.snapshotId());
        System.out.println("Snapshot parentId: " + s2.parentId());

        // --- Invariants ---
                List<Snapshot> snapshots = StreamSupport.stream(table.snapshots().spliterator(), false)
                .toList();

        System.out.println("All snapshots:");
        snapshots.forEach(s ->
                System.out.println("  id=" + s.snapshotId() + " timestamp=" + s.timestampMillis()));

        //Parent-child invariants
        assertEquals(s1.snapshotId(), s2.parentId(), "Snapshot 2 should have 1 as its parent");


        //Acyclicity check :  Traverse parent chain of each snapshot
        for (Snapshot snapshot : snapshots) {
            Set<Long> visited = new HashSet<>();
            Snapshot current = snapshot;
            while(current != null && current.parentId() != null && current.parentId() != 0L) {
                if (!visited.add(current.parentId())) {
                    fail("Cycle detected in snapshot parent chain starting at snapshot "+ snapshot.snapshotId());
                }
                current = table.snapshot(current.parentId());
            }
        }


    }

    @Test
    public void testSnapshotInvariantConcurrent() throws IOException, InterruptedException {
        HadoopTables tables = new HadoopTables();
        String tableLocation = "target/iceberg-snapshot-conc-test";
        Schema schema = new Schema(
                Types.NestedField.required(1, "id", Types.IntegerType.get())
        );
        PartitionSpec spec = PartitionSpec.unpartitioned();
        FileUtils.deleteDirectory(new File(tableLocation));

        Table table = tables.create(schema, spec, tableLocation);
        assertNull(table.currentSnapshot());

        int numThreads = 4;

        ExecutorService executor = Executors.newFixedThreadPool(numThreads);

        Runnable appendTask = () -> {
            try {
                DataFile f = DataFiles.builder(PartitionSpec.unpartitioned())
                        .withInputFile(Files.localInput("src/test/resources" + UUID.randomUUID() + ".parquet"))
                        .withRecordCount(1)
                        .withFileSizeInBytes(1)
                        .build();

                table.newAppend().appendFile(f).commit();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        };
        //Submit concurrent commits
        for (int i = 0; i < numThreads; i++) {
            executor.submit(appendTask);
        }

        executor.shutdown();
        executor.awaitTermination(30, TimeUnit.SECONDS);

        table.refresh();

        List<Snapshot> snapshots = StreamSupport.stream(table.snapshots().spliterator(),false).toList();

        System.out.println("All snapshots:");
        for (Snapshot snapshot : snapshots) {
            System.out.println("  id=" + snapshot.snapshotId() + "parent= "+ snapshot.parentId() + "timestamp=" + snapshot.timestampMillis());
        }
        // Basic checks
        // 1. Each snapshot has either null or valid parent
        for (Snapshot snapshot : snapshots) {
            if (snapshot.parentId() != null && snapshot.parentId() != 0L) {
                Snapshot parent = table.snapshot(snapshot.parentId());
                assertNotNull(parent, "Snapshot" + snapshot.snapshotId() + " has missing parent" + snapshot.snapshotId());
            }
        }

        //2. No two snapshots share same parent id
        Map<Long, Long> parentToChild = new HashMap<>();
        for (Snapshot s : snapshots) {
            Long pid = s.parentId();
            if (pid != null && pid != 0L) {
                assertFalse(parentToChild.containsKey(pid),
                        "Two snapshots share the same parent: " + pid);
                parentToChild.put(pid, s.snapshotId());
            }
        }

        //3. No cycles
        for (Snapshot s : snapshots) {
            Set<Long> visited = new HashSet<>();
            Snapshot current = s;
            while(current != null && current.parentId() != null && current.parentId() != 0L) {
                if (!visited.add(current.snapshotId())) {
                    fail("Cycle detected starting at snapshot "+ s.snapshotId());
                }
                current = table.snapshot(current.parentId());
            }
        }





    }


}
