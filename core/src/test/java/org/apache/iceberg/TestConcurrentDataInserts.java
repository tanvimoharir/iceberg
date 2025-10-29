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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.types.Types;


import org.eclipse.jetty.util.Callback;
import org.mpi_sws.jmc.annotations.JmcCheck;
import org.mpi_sws.jmc.annotations.JmcCheckConfiguration;
import org.mpi_sws.jmc.api.util.concurrent.JmcExecutorService;
import org.mpi_sws.jmc.api.util.concurrent.JmcFuture;

import static org.junit.jupiter.api.Assertions.assertNotEquals;


public class TestConcurrentDataInserts {

    public static void testConcurrentDataInserts() throws IOException, ExecutionException, InterruptedException {
        // 1. Create a parent temp directory manually
        File parentDir = new File(System.getProperty("java.io.tmpdir"),
                "iceberg_test_" + System.currentTimeMillis());
        if (!parentDir.mkdirs()) {
            throw new IOException("Failed to create parent temp directory: " + parentDir);
        }

        // Unique table name as well
        String tableName = "test_table_concurrent_jmc_" + System.nanoTime();

        try{
            Types.NestedField Column1 = Types.NestedField.required(1, "id", Types.IntegerType.get());
            Types.NestedField Column2 = Types.NestedField.optional(2, "age", Types.IntegerType.get());
            System.out.print("Created columns for table " + tableName);

            // 1. Create a table with simple schema
            Schema schema = new Schema(Column1, Column2);
            System.out.println("Created schema " + schema);

            PartitionSpec spec = PartitionSpec.unpartitioned();
            System.out.println("Partition spec: " + spec);

            TestTables.TestTable testTable = TestTables.create(
                    parentDir,
                    tableName,
                    schema,
                    spec,
                    2
            );
            System.out.println("Created test table: " + testTable);
            // 2. Capture initial metadata
            TableMetadata metadataBefore = TestTables.readMetadata(tableName);
            int snapshotCountBefore = metadataBefore.snapshots().size();
            System.out.println("Snapshot count: " + snapshotCountBefore);

            // 4. Prepare records for concurrent appends
            List<List<Record>> recordBatches = List.of(
                    List.of(createRecord(schema, 1, 20), createRecord(schema, 2, 25)),
                    List.of(createRecord(schema, 3, 30)),
                    List.of(createRecord(schema, 4, 40))
            );
            System.out.println("Created " + recordBatches.size() + " records");

            //4. Set up ExecutorService
            ExecutorService executor = Executors.newFixedThreadPool(2);
            if (executor instanceof JmcExecutorService) {
                int debug =0;
            }
            System.out.println("[DEBUG] Executor implementation: " + executor.getClass().getName());

            //ExecutorService executor = Executors.newSingleThreadExecutor();

            List<Future<Integer>> futures = new ArrayList<>();



            //5. Submit concurrent append tasks
//            for (List<Record> record : recordBatches) {
//                futures.add(executor.submit(() -> {
//                    // Each thread gets its own subdirectory
//                    File threadDir = new File(parentDir, "file_" + System.nanoTime());
//                    System.out.println("Writing " + record.size() + " records to " + threadDir);
//                    if (!threadDir.mkdirs()) {
//                        throw new IOException("Failed to create subdirectory: " + threadDir);
//                    }
//                    DataFile file = createDataFile(schema, record, threadDir);
//                    System.out.println("Created datafile" + file);
//                    testTable.newAppend().appendFile(file).commit();
//                    System.out.println("Table commited");
//                    return null;
//                                }));
//            }

            List<DataFile> files = new ArrayList<>();
            for (List<Record> record : recordBatches) {
                File threadDir = new File(parentDir, "file_" + System.nanoTime());
                threadDir.mkdirs();
                System.out.println("Writing " + record.size() + " records to " + threadDir);
                DataFile file = createDataFile(schema, record, threadDir);
                System.out.println("Created datafile" + file);
                files.add(file);
            }

//            for (DataFile file : files) {
//                futures.add(executor.submit(() -> {
//                    System.out.println("Before table commit");
//                    testTable.newAppend().appendFile(file).commit();
//                    System.out.println("Table commited");
//                    return 1;
//                }));
//            }
//            for (DataFile file : files) {
//
//                Callable<Integer> callable = () -> {
//                    System.out.println("Before table commit");
//                    testTable.newAppend().appendFile(file).commit();
//                    System.out.println("Table commited");
//                    return 1;
//                };
            for (DataFile file : files) {
                Callable<Integer> callable = new Callable<Integer>() {
                    @Override
                    public Integer call() throws Exception {
                        System.out.println("Before table commit");
                        testTable.newAppend().appendFile(file).commit();
                        System.out.println("Table committed");
                        return 1;
                    }
                };

                Future<Integer> future = executor.submit(callable);
                System.out.println("[JMC-DEBUG] Returned Future implementation: " + future.getClass().getName());
                futures.add(future);

            }

            //6 Wait for all tasks to complete
            for (Future<Integer> f : futures) {
                System.out.println("[JMC-DEBUG] Waiting for future: " + f);
                Integer result = f.get();
                System.out.println("[JMC-DEBUG] Future result: " + result);
            }
            executor.shutdown();
            System.out.println("Done waiting for futures to finish");

            // 7. Read final metadata and validate
            TableMetadata finalMetadata = TestTables.readMetadata(tableName);
            int snapshotCountAfter = finalMetadata.snapshots().size();

            // Snapshot count grew by one
            assertNotEquals(snapshotCountBefore, snapshotCountAfter);
        } finally {
            deleteDirectory(parentDir);
            System.out.println("Deleted " + parentDir);
        }

    }

    // Helper: create a single Iceberg Record
    private static org.apache.iceberg.data.Record createRecord(Schema schema, int id, int age) {
        org.apache.iceberg.data.Record rec = GenericRecord.create(schema);
        rec.setField("id", id);
        rec.setField("age", age);
        return rec;
    }


    // Helper: create a DataFile with given records
    private static DataFile createDataFile(Schema schema, List<org.apache.iceberg.data.Record> records, File baseDir) throws IOException {
        OutputFile file = Files.localOutput(baseDir);
        SortOrder sortOrder = SortOrder.builderFor(schema).withOrderId(10).asc("id").build();

        DataWriter<org.apache.iceberg.data.Record> dataWriter =
                Avro.writeData(file)
                        .schema(schema)
                        .createWriterFunc(org.apache.iceberg.data.avro.DataWriter::create)
                        .overwrite()
                        .withSpec(PartitionSpec.unpartitioned())
                        .withSortOrder(sortOrder)
                        .build();

        try {
            for (Record record : records) {
                dataWriter.write(record);
            }
        } finally {
            dataWriter.close();
        }

        return dataWriter.toDataFile();
    }

    // Recursive directory deletion
    private static void deleteDirectory(File dir) {
        if (!dir.exists()) return;
        for (File file : dir.listFiles()) {
            if (file.isDirectory()) deleteDirectory(file);
            else file.delete();
        }
        dir.delete();
    }
}
