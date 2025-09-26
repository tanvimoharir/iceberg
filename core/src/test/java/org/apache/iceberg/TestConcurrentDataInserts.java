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
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertNotEquals;

public class TestConcurrentDataInserts {
    @Test
    public void testConcurrentDataInserts() throws IOException, ExecutionException, InterruptedException {
        // 1. Create a parent temp directory manually
        File parentDir = new File(System.getProperty("java.io.tmpdir"),
                "iceberg_test_" + System.currentTimeMillis());
        if (!parentDir.mkdirs()) {
            throw new IOException("Failed to create parent temp directory: " + parentDir);
        }

        try{

            // 1. Create a table with simple schema
            Schema schema = new Schema(
                    Types.NestedField.required(1, "id", Types.IntegerType.get()),
                    Types.NestedField.optional(2, "age", Types.IntegerType.get())
            );

            PartitionSpec spec = PartitionSpec.unpartitioned();

            TestTables.TestTable testTable = TestTables.create(
                    parentDir,
                    "test_table_concurrent",
                    schema,
                    spec,
                    2
            );

            // 2. Capture initial metadata
            TableMetadata metadataBefore = TestTables.readMetadata("test_table_concurrent");
            int snapshotCountBefore = metadataBefore.snapshots().size();


            // 4. Prepare records for concurrent appends
            List<List<Record>> recordBatches = List.of(
                    List.of(createRecord(schema, 1, 20), createRecord(schema, 2, 25)),
                    List.of(createRecord(schema, 3, 30)),
                    List.of(createRecord(schema, 4, 40))
            );

            //4. Set up ExecutorService
            ExecutorService executor = Executors.newFixedThreadPool(recordBatches.size());
            List<Future<Void>> futures = new ArrayList<>();

            //5. Submit concurrent append tasks
            for (List<Record> record : recordBatches) {
                futures.add(executor.submit(() -> {
                    // Each thread gets its own subdirectory
                    File threadDir = new File(parentDir, "file_" + System.nanoTime());
                    if (!threadDir.mkdirs()) {
                        throw new IOException("Failed to create subdirectory: " + threadDir);
                    }
                    DataFile file = createDataFile(schema, record, threadDir);
                    testTable.newAppend().appendFile(file).commit();
                    return null;
                                }));
            }

            //6 Wait for all tasks to complete
            for (Future<Void> f : futures) {
                f.get();
            }
            executor.shutdown();

            // 7. Read final metadata and validate
            TableMetadata finalMetadata = TestTables.readMetadata("test_table_concurrent");
            int snapshotCountAfter = finalMetadata.snapshots().size();

            // Snapshot count grew by one
            assertNotEquals(snapshotCountBefore, snapshotCountAfter);
        } finally {
            deleteDirectory(parentDir);
        }

    }

    // Helper: create a single Iceberg Record
    private org.apache.iceberg.data.Record createRecord(Schema schema, int id, int age) {
        org.apache.iceberg.data.Record rec = GenericRecord.create(schema);
        rec.setField("id", id);
        rec.setField("age", age);
        return rec;
    }


    // Helper: create a DataFile with given records
    private DataFile createDataFile(Schema schema, List<org.apache.iceberg.data.Record> records, File baseDir) throws IOException {
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
    private void deleteDirectory(File dir) {
        if (!dir.exists()) return;
        for (File file : dir.listFiles()) {
            if (file.isDirectory()) deleteDirectory(file);
            else file.delete();
        }
        dir.delete();
    }
}
