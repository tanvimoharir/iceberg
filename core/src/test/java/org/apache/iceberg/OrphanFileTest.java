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
import java.nio.file.Files;
import java.util.List;



import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.hadoop.conf.Configuration;

import org.apache.iceberg.io.DataWriter;

import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;



public class OrphanFileTest {

    @Test
    public void testOrphanFile() throws Exception {
        //Setup temp directory for HadoopTables
        File tempDir = Files.createTempDirectory("iceberg-test").toFile();
        tempDir.deleteOnExit();
        HadoopTables tables = new HadoopTables(new Configuration());

        //Define schema
        // 1. Create a table with simple schema
        Schema schema = new Schema(
                Types.NestedField.required(1, "id", Types.IntegerType.get()),
                Types.NestedField.optional(2, "age", Types.IntegerType.get())
        );

        PartitionSpec spec = PartitionSpec.unpartitioned();

        Table table = tables.create(schema, tempDir.getAbsolutePath());

        //Write a valid parquet file
        //File dataFile = new File(tempDir, "data-file.parquet");
        OutputFile outputFile = org.apache.iceberg.Files.localOutput(tempDir);
//
        // 3. Append first data file
        DataFile file1 = createDataFile(outputFile, schema, List.of(
                createRecord(schema, 1, 20),
                createRecord(schema, 2, 25)
        ));

        table.newAppend().appendFile(file1).commit();
        //Inject orphan file by editing manifest
        Snapshot snapshot = table.currentSnapshot();
        ManifestFile manifestFile = snapshot.allManifests(table.io()).get(0);

        System.out.println("Before adding orphan data: " + manifestFile);

        ManifestReader<DataFile> originalFiles = ManifestFiles.read(manifestFile, table.io());

        //Create fake orphan file entry
        DataFile fakeOrphan = createDataFile(outputFile, schema, List.of(
                createRecord(schema, 91, 20),
                createRecord(schema, 92, 25)
        ));

        table.newAppend().appendFile(fakeOrphan).commit();

        //Delete the orphan
        table.newDelete().deleteFile(fakeOrphan).commit();

        Snapshot stableSnapshot = table.currentSnapshot();
        ManifestFile manifest = stableSnapshot.allManifests(table.io()).get(0);

        System.out.println("After deleting orphaned data : " + manifest);

        //Rewrite Manifest with original + fake orphan
        ManifestWriter<DataFile> writer = ManifestFiles.write(table.spec(), (OutputFile) table.io());
        originalFiles.forEach(writer::add);
        writer.add(fakeOrphan);
        writer.close();

        System.out.println("Modifying manifests manually");

        //Commit new snapshot that includes fake manifest
        table.newAppend().appendManifest(manifestFile).commit();


        // Test the new manifest
        ManifestFile manifestFile1 = table.currentSnapshot().allManifests(table.io()).get(0);
        System.out.println("Was the orphanFile added? : " +  manifestFile1);

        //Verify that orphan file was detected and removed
        //assertTrue(removedFiles.stream().anyMatch(f -> f.contains("orphan-file.parquet")),
                //"Orphan file was not detected or removed");

        //System.out.println("Orphan file removal test passed. Removed files: \" + removedFiles");


    }

    // Helper: create a single Iceberg Record
    private static org.apache.iceberg.data.Record createRecord(Schema schema, int id, int age) {
        org.apache.iceberg.data.Record rec = GenericRecord.create(schema);
        rec.setField("id", id);
        rec.setField("age", age);
        return rec;
    }

    private DataFile createDataFile(OutputFile file, Schema schema, List<org.apache.iceberg.data.Record> records) throws IOException {
        //OutputFile file = org.apache.iceberg.Files.localOutput(temp.toFile());

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
}
