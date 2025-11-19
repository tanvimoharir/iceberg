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
import java.util.concurrent.ExecutionException;

import org.apache.commons.io.FileUtils;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.types.Types;
import org.mpi_sws.jmc.annotations.JmcCheck;
import org.mpi_sws.jmc.annotations.JmcCheckConfiguration;

import static org.junit.jupiter.api.Assertions.assertNull;


public class TestConcurrentCommits {

    @JmcCheck
    @JmcCheckConfiguration(numIterations = 10, debug = true)
    public static void testConcurrentCommits()  {

        File tableDir = new File("target/iceberg-new-conc-test");

//
//        // Unique table name as well
        String tableName = "test_table_concurrent_jmc_" + System.nanoTime();

        try{
            Types.NestedField Column1 = Types.NestedField.required(1, "id", Types.IntegerType.get());
            Types.NestedField Column2 = Types.NestedField.optional(2, "age", Types.IntegerType.get());
            System.out.print("Created columns for table " + tableName);

//            // 1. Create a table with simple schema
            Schema schema = new Schema(Column1, Column2);
            System.out.println("Created schema " + schema);

            PartitionSpec spec = PartitionSpec.unpartitioned();
            System.out.println("Partition spec: " + spec);

            TestTables.TestTable testTable = TestTables.create(
                    tableDir,
                    tableName,
                    schema,
                    spec,
                    2
            );
            // --- Append 1: add a data file ---
            DataFile file1 = DataFiles.builder(PartitionSpec.unpartitioned())
                    .withInputFile(Files.localInput("src/test/resources/data1.parquet"))
                    .withRecordCount(10)
                    .withFileSizeInBytes(100)
                    .build();

            testTable.newAppend().appendFile(file1).commit();


        } finally {

            System.out.println("Deleted nothing " + tableDir);
        }


    }


}
