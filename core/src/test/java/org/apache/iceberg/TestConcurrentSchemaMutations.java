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
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.apache.iceberg.types.Types.NestedField.required;
import static org.junit.jupiter.api.Assertions.assertTrue;


public class TestConcurrentSchemaMutations {

    @Test
    public void testConcurrentAddColumn(@TempDir Path tempDir) throws Exception {
        File temp = tempDir.toFile();
        Schema schema = new Schema(required(1, "id", Types.IntegerType.get()));
        PartitionSpec spec = PartitionSpec.unpartitioned();

        TestTables.TestTable testTable = TestTables.create(
                temp, "test_concurrent_schema_add", schema, spec, 2
        );

        ExecutorService exec = Executors.newFixedThreadPool(2);
        List<Callable<Integer>> tasks = new ArrayList<>();

        tasks.add(() -> {
            testTable.updateSchema()
                    .addColumn("age", Types.StringType.get())
                    .commit();
            return 1;
        });

        tasks.add(() -> {
            testTable.updateSchema()
                    .addColumn("city", Types.StringType.get())
                    .commit();
            return 1;
        });

        List<Future<Integer>> results = exec.invokeAll(tasks);
        exec.shutdown();

        // Some commits may fail due to conflict, but not both
        int successCount = 0;
        for (Future<Integer> f : results) {
            try {
                f.get();
                successCount++;
            } catch (ExecutionException e) {
                assertTrue(e.getCause() instanceof CommitFailedException
                                || e.getCause() instanceof IllegalStateException,
                        "Expected commit conflict or retry failure");
            }
        }

        // After completion, reload metadata
        TableMetadata metadataAfter = TestTables.readMetadata("test_concurrent_schema_add");
        Schema finalSchema =  testTable.schema();

        // The table should have at least one of the new columns
        assertTrue(finalSchema.findField("age") != null || finalSchema.findField("city") != null);
        assertTrue(successCount >= 1);
    }


}
