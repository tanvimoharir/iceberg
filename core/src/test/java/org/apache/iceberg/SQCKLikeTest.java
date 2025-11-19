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
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.HashMap;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;

public class SQCKLikeTest {

    @Test
    public void testDeclarativeModelExtraction() {
        //Load Iceberg table
        File tableDir = new File("target/iceberg-table");
        HadoopTables tables = new HadoopTables();
        Table table;
        if (!tableDir.exists()) {
            Schema schema = new Schema(
                    Types.NestedField.required(1, "id", Types.IntegerType.get()),
                    Types.NestedField.optional(2, "currency", Types.StringType.get())
            );
            PartitionSpec spec = PartitionSpec.unpartitioned();
            table = tables.create(schema, spec, tableDir.getAbsolutePath());
        } else {
            table = tables.load(tableDir.getAbsolutePath());
        }


        //Extract the model we created
        DeclarativeModel model = DeclarativeModel.from(table);

        // Some assertions
        assertNotNull(model.name);
        assert(model.snapshotIds != null);

        //some basic sample invariant
        for (Snapshot snapshot : table.snapshots()) {
            assertTrue(snapshot.timestampMillis() > 0, "Snapshots must have valid timespamtps");
        }

        System.out.println(model);
    }

    @Test
    public void testDeclarativeModelExtractionInMemory() {
        InMemoryCatalog catalog = new InMemoryCatalog();

        catalog.initialize("test_catalog", new HashMap<>());
        catalog.createNamespace(Namespace.of("db"));
        TableIdentifier tableId = TableIdentifier.of("db", "test_table");


        Schema schema = new Schema(
                Types.NestedField.required(1, "id", Types.IntegerType.get()),
                Types.NestedField.optional(2, "currency", Types.StringType.get())
        );

        Table table = catalog.createTable(tableId, schema);
        DeclarativeModel model = DeclarativeModel.from(table);
        assertNotNull(model.name);
        System.out.println(model);
    }


    @Test
    public void testWhatExistsOnJustTableCreation() throws IOException {
        //Load Iceberg table
        File tableDir = new File("target/iceberg-table2");
        HadoopTables tables = new HadoopTables();
        Table table;

        if (!tableDir.exists()) {
            Schema schema = new Schema(
                    Types.NestedField.required(1, "id", Types.IntegerType.get()),
                    Types.NestedField.optional(2, "currency", Types.StringType.get())
            );
            PartitionSpec spec = PartitionSpec.unpartitioned();
            table = tables.create(schema, spec, tableDir.getAbsolutePath());
        } else {
            table = tables.load(tableDir.getAbsolutePath());
        }

        System.out.println("Table location: " + table.location());
        System.out.println("Current snapshot: " + table.currentSnapshot());
        System.out.println("Snapshots: " + table.snapshots());
        assertNull(table.currentSnapshot());
        // Derive metadata path manually
        //Path metadataDir = tableDir.toPath().resolve("metadata");
        File metadataDir = new File(tableDir.getAbsolutePath(), "metadata");
        // List files in metadata directory
        File[] files = metadataDir.listFiles();
        System.out.println("Metadata directory contents:");
        assert files != null;
        for (File f : files) {
            System.out.println("  " + f.getName());
        }
        // Locate "metadata.json" (the pointer file)
        File metadataJson = new File(metadataDir, "v1.metadata.json");
        assertTrue(metadataJson.exists());

        // Read it
        String json = Files.readString(metadataJson.toPath());
        System.out.println("Contents of metadata.json:\n" + json);

    }

}
