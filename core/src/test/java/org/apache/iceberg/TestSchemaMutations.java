package org.apache.iceberg;

import static org.apache.iceberg.types.Types.NestedField.required;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;


import java.io.File;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.iceberg.types.Types;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;


public class TestSchemaMutations {

    @Test
    public void testSchemaUpdateAddColumn(@TempDir Path tempDir) {
        File temp = tempDir.toFile();

        //Create a table with simple schema
        Schema schema = new Schema(
                required(1, "id", Types.IntegerType.get())
        );

        PartitionSpec spec = PartitionSpec.unpartitioned();

        TestTables.TestTable testTable = TestTables.create(
                temp,
                "test_schema_update",
                schema,
                spec,
                2
        );



        //Capture current metadata and snapshot state
        TableMetadata metadataBefore = TestTables.readMetadata("test_schema_update");
        int snapshotCountBefore = metadataBefore.snapshots().size();
        int schemaCountBefore = metadataBefore.schemas().size();

        //String metadataFileBefore = metadataBefore.metadataFileLocation(); this does not work since we are not using actual Table object
        List<String> manifestsBefore = metadataBefore.snapshots().stream()
                .map(Snapshot::manifestListLocation)
                .collect(Collectors.toList());



        //Add a new column
        testTable.updateSchema()
                .addColumn("age", Types.StringType.get())
                .commit();


        //Reload metadata
        TableMetadata metadataAfter = TestTables.readMetadata("test_schema_update");
        int snapshotCountAfter = metadataAfter.snapshots().size();
        int schemaCountAfter = metadataAfter.schemas().size();
        String metadataFileAfter = metadataAfter.metadataFileLocation();
        List<String> manifestsAfter = metadataAfter.snapshots().stream()
                .map(Snapshot::manifestListLocation)
                .collect(Collectors.toList());


        //Assertions
        // Snapshot count unchanged
        assertEquals(snapshotCountBefore, snapshotCountAfter);
        //Schema count grows
        assertEquals(schemaCountBefore + 1, schemaCountAfter);
        // Schema updates change metadata
        assertNotEquals(metadataBefore, metadataAfter);
        //Manifests are not changed
        assertEquals(manifestsBefore, manifestsAfter);

    }


    @Test
    public void testSchemaUpdateWithoutMetadataChange(@TempDir Path tempDir) {
        File temp = tempDir.toFile();
        Schema schema = new Schema(
                required(1, "id", Types.IntegerType.get())
        );
        PartitionSpec spec = PartitionSpec.unpartitioned();
        TestTables.TestTable testTable = TestTables.create(
                temp,
                "test_schema_update2",
                schema,
                spec,
                2
        );

        //Capture current metadata and snapshot state
        TableMetadata metadataBefore = TestTables.readMetadata("test_schema_update2");
        int schemaCountBefore = metadataBefore.schemas().size();


        //Add a new column to schema via SchemaUpdate note that this does not change metadata
        Schema updated = new SchemaUpdate(schema, 1).addColumn("age", Types.StringType.get()).apply();

        //Reload metadata
        TableMetadata metadataAfter = TestTables.readMetadata("test_schema_update2");
        int schemaCountAfter = metadataAfter.schemas().size();

        //Schema count grows
        assertEquals(schemaCountBefore, schemaCountAfter);
    }


}