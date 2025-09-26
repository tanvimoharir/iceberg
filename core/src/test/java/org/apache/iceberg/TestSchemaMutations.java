package org.apache.iceberg;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;



import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.types.Types;


import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;


public class TestSchemaMutations {

    @Test
    public void testSchemaUpdateAddColumn(@TempDir Path tempDir) {
        //Case 1 of schema updates where neither manifests, nor snapshots are changed
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
        int schemeBefore = metadataBefore.currentSchemaId();

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
        int schemeAfter = metadataAfter.currentSchemaId();
        List<String> manifestsAfter = metadataAfter.snapshots().stream()
                .map(Snapshot::manifestListLocation)
                .collect(Collectors.toList());

        //Assertions
        // Snapshot count unchanged
        assertEquals(snapshotCountBefore, snapshotCountAfter);
        //Schema count grows
        assertEquals(schemaCountBefore + 1, schemaCountAfter);
        assertNotEquals(schemeBefore, schemeAfter);
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
        assertEquals(metadataBefore, metadataAfter);
    }

    @Test
    public void testSchemaUpdateMakeColumnRequired(@TempDir Path tempDir) {
        //Case 2 of schema updates where neither manifests, nor snapshots are changed
        File temp = tempDir.toFile();

        //Create a table with simple schema
        Schema schema = new Schema(
                required(1, "id", Types.IntegerType.get()),
                optional(2, "age", Types.IntegerType.get())
        );

        PartitionSpec spec = PartitionSpec.unpartitioned();

        TestTables.TestTable testTable = TestTables.create(
                temp,
                "test_schema_update3",
                schema,
                spec,
                2
        );

        // Iceberg does not allow this 'Cannot change column nullability'
        assertThrows(IllegalArgumentException.class, () -> {
            testTable.updateSchema()
                    .requireColumn("age")
                    .commit();
        });

    }

    @Test
    public void testSchemaUpdateMakeColumnOptional(@TempDir Path tempDir) {
        //Case 1 of schema updates where neither manifests, nor snapshots are changed
        File temp = tempDir.toFile();

        //Create a table with simple schema
        Schema schema = new Schema(
                required(1, "id", Types.IntegerType.get()),
                required(2, "age", Types.IntegerType.get())
        );

        PartitionSpec spec = PartitionSpec.unpartitioned();

        TestTables.TestTable testTable = TestTables.create(
                temp,
                "test_schema_update6",
                schema,
                spec,
                2
        );

        TableMetadata metadataBefore = TestTables.readMetadata("test_schema_update6");
        int snapshotCountBefore = metadataBefore.snapshots().size();
        int schemaCountBefore = metadataBefore.schemas().size();

        //String metadataFileBefore = metadataBefore.metadataFileLocation(); this does not work since we are not using actual Table object
        List<String> manifestsBefore = metadataBefore.snapshots().stream()
                .map(Snapshot::manifestListLocation)
                .collect(Collectors.toList());


        testTable.updateSchema()
                .makeColumnOptional("age")
                .commit();

        //Reload metadata
        TableMetadata metadataAfter = TestTables.readMetadata("test_schema_update6");
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
    public void testSchemaUpdateMakeColumnSmaller(@TempDir Path tempDir) {
        //Case 2 of schema updates where neither manifests, nor snapshots are changed
        File temp = tempDir.toFile();

        //Create a table with simple schema
        Schema schema = new Schema(
                required(1, "id", Types.IntegerType.get()),
                optional(2, "age", Types.LongType.get())
        );

        PartitionSpec spec = PartitionSpec.unpartitioned();

        TestTables.TestTable testTable = TestTables.create(
                temp,
                "test_schema_update4",
                schema,
                spec,
                2
        );

        //These are unsafe type changes in iceberg
        // Iceberg does not allow this IllegalArgumentException: Cannot change column type: age: long -> int
        assertThrows(IllegalArgumentException.class, () -> {
        testTable.updateSchema()
                .updateColumn("age", Types.IntegerType.get());
        });

    }


    @Test
    public void testSchemaUpdateMakeColumnBigger(@TempDir Path tempDir) {
        //Case 1 of schema updates where neither manifests, nor snapshots are changed
        File temp = tempDir.toFile();

        //Create a table with simple schema
        Schema schema = new Schema(
                required(1, "id", Types.IntegerType.get()),
                optional(2, "age", Types.IntegerType.get())
        );

        PartitionSpec spec = PartitionSpec.unpartitioned();

        TestTables.TestTable testTable = TestTables.create(
                temp,
                "test_schema_update5",
                schema,
                spec,
                2
        );

        TableMetadata metadataBefore = TestTables.readMetadata("test_schema_update5");
        int snapshotCountBefore = metadataBefore.snapshots().size();
        int schemaCountBefore = metadataBefore.schemas().size();

        //String metadataFileBefore = metadataBefore.metadataFileLocation(); this does not work since we are not using actual Table object
        List<String> manifestsBefore = metadataBefore.snapshots().stream()
                .map(Snapshot::manifestListLocation)
                .collect(Collectors.toList());


        testTable.updateSchema()
                    .updateColumn("age", Types.LongType.get())
                    .commit();

        //Reload metadata
        TableMetadata metadataAfter = TestTables.readMetadata("test_schema_update5");
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
    public void testSequentialDataInserts(@TempDir Path tempDir) throws IOException {
        File temp = tempDir.toFile();

        // 1. Create a table with simple schema
        Schema schema = new Schema(
                Types.NestedField.required(1, "id", Types.IntegerType.get()),
                Types.NestedField.optional(2, "age", Types.IntegerType.get())
        );

        PartitionSpec spec = PartitionSpec.unpartitioned();

        TestTables.TestTable testTable = TestTables.create(
                temp,
                "test_table_sequential",
                schema,
                spec,
                2
        );

        // 2. Capture initial metadata
        TableMetadata metadataBefore = TestTables.readMetadata("test_table_sequential");
        int snapshotCountBefore = metadataBefore.snapshots().size();
        List<String> manifestsBefore = metadataBefore.snapshots().stream()
                .map(Snapshot::manifestListLocation)
                .collect(Collectors.toList());

        // 3. Append first data file
        DataFile file1 = createDataFile(schema, List.of(
                createRecord(schema, 1, 20),
                createRecord(schema, 2, 25)
        ));
        testTable.newAppend().appendFile(file1).commit();

        TableMetadata metadataAfterFirst = TestTables.readMetadata("test_table_sequential");
        int snapshotCountAfterFirst = metadataAfterFirst.snapshots().size();
        List<String> manifestsAfterFirst = metadataAfterFirst.snapshots().stream()
                .map(Snapshot::manifestListLocation)
                .collect(Collectors.toList());

        // Assertions after first insert
        assertNotEquals(snapshotCountBefore, snapshotCountAfterFirst);
        assertNotEquals(manifestsBefore, manifestsAfterFirst);

        // 4. Append second data file
        DataFile file2 = createDataFile(schema, List.of(
                createRecord(schema, 3, 30)
        ));
        testTable.newAppend().appendFile(file2).commit();

        TableMetadata metadataAfterSecond = TestTables.readMetadata("test_table_sequential");
        int snapshotCountAfterSecond = metadataAfterSecond.snapshots().size();
        List<String> manifestsAfterSecond = metadataAfterSecond.snapshots().stream()
                .map(Snapshot::manifestListLocation)
                .collect(Collectors.toList());

        // Assertions after second insert
        assertNotEquals(snapshotCountAfterFirst, snapshotCountAfterSecond);
        assertNotEquals(manifestsAfterFirst, manifestsAfterSecond);
    }

    // Helper: create a single Iceberg Record
    private org.apache.iceberg.data.Record createRecord(Schema schema, int id, int age) {
        org.apache.iceberg.data.Record rec = GenericRecord.create(schema);
        rec.setField("id", id);
        rec.setField("age", age);
        return rec;
    }

    @TempDir Path temp;

    // Helper: create a DataFile with given records
    private DataFile createDataFile(Schema schema, List<org.apache.iceberg.data.Record> records) throws IOException {
        OutputFile file = Files.localOutput(temp.toFile());

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
