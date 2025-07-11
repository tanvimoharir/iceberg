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
package org.apache.iceberg.hadoop;

import static org.apache.iceberg.CatalogProperties.LOCK_ACQUIRE_TIMEOUT_MS;
import static org.apache.iceberg.TableProperties.COMMIT_MAX_RETRY_WAIT_MS;
import static org.apache.iceberg.TableProperties.COMMIT_MIN_RETRY_WAIT_MS;
import static org.apache.iceberg.TableProperties.COMMIT_NUM_RETRIES;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.LockManager;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.UpdateSchema;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.Tasks;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;
import org.mpisws.jmc.annotations.JmcCheckConfiguration;

public class TestHadoopCommits extends HadoopTableTestBase {

//  @Test
//  public void testCreateTable() throws Exception {
//    PartitionSpec expectedSpec = PartitionSpec.builderFor(TABLE_SCHEMA).bucket("data", 16).build();
//
//    assertThat(table.schema().asStruct())
//        .as("Table schema should match schema with reassigned ids")
//        .isEqualTo(TABLE_SCHEMA.asStruct());
//    assertThat(table.spec())
//        .as("Table partition spec should match with reassigned ids")
//        .isEqualTo(expectedSpec);
//
//    List<FileScanTask> tasks = Lists.newArrayList(table.newScan().planFiles());
//    assertThat(tasks).as("Should not create any scan tasks").isEmpty();
//    assertThat(tableDir).as("Table location should exist").exists();
//    assertThat(metadataDir).as("Should create metadata folder").exists().isDirectory();
//    assertThat(version(1)).as("Should create v1 metadata").exists().isFile();
//    assertThat(version(2)).as("Should not create v2 or newer versions").doesNotExist();
//    assertThat(versionHintFile).as("Should create version hint file").exists();
//    assertThat(readVersionHint())
//        .as("Should write the current version to the hint file")
//        .isEqualTo(1);
//    List<File> manifests = listManifestFiles();
//    assertThat(manifests).as("Should contain 0 Avro manifest files").isEmpty();
//  }
//
//  @Test
//  public void testSchemaUpdate() throws Exception {
//    assertThat(version(1)).as("Should create v1 metadata").exists().isFile();
//    assertThat(version(2)).as("Should not create v2 or newer versions").doesNotExist();
//
//    table.updateSchema().addColumn("n", Types.IntegerType.get()).commit();
//
//    assertThat(version(2)).as("Should create v2 for the update").exists().isFile();
//    assertThat(readVersionHint())
//        .as("Should write the current version to the hint file")
//        .isEqualTo(2);
//    assertThat(table.schema().asStruct())
//        .as("Table schema should match schema with reassigned ids")
//        .isEqualTo(UPDATED_SCHEMA.asStruct());
//
//    List<FileScanTask> tasks = Lists.newArrayList(table.newScan().planFiles());
//    assertThat(tasks).as("Should not create any scan tasks").isEmpty();
//
//    List<File> manifests = listManifestFiles();
//    assertThat(manifests).as("Should contain 0 Avro manifest files").isEmpty();
//  }
//
//  @Test
//  public void testSchemaUpdateComplexType() throws Exception {
//    assertThat(version(1)).as("Should create v1 metadata").exists().isFile();
//    assertThat(version(2)).as("Should not create v2 or newer versions").doesNotExist();
//
//    Types.StructType complexColumn =
//        Types.StructType.of(
//            required(0, "w", Types.IntegerType.get()),
//            required(1, "x", Types.StringType.get()),
//            required(2, "y", Types.BooleanType.get()),
//            optional(
//                3,
//                "z",
//                Types.MapType.ofOptional(0, 1, Types.IntegerType.get(), Types.StringType.get())));
//    Schema updatedSchema =
//        new Schema(
//            required(1, "id", Types.IntegerType.get(), "unique ID"),
//            required(2, "data", Types.StringType.get()),
//            optional(
//                3,
//                "complex",
//                Types.StructType.of(
//                    required(4, "w", Types.IntegerType.get()),
//                    required(5, "x", Types.StringType.get()),
//                    required(6, "y", Types.BooleanType.get()),
//                    optional(
//                        7,
//                        "z",
//                        Types.MapType.ofOptional(
//                            8, 9, Types.IntegerType.get(), Types.StringType.get())))));
//
//    table.updateSchema().addColumn("complex", complexColumn).commit();
//
//    assertThat(version(2)).as("Should create v2 for the update").exists().isFile();
//    assertThat(readVersionHint())
//        .as("Should write the current version to the hint file")
//        .isEqualTo(2);
//    assertThat(table.schema().asStruct())
//        .as("Table schema should match schema with reassigned ids")
//        .isEqualTo(updatedSchema.asStruct());
//
//    List<FileScanTask> tasks = Lists.newArrayList(table.newScan().planFiles());
//    assertThat(tasks).as("Should not create any scan tasks").isEmpty();
//
//    List<File> manifests = listManifestFiles();
//    assertThat(manifests).as("Should contain 0 Avro manifest files").isEmpty();
//  }
//
//  @Test
//  public void testSchemaUpdateIdentifierFields() throws Exception {
//    assertThat(version(1)).as("Should create v1 metadata").exists().isFile();
//    assertThat(version(2)).as("Should not create v2 or newer versions").doesNotExist();
//
//    Schema updatedSchema =
//        new Schema(
//            Lists.newArrayList(
//                required(1, "id", Types.IntegerType.get(), "unique ID"),
//                required(2, "data", Types.StringType.get())),
//            Sets.newHashSet(1));
//
//    table.updateSchema().setIdentifierFields("id").commit();
//
//    assertThat(version(2)).as("Should create v2 for the update").exists().isFile();
//    assertThat(readVersionHint())
//        .as("Should write the current version to the hint file")
//        .isEqualTo(2);
//    assertThat(table.schema().asStruct())
//        .as("Table schema should match schema with reassigned ids")
//        .isEqualTo(updatedSchema.asStruct());
//    assertThat(table.schema().identifierFieldIds())
//        .as("Identifier fields should match schema with reassigned ids")
//        .isEqualTo(updatedSchema.identifierFieldIds());
//  }
//
//  @Test
//  public void testFailedCommit() throws Exception {
//    // apply the change to metadata without committing
//    UpdateSchema update = table.updateSchema().addColumn("n", Types.IntegerType.get());
//    update.apply();
//
//    assertThat(version(1)).as("Should create v1 metadata").exists().isFile();
//    assertThat(version(2)).as("Should not create v2 or newer versions").doesNotExist();
//
//    version(2).createNewFile();
//
//    assertThatThrownBy(update::commit)
//        .isInstanceOf(CommitFailedException.class)
//        .hasMessageStartingWith("Version 2 already exists");
//
//    List<File> manifests = listManifestFiles();
//    assertThat(manifests).as("Should contain 0 Avro manifest files").isEmpty();
//
//    // verifies that there is no temporary metadata.json files left on disk
//    List<String> actual =
//        listMetadataJsonFiles().stream().map(File::getName).sorted().collect(Collectors.toList());
//    assertThat(actual)
//        .as("only v1 and v2 metadata.json should exist.")
//        .containsExactly("v1.metadata.json", "v2.metadata.json");
//  }
//
//  @Test
//  public void testStaleMetadata() throws Exception {
//    Table tableCopy = TABLES.load(tableLocation);
//
//    assertThat(version(1)).as("Should create v1 metadata").exists().isFile();
//    assertThat(version(2)).as("Should not create v2 or newer versions").doesNotExist();
//
//    // prepare changes on the copy without committing
//    UpdateSchema updateCopy = tableCopy.updateSchema().addColumn("m", Types.IntegerType.get());
//    updateCopy.apply();
//
//    table.updateSchema().addColumn("n", Types.IntegerType.get()).commit();
//
//    assertThat(version(2)).as("Should create v2 for the update").exists().isFile();
//    assertThat(table.schema().asStruct())
//        .as("Unmodified copy should be out of date after update")
//        .isNotEqualTo(tableCopy.schema().asStruct());
//
//    // update the table
//    tableCopy.refresh();
//
//    assertThat(table.schema().asStruct())
//        .as("Copy should be back in sync")
//        .isEqualTo(tableCopy.schema().asStruct());
//
//    assertThatThrownBy(updateCopy::commit)
//        .isInstanceOf(CommitFailedException.class)
//        .hasMessage("Cannot commit changes based on stale table metadata");
//
//    List<File> manifests = listManifestFiles();
//    assertThat(manifests).as("Should contain 0 Avro manifest files").isEmpty();
//  }
//
//  @Test
//  public void testStaleVersionHint() throws Exception {
//    Table stale = TABLES.load(tableLocation);
//
//    assertThat(version(1)).as("Should create v1 metadata").exists().isFile();
//    assertThat(version(2)).as("Should not create v2 or newer versions").doesNotExist();
//
//    table.updateSchema().addColumn("n", Types.IntegerType.get()).commit();
//
//    assertThat(version(2)).as("Should create v2 for the update").exists().isFile();
//    assertThat(readVersionHint())
//        .as("Should write the current version to the hint file")
//        .isEqualTo(2);
//    assertThat(stale.schema().asStruct())
//        .as("Stable table schema should not match")
//        .isNotEqualTo(UPDATED_SCHEMA.asStruct());
//
//    // roll the version hint back to 1
//    replaceVersionHint(1);
//
//    Table reloaded = TABLES.load(tableLocation);
//    assertThat(reloaded.schema().asStruct())
//        .as("Updated schema for newly loaded table should match")
//        .isEqualTo(UPDATED_SCHEMA.asStruct());
//
//    stale.refresh();
//    assertThat(reloaded.schema().asStruct())
//        .as("Refreshed schema for stale table should match")
//        .isEqualTo(UPDATED_SCHEMA.asStruct());
//  }
//
//  @Test
//  public void testFastAppend() throws Exception {
//    // first append
//    table.newFastAppend().appendFile(FILE_A).commit();
//
//    assertThat(version(2)).as("Should create v2 for the update").exists().isFile();
//    assertThat(readVersionHint())
//        .as("Should write the current version to the hint file")
//        .isEqualTo(2);
//
//    List<FileScanTask> tasks = Lists.newArrayList(table.newScan().planFiles());
//    assertThat(tasks).as("Should scan 1 file").hasSize(1);
//
//    List<File> manifests = listManifestFiles();
//    assertThat(manifests).as("Should contain only one Avro manifest file").hasSize(1);
//
//    // second append
//    table.newFastAppend().appendFile(FILE_B).commit();
//
//    assertThat(version(3)).as("Should create v3 for the update").exists().isFile();
//    assertThat(readVersionHint())
//        .as("Should write the current version to the hint file")
//        .isEqualTo(3);
//
//    tasks = Lists.newArrayList(table.newScan().planFiles());
//    assertThat(tasks).as("Should scan 2 files").hasSize(2);
//    assertThat(listManifestFiles()).as("Should contain 2 Avro manifest files").hasSize(2);
//
//    TableMetadata metadata = readMetadataVersion(3);
//    assertThat(metadata.currentSnapshot().allManifests(table.io()))
//        .as("Current snapshot should contain 2 manifests")
//        .hasSize(2);
//  }
//
//  @Test
//  public void testMergeAppend() throws Exception {
//    testFastAppend(); // create 2 compatible manifest files that will be merged
//
//    // merge all manifests for this test
//    table.updateProperties().set("commit.manifest.min-count-to-merge", "1").commit();
//
//    // third append
//    table.newAppend().appendFile(FILE_C).commit();
//
//    List<FileScanTask> tasks = Lists.newArrayList(table.newScan().planFiles());
//    assertThat(tasks).as("Should scan 3 files").hasSize(3);
//
//    assertThat(listManifestFiles()).as("Should contain 3 Avro manifest files").hasSize(3);
//
//    TableMetadata metadata = readMetadataVersion(5);
//    assertThat(metadata.currentSnapshot().allManifests(table.io()))
//        .as("Current snapshot should contain 1 merged manifest")
//        .hasSize(1);
//  }
//
//  @Test
//  public void testRenameReturnFalse() throws Exception {
//    FileSystem mockFs = mock(FileSystem.class);
//    when(mockFs.exists(any())).thenReturn(true, false);
//    when(mockFs.rename(any(), any())).thenReturn(false);
//    testRenameWithFileSystem(mockFs);
//  }

//  @Test
//  public void testRenameThrow() throws Exception {
//    FileSystem mockFs = mock(FileSystem.class);
//    when(mockFs.exists(any())).thenReturn(true, false);
//    when(mockFs.rename(any(), any())).thenThrow(new IOException("test injected"));
//    testRenameWithFileSystem(mockFs);
//  }
//
//  /**
//   * Test rename during {@link HadoopTableOperations#commit(TableMetadata, TableMetadata)} with the
//   * provided {@link FileSystem} object. The provided FileSystem will be injected for commit call.
//   */
//  private void testRenameWithFileSystem(FileSystem mockFs) throws Exception {
//    assertThat(version(1)).as("Should create v1 metadata").exists().isFile();
//    assertThat(version(2)).as("Should not create v2 or newer versions").doesNotExist();
//    assertThat(table).isInstanceOf(BaseTable.class);
//    BaseTable baseTable = (BaseTable) table;
//    // use v1 metafile as the test rename destination.
//    TableMetadata meta1 = baseTable.operations().current();
//
//    // create v2 metafile as base. This is solely for the convenience of rename testing later
//    // (so that we have 2 valid and different metadata files, which will reach the rename part
//    // during commit)
//    table.updateSchema().addColumn("n", Types.IntegerType.get()).commit();
//    assertThat(version(2)).as("Should create v2 for the update").exists().isFile();
//    assertThat(readVersionHint())
//        .as("Should write the current version to the hint file")
//        .isEqualTo(2);
//
//    // mock / spy the classes for testing
//    TableOperations tops = baseTable.operations();
//    assertThat(tops).isInstanceOf(HadoopTableOperations.class);
//    HadoopTableOperations spyOps = Mockito.spy((HadoopTableOperations) tops);
//
//    // inject the mockFS into the TableOperations
//    doReturn(mockFs).when(spyOps).getFileSystem(any(), any());
//    assertThatThrownBy(() -> spyOps.commit(tops.current(), meta1))
//        .isInstanceOf(CommitFailedException.class)
//        .hasMessage("Cannot commit changes based on stale table metadata");
//
//    // Verifies that there is no temporary metadata.json files left on rename failures.
//    Set<String> actual =
//        listMetadataJsonFiles().stream().map(File::getName).collect(Collectors.toSet());
//    Set<String> expected = Sets.newHashSet("v1.metadata.json", "v2.metadata.json");
//    assertThat(actual).as("only v1 and v2 metadata.json should exist.").isEqualTo(expected);
//  }
//
//  @Test
//  public void testCanReadOldCompressedManifestFiles() throws Exception {
//    assertThat(version(1)).as("Should create v1 metadata").exists().isFile();
//
//    // do a file append
//    table.newAppend().appendFile(FILE_A).commit();
//
//    // since we don't generate old file extensions anymore, let's convert existing metadata to old
//    // .metadata.json.gz
//    // to test backwards compatibility
//    rewriteMetadataAsGzipWithOldExtension();
//
//    List<File> metadataFiles = listMetadataJsonFiles();
//
//    assertThat(metadataFiles).as("Should have two versions").hasSize(2);
//    assertThat(metadataFiles.stream().map(File::getName))
//        .as("Metadata should be compressed with old format.")
//        .allMatch(f -> f.endsWith(".metadata.json.gz"));
//
//    Table reloaded = TABLES.load(tableLocation);
//
//    List<FileScanTask> tasks = Lists.newArrayList(reloaded.newScan().planFiles());
//    assertThat(tasks).as("Should scan 1 files").hasSize(1);
//  }

  @JmcCheckConfiguration(
          numIterations = 10,
          debug = true
  )
  public void testConcurrentFastAppends(@TempDir File dir) throws Exception {
    assertThat(version(1)).as("Should create v1 metadata").exists().isFile();
    int threadsCount = 5;
    int numberOfCommitedFilesPerThread = 10;
    Table tableWithHighRetries =
        TABLES.create(
            SCHEMA,
            SPEC,
            ImmutableMap.of(
                COMMIT_NUM_RETRIES,
                String.valueOf(threadsCount),
                COMMIT_MIN_RETRY_WAIT_MS,
                "10",
                COMMIT_MAX_RETRY_WAIT_MS,
                "1000",
                // Disable extra retry on lock acquire failure since commit will fail anyway.
                LOCK_ACQUIRE_TIMEOUT_MS,
                "0"),
            dir.toURI().toString());

    String fileName = UUID.randomUUID().toString();
    DataFile file =
        DataFiles.builder(tableWithHighRetries.spec())
            .withPath(FileFormat.PARQUET.addExtension(fileName))
            .withRecordCount(2)
            .withFileSizeInBytes(0)
            .build();
    ExecutorService executorService = Executors.newFixedThreadPool(threadsCount);

    AtomicInteger barrier = new AtomicInteger(0);
    Tasks.range(threadsCount)
        .stopOnFailure()
        .throwFailureWhenFinished()
        .executeWith(executorService)
        .run(
            index -> {
              for (int numCommittedFiles = 0;
                  numCommittedFiles < numberOfCommitedFilesPerThread;
                  numCommittedFiles++) {
                final int currentFilesCount = numCommittedFiles;
                Awaitility.await()
                    .pollInterval(Duration.ofMillis(10))
                    .atMost(Duration.ofSeconds(60))
                    .until(() -> barrier.get() >= currentFilesCount * threadsCount);
                tableWithHighRetries.newFastAppend().appendFile(file).commit();
                barrier.incrementAndGet();
              }
            });

    tableWithHighRetries.refresh();
    assertThat(Lists.newArrayList(tableWithHighRetries.snapshots()))
        .hasSize(threadsCount * numberOfCommitedFilesPerThread);
  }

//  @Test
//  public void testCommitFailedToAcquireLock() {
//    table.newFastAppend().appendFile(FILE_A).commit();
//    Configuration conf = new Configuration();
//    LockManager lockManager = new NoLockManager();
//    HadoopTableOperations tableOperations =
//        new HadoopTableOperations(
//            new Path(table.location()), new HadoopFileIO(conf), conf, lockManager);
//    tableOperations.refresh();
//    BaseTable baseTable = (BaseTable) table;
//    TableMetadata meta2 = baseTable.operations().current();
//    assertThatThrownBy(() -> tableOperations.commit(tableOperations.current(), meta2))
//        .isInstanceOf(CommitFailedException.class)
//        .hasMessageStartingWith("Failed to acquire lock on file");
//  }

  // Always returns false when trying to acquire
  static class NoLockManager implements LockManager {

    @Override
    public boolean acquire(String entityId, String ownerId) {
      return false;
    }

    @Override
    public boolean release(String entityId, String ownerId) {
      return false;
    }

    @Override
    public void close() throws Exception {}

    @Override
    public void initialize(Map<String, String> properties) {}
  }
}
