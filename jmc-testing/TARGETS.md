# JMC Testing Targets

## Target 1: InMemoryCatalog Create/Drop Race

**File:** `InMemoryCatalogRaceJmc.java`  
**Package:** `org.mpi_sws.jmc.iceberg.targeted`  
**Priority:** CRITICAL (affects all catalog operations)

**Target Code:**
```
core/src/main/java/org/apache/iceberg/inmemory/InMemoryCatalog.java
- createTable() at line ~89
- dropTable() at line ~201
- synchronized blocks at 129, 161, 192, 209, 233, 253, 346, 357
```

**Test Scenario:**
- Setup: InMemoryCatalog, create schema with sample table
- Thread 1: Attempt to create a new table with concurrent writes
- Thread 2: Concurrently drop the same table or update metadata
- Race window: Between table.exists() check and actual table store

**Correctness Properties:**
- Tables map must not be corrupted
- Either CREATE succeeds and table exists, OR DROP succeeds and table gone
- No phantom reads or lost updates to table registry
- Namespace isolation maintained

**Estimated Complexity:** Medium  
**Concurrent Operations:** Create, Drop, List  
**Number of Variables:** 3 (namespaces, tables, version)

---

## Target 2: HadoopTableOperations Metadata Refresh Race

**File:** `HadoopMetadataRefreshJmc.java`  
**Package:** `org.mpi_sws.jmc.iceberg.targeted`  
**Priority:** HIGH (affects correctness of table view)

**Target Code:**
```
core/src/main/java/org/apache/iceberg/hadoop/HadoopTableOperations.java
- lines 68-70: volatile fields (currentMetadata, version, shouldRefresh)
- refresh() method
- metadata() getter
```

**Test Scenario:**
- Setup: HadoopTableOperations with mock file system
- Thread 1: Call metadata() to read current version
- Thread 2: Concurrently trigger refresh() to update metadata
- Race window: Between shouldRefresh check and actual metadata update

**Correctness Properties:**
- Metadata visibility consistent: version and metadata must align
- No torn writes (partial metadata reads)
- Refresh must be atomic or at least version-consistent
- Double-checked locking (if used) must work correctly

**Estimated Complexity:** Medium-High  
**Concurrent Operations:** Read metadata, Refresh, Update version  
**Number of Variables:** 3 (currentMetadata, version, shouldRefresh)

---

## Target 3: MergingSnapshotProducer Retry Counter Race

**File:** `MergingSnapshotProducerJmc.java`  
**Package:** `org.mpi_sws.jmc.iceberg.targeted`  
**Priority:** HIGH (affects merge retry logic correctness)

**Target Code:**
```
core/src/main/java/org/apache/iceberg/MergingSnapshotProducer.java
- line 93: private final AtomicInteger dvMergeAttempt = new AtomicInteger(0);
- Merge operations with retry logic
- incrementAndGet() / get() operations
```

**Test Scenario:**
- Setup: Create table with delete files, MergingSnapshotProducer
- Thread 1: Perform merge operation, increment attempt counter
- Thread 2: Concurrently perform merge on same snapshot
- Race: Attempt counter increments and retry logic decisions

**Correctness Properties:**
- Attempt counter increments correctly without lost updates
- Retry limits enforced consistently across threads
- No ABA-style problems with counter wrapping
- Final state matches total operations performed

**Estimated Complexity:** Medium  
**Concurrent Operations:** Merge, Increment, Check retry limit  
**Number of Variables:** 1 (dvMergeAttempt atomic counter)

---

## Target 4: BaseCombinedScanTask Lazy Task List Initialization

**File:** `ScanTaskLazyInitJmc.java`  
**Package:** `org.mpi_sws.jmc.iceberg.targeted`  
**Priority:** HIGH (affects scan execution)

**Target Code:**
```
core/src/main/java/org/apache/iceberg/BaseCombinedScanTask.java
- line 31: private transient volatile List<FileScanTask> taskList = null;
- flattenTasks() or similar lazy initialization method
```

**Test Scenario:**
- Setup: Create combined scan task with multiple files
- Thread 1: Call flattenedTasks() to trigger lazy initialization
- Thread 2: Concurrently call flattenedTasks()
- Race: Which thread initializes the list? Are both threads synchronized?

**Correctness Properties:**
- Only one taskList instance created (no duplicate initialization)
- All threads see same list
- List contents must match expected flattened tasks
- No memory leaks from discarded initializations

**Estimated Complexity:** Medium  
**Concurrent Operations:** Read taskList, Lazy initialize  
**Number of Variables:** 1 (taskList with volatile guard)

---

## Target 5: StandardEncryptionManager SecureRandom Initialization

**File:** `EncryptionManagerInitJmc.java`  
**Package:** `org.mpi_sws.jmc.iceberg.targeted`  
**Priority:** MEDIUM (affects encryption correctness)

**Target Code:**
```
core/src/main/java/org/apache/iceberg/encryption/StandardEncryptionManager.java
- line 54: private transient volatile SecureRandom lazyRNG = null;
- Encryption/decryption operations using RNG
```

**Test Scenario:**
- Setup: Create StandardEncryptionManager
- Thread 1: First decryption call (initializes lazyRNG)
- Thread 2: Concurrently decrypt (triggers RNG init race)
- Race: Which RNG instance used? Are they both initialized separately?

**Correctness Properties:**
- Single SecureRandom instance across threads
- No duplicate initialization
- All random values generated must be from single source
- Encryption/decryption consistent with single RNG

**Estimated Complexity:** Low-Medium  
**Concurrent Operations:** Decrypt, Initialize RNG  
**Number of Variables:** 1 (lazyRNG with double-checked locking)

---

## Implementation Order

1. **InMemoryCatalogRaceJmc** (Priority 1: Catalog foundation)
2. **HadoopMetadataRefreshJmc** (Priority 2: Metadata consistency)
3. **MergingSnapshotProducerJmc** (Priority 3: Merge correctness)
4. **ScanTaskLazyInitJmc** (Priority 4: Scan execution)
5. **EncryptionManagerInitJmc** (Priority 5: Encryption safety)

Each test follows the JMC test pattern:
- 2-thread ExecutorService for concurrency
- 100 random interleaving iterations
- InMemoryCatalog for test isolation
- Clear assertion of correctness properties

---

**Created:** 2026-04-27
**JMC Version:** Latest (with random strategy)
**Test Directory:** `core/src/test/java/org/mpi_sws/jmc/iceberg/`
