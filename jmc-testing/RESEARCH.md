# JMC Concurrency Research: Apache Iceberg

## Overview
Systematic concurrency testing for Apache Iceberg, focusing on critical shared state mutations, commit operations, and metadata tracking.

## High-Priority Findings

### 1. InMemoryCatalog Synchronized Blocks (CRITICAL)
**Location:** `core/src/main/java/org/apache/iceberg/inmemory/InMemoryCatalog.java`

**Issue:** Multiple `synchronized(this)` blocks protecting internal state:
- `namespaces` map (lines 129, 161, 192, 209, 233, 253)
- `tables` map (lines 346, 357)
- Custom operations in `TransactionTable` (lines 412, 483)

**Shared State:**
- `namespaces`: ConcurrentHashMap-like structure for namespace isolation
- `tables`: Table storage and metadata
- Transaction state

**Concurrent Scenario:**
- Thread 1: Create table while Thread 2: Drops table
- Thread 1: Update table metadata while Thread 2: Reads metadata
- Thread 1: Create namespace while Thread 2: Lists namespaces

**Correctness Property:**
- Table creation/drop/update must be atomic from catalog perspective
- Concurrent operations must not corrupt catalog state
- Metadata visibility must be consistent

**Complexity:** Medium (2 threads, ~3 operations, single monitor)

---

### 2. HadoopTableOperations Volatile Fields (HIGH)
**Location:** `core/src/main/java/org/apache/iceberg/hadoop/HadoopTableOperations.java:68-70`

**Issue:** Volatile fields for caching:
```java
private volatile TableMetadata currentMetadata = null;
private volatile Integer version = null;
private volatile boolean shouldRefresh = true;
```

**Shared State:**
- `currentMetadata`: Table metadata cache
- `version`: Metadata version number
- `shouldRefresh`: Cache invalidation flag

**Concurrent Scenario:**
- Thread 1: Refresh metadata (sets shouldRefresh=false, updates currentMetadata)
- Thread 2: Read metadata while refresh in progress
- Race: Which metadata version is visible?

**Correctness Property:**
- Metadata reads must never see torn writes (version and metadata must align)
- Refresh operations must be atomic
- Double-checked locking must correctly implement initialization

**Complexity:** High (3 volatile fields, refresh coordination required)

---

### 3. MergingSnapshotProducer AtomicInteger (HIGH)
**Location:** `core/src/main/java/org/apache/iceberg/MergingSnapshotProducer.java:93`

**Issue:** Atomic counter for retry tracking:
```java
private final AtomicInteger dvMergeAttempt = new AtomicInteger(0);
```

**Shared State:**
- Delete file merge attempt counter
- Commit retry logic

**Concurrent Scenario:**
- Thread 1: Merge deletes and incrementCounter (attempts++)
- Thread 2: Concurrently modify delete state
- Thread 3: Check attempt count and retry

**Correctness Property:**
- Merge attempts must be tracked accurately across threads
- Retry limits must be enforced correctly
- No lost updates to retry counter

**Complexity:** Medium (atomic operations, retry coordination)

---

### 4. BaseCombinedScanTask Volatile Lazy List (HIGH)
**Location:** `core/src/main/java/org/apache/iceberg/BaseCombinedScanTask.java:31`

**Issue:** Double-checked locking pattern with volatile field:
```java
private transient volatile List<FileScanTask> taskList = null;
```

**Shared State:**
- Lazily initialized task list
- Scan task decomposition

**Concurrent Scenario:**
- Thread 1: Access taskList (triggers lazy initialization)
- Thread 2: Concurrently access taskList
- Both threads initialize separately (ABA problem)

**Correctness Property:**
- Single initialization of task list
- All threads see same list instance
- No duplicate initialization or memory corruption

**Complexity:** Medium (double-checked locking, lazy init race)

---

### 5. StandardEncryptionManager Volatile Caches (MEDIUM)
**Location:** `core/src/main/java/org/apache/iceberg/encryption/StandardEncryptionManager.java:53-54`

**Issue:** Volatile LoadingCache and LazyRNG:
```java
private transient volatile LoadingCache<String, ByteBuffer> unwrappedKeyCache;
private transient volatile SecureRandom lazyRNG = null;
```

**Shared State:**
- Encryption key cache
- Random number generator

**Concurrent Scenario:**
- Thread 1: Decrypt first block (initializes lazyRNG)
- Thread 2: Concurrently decrypt block
- Race: Which RNG instance is used?

**Correctness Property:**
- Single SecureRandom instance must be used across threads
- Cache must not be duplicated
- No corrupted encryption state

**Complexity:** Medium (lazy initialization, cache coordination)

---

## Medium-Priority Findings

### 6. InMemoryCatalog.TransactionTable (MEDIUM)
**Location:** `core/src/main/java/org/apache/iceberg/inmemory/InMemoryCatalog.java:412, 483`

**Issue:** Custom nested synchronized blocks within transaction wrapper

**Scenario:**
- Concurrent transaction commits
- Transaction isolation violations

---

### 7. AvroIO Mark/Reset (MEDIUM)
**Location:** `core/src/main/java/org/apache/iceberg/avro/AvroIO.java:131, 136`

**Issue:** Synchronized mark/reset on shared stream

**Scenario:**
- Thread 1: Mark stream position
- Thread 2: Mark/reset concurrently
- Stream position corruption

---

## Test Targets (Prioritized)

| Priority | Target | Scenario | File |
|----------|--------|----------|------|
| 1 | InMemoryCatalog | Concurrent table create/drop | `InMemoryCatalogRaceJmc.java` |
| 2 | HadoopTableOperations | Metadata refresh race | `HadoopMetadataRefreshJmc.java` |
| 3 | MergingSnapshotProducer | Concurrent merge attempts | `MergingSnapshotProducerJmc.java` |
| 4 | BaseCombinedScanTask | Lazy initialization race | `ScanTaskLazyInitJmc.java` |
| 5 | StandardEncryptionManager | RNG initialization | `EncryptionManagerInitJmc.java` |

## Dependencies
All tests will use:
- `org.apache.iceberg:iceberg-core` (InMemoryCatalog, TableMetadata, etc.)
- `org.apache.iceberg:iceberg-data` (GenericRecord, IcebergGenerics)
- `org.mpi-sws.jmc:jmc` (JmcCheck, JmcCheckConfiguration)
- `org.junit.jupiter:junit-jupiter` (assertions)

## Testing Infrastructure
- Strategy: `random` (systematic interleaving exploration)
- Iterations: 100-500 per test
- Concurrency: 2-thread tests (thread pool size 2)
- Executor: Fixed thread pool with `Future` synchronization
- Catalog: In-memory only (no external dependencies)

---

**Generated:** 2026-04-27
**Analysis Scope:** Apache Iceberg core module (v1.x)
