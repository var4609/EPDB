# EPDB

EPDB is a simple relational database written in Kotlin, built as a learning
project to explore the internals of a traditional disk-oriented DBMS — storage,
buffering, indexing, the catalog, and a Volcano-style query execution engine.

The codebase is intentionally small and read-first: each component lives in its
own Gradle module, each with a clean interface that hides module-internal implementation 
details from the outside world.

## Architecture

EPDB is organized into focused modules that mirror the layers of a textbook
database system:

| Module        | Responsibility |
|---------------|----------------|
| `application` | Entry point that wires the modules together and runs a sample workload. |
| `engine`      | Query execution. Volcano-style operators (`TableScan`, `IndexScan`, `Selection`, `Projection`, `Insert`), predicates, and a `Database` query executor. |
| `buffer`      | Buffer manager with page pinning, dirty tracking, and eviction. |
| `storage`     | Page-based storage. Fixed 4 KB pages with a header (free-space offset, row count, next page id) and a slotted layout. |
| `index`       | Index manager mapping keys to `(pageId, slotIndex)` pointers. |
| `catalog`     | Table schemas and per-table page lists. |
| `commons`     | Shared types: `ColumnType`, `Schema`, `ColumnValue`, serializers, logging. |
| `integration` | Integration-level entry points and tests across modules. |

Today every manager has an in-memory implementation (`InMemoryStorageManager`,
`InMemoryBufferManager`, `InMemoryIndexManager`, `InMemoryCatalog`), which keeps
the focus on the data structures and operator pipeline.

### Module dependencies

```
        application
             │
             ▼
          engine ──────┬─────────┐
             │         │         │
             ▼         ▼         ▼
          buffer    index     catalog
             │
             ▼
          storage

          commons   (shared types, used by every module)
```

## Execution model

The engine uses the classic Volcano (iterator) model. Every operator implements
a tiny interface:

```kotlin
interface Operator : AutoCloseable {
    fun open()
    fun next(): Tuple?
}
```

Operators compose into a tree. A query is executed by pulling tuples one at a
time from the root operator's `next()`; each call recursively pulls from its
children until a leaf scan returns the next row or `null` to signal end of
stream. Resources (pinned pages, iterator state) are released through
`AutoCloseable` so an operator subtree can be used in a Kotlin `use { }` block.

This lazy, pipelined model means no operator materializes its full output: a
`Projection` over a `Selection` over an `IndexScan` only touches the rows it
actually needs to return.

## On-disk page layout

Storage is page-oriented. Every page is a fixed 4 KB byte array with a
12-byte header, records growing forward from the header, and slot entries
growing backward from the end of the page — the canonical slotted-page format:

```
Page (4096 bytes)
┌──────────────────────────────────────────────────────────────────┐
│ Header (12 B): freeSpaceOffset | numRows | nextPageId            │
├──────────────────────────────────────────────────────────────────┤
│ Record 0 → Record 1 → Record 2 → ...      (records grow down)    │
│                                                                  │
│                          ... free space ...                      │
│                                                                  │
│              ... ← Slot 2 ← Slot 1 ← Slot 0   (slots grow up)    │
└──────────────────────────────────────────────────────────────────┘

Slot (8 B) = recordOffset (4 B) | recordLength (4 B)
```

Pages are chained via `nextPageId` in the header, so a table is a linked list
of pages. Indexes reference rows by `PagePointer(pageId, slotIndex)`, which
stays valid as long as the slot directory is append-only (which it is today —
deletes and compaction are future work).

## Supported features

- Table creation via the catalog with a typed schema (`INT`, `STRING_FIXED_TYPE`).
- Row inserts that flow through the buffer manager and update the index.
- Query execution through composable operators:
  - Sequential scan (`TableScan`)
  - Index lookup (`IndexScan`)
  - Filter (`Selection`) with comparison predicates (`=`, `>`, ...)
  - Projection over a column subset
- 4 KB slotted pages with a fixed page header and forward `nextPageId` links.

## Project layout

```
modules/
  application/   # Main.kt — sample workload
  engine/        # operators, predicates, query executor
  buffer/        # buffer manager
  storage/       # page format + storage manager
  index/         # index manager
  catalog/       # table catalog
commons/         # shared types and utilities
integration/     # cross-module integration
buildSrc/        # shared Gradle conventions (kotlin-conventions)
```

## Building and running

EPDB uses Gradle with the Kotlin DSL.

```bash
# Run the full build (compile + tests)
./gradlew build

# Run only the tests
./gradlew test

# Run the sample workload defined in application/Main.kt
./gradlew :application:run

# Run in verification mode (asserts the sample query result)
./gradlew :application:run --args="--verify"
```

The sample workload creates a `users` table (`id INT`, `name STRING`,
`age INT`), inserts 5,000 rows, then runs an index-scan + filter + projection
query against it.

## Design notes

A few deliberate choices that shape the codebase:

- **Interface-per-layer:** Every layer (`StorageManager`, `BufferManager`,
  `IndexManager`, `Catalog`) is an interface, with the in-memory implementation
  hidden behind a module-level object (`StorageModule`, `BufferModule`, …).
- **In-memory first:** Persistence is intentionally deferred so the project can
  iterate on the data structures, operator pipeline, and module boundaries
  without fighting durability concerns at the same time.
- **Volcano execution model:** Open / `next()` / close is the classic teaching
  model for query execution. It composes cleanly with Kotlin's
  `AutoCloseable` and keeps each operator small and independently testable.
- **Multi-module Gradle build:** Each layer is its own Gradle module with its
  own tests, which enforces dependency direction and makes the architecture
  visible from the build file itself.

## Roadmap

EPDB is a work in progress. Concrete next milestones:

- Disk-backed `StorageManager` with a real page cache and file format.
- Write-ahead logging and crash recovery.
- B-tree index to replace the in-memory hash index.
- A SQL parser and a basic cost-based query planner.
- Multi-statement transactions, isolation levels, and MVCC.
- Joins and aggregations.
- Deletes, updates, and on-page compaction.

## Further reading

Background that informed the design choices in EPDB:

- [CMU 15-445 / 15-721 — Database Systems](https://15445.courses.cs.cmu.edu/) (Andy Pavlo).
- *Database System Concepts*, Silberschatz, Korth, and Sudarshan.
- *Architecture of a Database System*, Hellerstein, Stonebraker, and Hamilton
  (Foundations and Trends in Databases, 2007).
