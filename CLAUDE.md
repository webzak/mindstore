# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Security restrictions.

Never suggest or use `rm -rf` commands, ask user to clean the files.

## Overview

**mindstore** is a high-performance Go library for managing datasets with support for:
- Binary data and metadata storage
- Float32 vector embeddings (768-dimensional by default)
- Tags and groups for organization
- Lazy file creation and buffered writes for performance
- Process-level file locking for concurrent access safety

## Common Commands

### Testing
```bash
# Run all tests
go test ./...

# Run tests in a specific package
go test ./db/dataset
go test ./internal/data

# Run a specific test
go test ./db/dataset -run TestDatasetAppend

# Run tests with verbose output
go test -v ./...

# Run tests with coverage
go test -cover ./...
```

### Building
```bash
# Build (no executable - this is a library)
go build ./...

# Check for compilation errors
go build ./db/dataset
```

### Linting & Formatting
```bash
# Format code
go fmt ./...

# Run go vet
go vet ./...
```

## Architecture

### Four-Layer Storage Architecture

1. **Collection layer**
   Based on Dataset. Not implemented yet.


2. **Dataset Layer** (`db/dataset/`) - Public API
   - Provides high-level operations: Append, Read, Delete, Optimize
   - Coordinates all internal components
   - Manages process-level locking via `.lock` file
   - Thread-safe with mutex protection

3. **Internal Components** (`internal/`)
   - `data` - Variable-length binary storage with buffering
   - `index` - Fixed-size record index (40 bytes per record)
   - `vectors` - Fixed-size float32 vector storage
   - `tags` - Inverted index mapping tags to record IDs
   - `groups` - Group membership with ordering
   - `storage` - Low-level file operations (lazy creation)

4. **Storage Layer** (`internal/storage/`)
   - Handles raw file I/O with lazy file creation
   - Returns readers/writers/appenders positioned at offsets

### File Structure

Each dataset creates a directory with these files:
```
<dataset-name>/
  .lock              # Process lock file
  <name>.dat         # Variable-length data blobs
  <name>.met         # Variable-length metadata blobs
  <name>.idx         # Fixed-size index records (40 bytes each)
  <name>.vec         # Fixed-size float32 vectors
  <name>.tag         # Gob-encoded tag index
  <name>.grp         # Gob-encoded group membership
```

### Key Design Patterns

**Lazy File Creation**
- Files are not created until first write
- `storage.Init(false)` defers creation, `Init(true)` creates immediately
- Index files use `Init(true)`, others use `Init(false)`

**Append Buffering**
- `data`, `meta`, `index`, and `vectors` buffer writes in memory
- Auto-flush when buffer size exceeds configurable threshold
- Set buffer size to 0 for immediate writes (testing/debugging)
- Buffer sizes configured in `dataset.Options`

**Index-Centric Design**
- Each record has a sequential integer ID (its position in the index)
- Index stores offsets/sizes for data and metadata
- Index also stores descriptors (uint8) and flags (uint8)
- All components align by ID: `vectors[id]`, `tags[id]`, `groups[id]`

**Soft Deletion & Optimization**
- `Delete()` sets `index.MarkedForRemoval` flag
- `Optimize()` compacts by removing flagged records
- **CRITICAL**: After optimization, record IDs change (array compaction causes shifts)
  - Tags, vectors, and groups are automatically remapped to new IDs
  - External references to IDs become invalid after optimization
  - Applications should not cache record IDs across optimization calls

**Groups for Ordering**
- Groups allow ordered relationships between records
- Each record can belong to 0 or 1 group
- Within a group, records have a `place` (position)
- Use `GroupID: -1` on append to auto-create a new group

### Component Details

**Index (`internal/index/`)**
- Fixed 40-byte records stored in `<name>.idx`
- Structure: `[Offset:8][Size:8][MetaOffset:8][MetaSize:8][DataDesc:1][MetaDesc:1][Flags:1][Reserved:5]`
- `Offset` of -1 indicates empty/nil data
- Loads entire index into memory on open
- Supports in-place updates via `Replace()` and `SetFlags()`

**Data (`internal/data/`)**
- Variable-length storage for data and metadata
- Maintains reader FD for reuse (closed on `Close()`)
- Append buffer with configurable size threshold
- `Read()` handles buffered data transparently

**Vectors (`internal/vectors/`)**
- Fixed-size float32 vectors (default 768 dimensions)
- Stored contiguously: `[vec0][vec1][vec2]...`
- Buffered iteration via `Iterator()` (chunks by `MaxBufferSize`)
- `Delete()` requires flush, then reads all, rewrites without deleted

**Tags (`internal/tags/`)**
- Inverted index: tag → set of record IDs
- Stored as gob-encoded `map[string]map[int]bool`
- Lazy-loaded on first access
- `GetByTag()` returns IDs with a specific tag

**Groups (`internal/groups/`)**
- Maps GroupID → []Member (IndexID + Place)
- Stored as gob-encoded map
- Lazy-loaded on first access
- Auto-increments group IDs starting from 1

### ReadOptions Bitmask

`Read()` accepts `ReadOptions` bitflags for selective loading:
```go
ReadData    // Load data blob
ReadMeta    // Load metadata blob
ReadVector  // Load vector
ReadTags    // Load tags
ReadGroup   // Load group membership

// Example:
item, err := ds.Read(id, ReadData|ReadVector|ReadTags)
```

This enables efficient queries when you only need certain fields.

### Iterators

Go 1.23+ `iter.Seq2` iterators available:
- `DataIterator(opts ReadOptions)` - iterate over data blobs
- `MetaIterator()` - iterate over metadata
- `VectorIterator()` - iterate over vectors (buffered chunks)
- `index.Iterator()` - iterate over index rows

## Development Patterns

### Adding New Fields to Items

1. Update `Item` struct in `db/dataset/dataset.go`
2. Update `Append()` to write the new field
3. Update `Read()` to read the new field (consider adding a ReadOption flag)
4. Update relevant internal component (`data`, `index`, `vectors`, etc.)

### Modifying Storage Format

Storage formats are not versioned. Breaking changes require manual migration:
1. Test new format thoroughly
2. Consider writing a migration tool
3. Update relevant marshal/unmarshal logic

### Testing Approach

- Each package has `*_test.go` files
- `internal/testutil` provides test helpers
- Use temp directories for test datasets
- Always `defer ds.Close()` to cleanup locks

### Thread Safety

- Dataset methods use `ds.mu.Lock()` for thread safety
- Internal components are NOT thread-safe on their own
- Lock is held for entire operation duration

## Common Gotchas

1. **File locks persist until Close()** - Always defer `ds.Close()` or locks remain
2. **Record IDs change after Optimize()** - Don't cache IDs across optimization; all components (tags, vectors, groups) are automatically remapped to new IDs
3. **Empty data gets offset -1** - Check for -1 before reading data/meta
4. **Vector count may be less than index count** - Not all records have vectors
5. **Groups are 1-indexed** - GroupID 0 means "no group assigned"
6. **Tags and Groups are lazy-loaded** - First access triggers disk read
7. **Flushing is component-specific** - Each component tracks its own persistence state

## Performance Tuning

Adjust buffer sizes in `dataset.Options`:
- `MaxDataAppendBufferSize` (default: 128KB) - Data append buffer
- `MaxMetaDataAppendBufferSize` (default: 32KB) - Metadata append buffer
- `MaxIndexAppendBufferSize` (default: 64) - Index records before flush
- `MaxVectorAppendBufferSize` (default: 64) - Vectors before flush
- `MaxVectorBufferSize` (default: 64) - Vectors per iteration chunk

Set to 0 for immediate writes (testing/debugging).

