## Dataset file structure

### File strucrue

- 4 magic bytes 0x19 0x72 0x06 0x11
- config size u32
- config body []byte (messagepack format internally)
- index capacity u32 (the amoutn of real and reserved records)
- index length u32 (real amount of index records)
- index space (contains index records, format is described below and spare space according to index capacity)
- data space (contains data chunks, format is described below)

### Index record structure

- ID u32 - chunk id
- Flags u8
- Data descriptor u8
- Meta descriptor u8
- Vector descriptor u8
- Position u64 - chunk position relative to data space start (not file start)
- Size u64 - chunk size
- Date u64 - unit datetime of last modification

#### Index flags

bit 0 - if set to 1 it means that record is deleted, the flag has to be checked on read operations.

### Chunk structure

Zero values for sizes mean that the appropriate blob is absent.

- data size u64
- meta size u32
- vector size u32
- data blob
- meta blob
- vector blob

## Implementation

### Overview

- Single-file storage for chunks with data, meta, and vector blobs
- In-memory index for fast lookups by ID
- Append-only data writes (updates append new chunk, old data becomes garbage until optimization)

### Operations

- **Append** - Add new chunk, auto-assign sequential ID, write chunk to end of file
- **Read** - Lookup by ID from in-memory index, supports selective field loading (Data, Meta, Vector)
- **Update** - Merge provided fields with existing chunk, append new chunk data to end of file
- **Delete** - Soft delete via index flag, data remains in file until optimization
- **List** - Pipeline-based iterator with filter stages and selective field loading

### Index management

- Index is loaded into memory on dataset open
- Deleted records (flag bit 0 set) are excluded from in-memory index
- Index capacity auto-expands (doubles) when full during Append
- ChangeIndexCap rewrites entire file to resize index space

### Concurrency

- Single mutex protects all operations
- List iterator holds lock for entire iteration duration
