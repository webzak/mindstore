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
- Position u64 - chunk position in file
- Size u64 - chunk size
- Date u64 - unit datetime of last modification

### Chunk structure

Zero values for sizes mean that the appropriate blob is absent.

- data size u64
- meta size u32
- vector size u32
- data blob
- meta blog
- vector blob
