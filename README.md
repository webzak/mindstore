# mindstore

A high-performance Go library and CLI tool for managing datasets with support for binary data, metadata, and vector embeddings.

## Features

- **Binary Data Storage**: Efficient variable-length storage for data and metadata
- **Vector Embeddings**: Float32 vector storage (768-dimensional by default) with similarity search
- **Organization**: Tags and groups for flexible data organization
- **Collection Layer**: High-level semantic API with embedder configuration management
- **Vector Similarity Search**: Cosine distance-based search across embeddings
- **CLI Tool**: Command-line interface (mindb) for collection management
- **Performance**: Lazy file creation and buffered writes for optimal performance
- **Concurrency Safety**: Process-level file locking for safe concurrent access

## Installation

### As a Library

```bash
go get github.com/yourusername/mindstore
```

### CLI Tool

```bash
# Clone the repository
git clone https://github.com/yourusername/mindstore.git
cd mindstore

# Build the CLI
go build -o mindb ./cmd/mindb

# Install globally (optional)
go install ./cmd/mindb
```

## Quick Start

### Using the CLI

```bash
# Create a collection with an embedder
mindb collection create mydata --embedder llamacpp-text --description "My dataset"

# Add text to the collection
mindb collection append mydata --text "Hello, world!"

# List all items
mindb collection list mydata

# Get collection info and statistics
mindb collection info mydata

# Search by vector similarity
mindb collection vector-search mydata --text "query text" --limit 10

# List all collections
mindb list
```

### Using the Library

```go
package main

import (
    "github.com/yourusername/mindstore/db/collection"
    "github.com/yourusername/mindstore/db/dataset"
)

func main() {
    // Create a new collection
    cfg := collection.Config{
        DatasetOptions: dataset.Options{
            VectorSize: 768,
        },
        Description: "My dataset",
    }
    coll, err := collection.CreateCollection("/data", "mydata", cfg)
    if err != nil {
        panic(err)
    }
    defer coll.Close()

    // Add text with metadata and tags
    coll.AddText("Hello, world!").
        WithMeta("author", "Alice").
        WithMeta("created", "2025-01-15").
        WithTag("greeting").
        Apply()

    // Read an item
    item, err := coll.Read(0, collection.ReturnAll)
    if err != nil {
        panic(err)
    }

    // Vector similarity search
    results, err := coll.VectorSearch(queryVector, collection.VectorSearchOptions{
        SortOrder: collection.SortAscending,
        Limit:     10,
    })
}
```

## Architecture

### Four-Layer Storage Architecture

1. **Collection Layer** (`db/collection/`)
   - High-level semantic API for text and metadata
   - Fluent builder pattern for item creation
   - Embedder configuration management
   - Vector similarity search

2. **Dataset Layer** (`db/dataset/`)
   - Public API for core operations
   - Process-level locking
   - Thread-safe operations

3. **Internal Components** (`internal/`)
   - `data` - Variable-length binary storage
   - `index` - Fixed-size record index (40 bytes per record)
   - `vectors` - Float32 vector storage
   - `tags` - Inverted index for tag-based queries
   - `groups` - Group membership with ordering

4. **Storage Layer** (`internal/storage/`)
   - Low-level file I/O with lazy creation
   - Positioned readers/writers/appenders

### File Structure

Each collection creates a directory with these files:

```
<collection-name>/
  .lock              # Process lock file
  <name>.json        # Collection configuration (embedders, description)
  <name>.dat         # Variable-length data blobs (text content)
  <name>.met         # Variable-length metadata blobs (JSON)
  <name>.idx         # Fixed-size index records (40 bytes each)
  <name>.vec         # Float32 vectors (embeddings)
  <name>.tag         # Gob-encoded tag index
  <name>.grp         # Gob-encoded group membership
```

## CLI Commands

### Configuration

Default settings stored in `~/.config/mindb/config.json`:

```json
{
  "path": "/data/collections"
}
```

### Top-Level Commands

- `mindb list [--path <path>]` - List all collections with statistics
- `mindb collection <subcommand>` - Collection management commands

### Collection Subcommands

- `create <name> [options]` - Create new collection
  - `--embedder <name>` - Add embedder (repeatable)
  - `--vector-size <n>` - Set vector dimensions (default: 768)
  - `--description <text>` - Set description

- `feed <name> --file <path> --parser <type>` - Import data from files
- `append <name> --text <content>` - Append single text item
- `list <name>` - List all records with previews
- `read <name> <id>` - Read specific record by ID
- `info <name>` - Display collection statistics
- `rows <name>` - Display records in tabular format
- `delete <name>` - Delete entire collection (with confirmation)
- `create-embeddings <name>` - Generate embeddings for all items
- `clear-embeddings <name>` - Remove all vectors
- `edit-config <name>` - Modify collection configuration
- `vector-search <name> --text <query> [options]` - Similarity search
  - `--limit <n>` - Maximum results to return

## Development

### Testing

```bash
# Run all tests
go test ./...

# Run tests in a specific package
go test ./db/dataset
go test ./internal/data

# Run with coverage
go test -cover ./...

# Run with verbose output
go test -v ./...
```

### Building

```bash
# Build all packages
go build ./...

# Build CLI tool
go build -o mindb ./cmd/mindb

# Format code
go fmt ./...

# Run linter
go vet ./...
```

### Key Design Patterns

**Lazy File Creation**
- Files created only on first write
- Improves performance for read-only operations

**Append Buffering**
- In-memory buffering with configurable thresholds
- Auto-flush when buffer size exceeded
- Configurable via `dataset.Options`

**Index-Centric Design**
- Sequential integer IDs (position in index)
- All components aligned by ID
- Enables efficient random access

**Soft Deletion & Optimization**
- `Delete()` sets flag, doesn't remove immediately
- `Optimize()` compacts by removing flagged records
- **Note**: Record IDs change after optimization

## Performance Tuning

Adjust buffer sizes in `dataset.Options`:

```go
opts := dataset.Options{
    MaxDataAppendBufferSize:     128 * 1024,  // 128KB
    MaxMetaDataAppendBufferSize: 32 * 1024,   // 32KB
    MaxIndexAppendBufferSize:    64,          // 64 records
    MaxVectorAppendBufferSize:   64,          // 64 vectors
    VectorSize:                  768,         // Dimensions
}
```

Set buffer sizes to 0 for immediate writes (useful for testing/debugging).

## Important Notes

- Always `defer coll.Close()` to release file locks
- Record IDs change after `Optimize()` - don't cache IDs across optimization
- Empty data gets offset -1 in index
- Vector count may be less than record count (not all records have vectors)
- Thread-safe with mutex protection
- Process-level locking prevents concurrent access

## License

MIT
