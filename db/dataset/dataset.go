package dataset

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"syscall"

	"github.com/webzak/mindstore/internal/data"
	"github.com/webzak/mindstore/internal/groups"
	"github.com/webzak/mindstore/internal/index"
	"github.com/webzak/mindstore/internal/storage"
	"github.com/webzak/mindstore/internal/tags"
	"github.com/webzak/mindstore/internal/vectors"
)

var (
	ErrDatasetExists   = errors.New("dataset already exists")
	ErrDatasetNotFound = errors.New("dataset is not found")
	ErrInvalidRecordID = errors.New("invalid record ID")
	ErrDatasetLocked   = errors.New("dataset is locked by another process")
	ErrDatasetClosed   = errors.New("dataset is closed")
)

// Dataset represents indexed set of data with optional support for metadata, embeddings, tags and groups
type Dataset struct {
	path string
	name string

	mu       sync.Mutex // Protects all operations
	lockFile *os.File   // Process-level lock file
	closed   bool       // Prevents use after close

	data  *data.Data
	meta  *data.Data
	index *index.Index

	vectors *vectors.Vectors
	tags    *tags.Tags
	groups  *groups.Groups
}

// Item represents a complete record with all its associated data
type Item struct {
	// ID is the record identifier
	ID int
	// Data is the main record data
	Data []byte
	// Metadata is the metadata for data
	Meta []byte
	// DataDescriptor is the type of data
	DataDescriptor uint8
	// MetaDescriptor db agnostic
	MetaDescriptor uint8
	// Flags
	Flags uint8
	// reserved
	_ uint8
	// Vector is the vector data
	Vector []float32
	// Tags is the list of tags
	Tags []string
	// GroupID is the group identifier (0 means no group assigned)
	GroupID int
	// GroupPlace is the position within the group
	GroupPlace int
}

// GetMeta retrieves and deserializes JSON metadata from an Item
// Returns empty map if no metadata is present
// Returns error if metadata cannot be unmarshaled
func (item *Item) GetMeta() (map[string]any, error) {
	if len(item.Meta) == 0 {
		return make(map[string]any), nil
	}

	var meta map[string]any
	if err := json.Unmarshal(item.Meta, &meta); err != nil {
		return nil, fmt.Errorf("failed to unmarshal metadata: %w", err)
	}

	return meta, nil
}

// GetMetaValue retrieves a specific metadata value by key
// Returns error if key not found or metadata cannot be unmarshaled
func (item *Item) GetMetaValue(key string) (any, error) {
	meta, err := item.GetMeta()
	if err != nil {
		return nil, err
	}

	value, exists := meta[key]
	if !exists {
		return nil, fmt.Errorf("metadata key %q not found", key)
	}

	return value, nil
}

// Open opens dataset or creates empty one
func Open(path, name string, opt Options) (*Dataset, error) {

	dir := filepath.Join(path, name)
	if err := storage.EnsureDir(dir); err != nil {
		return nil, err
	}

	// Create/open lock file
	lockPath := filepath.Join(dir, ".lock")
	lockFile, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return nil, fmt.Errorf("failed to open lock file: %w", err)
	}

	// Try to acquire exclusive lock
	err = syscall.Flock(int(lockFile.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
	if err != nil {
		lockFile.Close()
		return nil, ErrDatasetLocked
	}

	ds := &Dataset{
		path:     dir,
		name:     name,
		lockFile: lockFile,
	}

	// init data
	dt, err := data.New(filepath.Join(dir, name+".dat"), data.Options{
		MaxAppendBufferSize: int64(opt.MaxDataAppendBufferSize),
	})
	if err != nil {
		return nil, fmt.Errorf("failed init data storage: %w", err)
	}
	ds.data = dt

	// init metadata
	meta, err := data.New(filepath.Join(dir, name+".met"), data.Options{
		MaxAppendBufferSize: int64(opt.MaxMetaDataAppendBufferSize),
	})
	if err != nil {
		return nil, fmt.Errorf("failed init data storage: %w", err)
	}
	ds.meta = meta

	// init index
	index, err := index.New(filepath.Join(dir, name+".idx"), index.Options{
		MaxAppendBufferSize: opt.MaxIndexAppendBufferSize,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create index: %w", err)
	}
	ds.index = index

	// init vectors
	vectors, err := vectors.New(filepath.Join(dir, name+".vec"), vectors.Options{
		VectorSize:          opt.VectorSize,
		MaxBufferSize:       opt.MaxVectorBufferSize,
		MaxAppendBufferSize: opt.MaxVectorAppendBufferSize,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to init vectors: %w", err)
	}
	ds.vectors = vectors

	// init tags
	tags, err := tags.New(filepath.Join(dir, name+".tag"))
	if err != nil {
		return nil, fmt.Errorf("failed to init tags: %w", err)
	}
	ds.tags = tags

	// init groups
	groups, err := groups.New(filepath.Join(dir, name+".grp"))
	if err != nil {
		return nil, fmt.Errorf("failed to init groups: %w", err)
	}
	ds.groups = groups
	return ds, nil
}

// IsPersisted returns true when all data is saved
func (c *Dataset) IsPersisted() bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.data.IsPersisted() &&
		c.meta.IsPersisted() &&
		c.index.IsPersisted() &&
		c.vectors.IsPersisted() &&
		c.tags.IsPersisted() &&
		c.groups.IsPersisted()
}

// Flush persists all in-memory changes to storage
func (c *Dataset) Flush() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return ErrDatasetClosed
	}

	if err := c.data.Flush(); err != nil {
		return fmt.Errorf("failed to flush data: %w", err)
	}
	if err := c.meta.Flush(); err != nil {
		return fmt.Errorf("failed to flush metadata: %w", err)
	}
	if err := c.index.Flush(); err != nil {
		return fmt.Errorf("failed to flush index: %w", err)
	}
	if err := c.vectors.Flush(); err != nil {
		return fmt.Errorf("failed to flush vectors: %w", err)
	}
	if err := c.tags.Flush(); err != nil {
		return fmt.Errorf("failed to flush tags: %w", err)
	}
	if err := c.groups.Flush(); err != nil {
		return fmt.Errorf("failed to flush groups: %w", err)
	}
	return nil
}

// Close closes the dataset and all its components
func (c *Dataset) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return ErrDatasetClosed
	}

	// Close all components
	if err := c.data.Close(); err != nil {
		return fmt.Errorf("failed to close data: %w", err)
	}
	if err := c.meta.Close(); err != nil {
		return fmt.Errorf("failed to close metadata: %w", err)
	}
	if err := c.index.Close(); err != nil {
		return fmt.Errorf("failed to close index: %w", err)
	}
	if err := c.vectors.Close(); err != nil {
		return fmt.Errorf("failed to close vectors: %w", err)
	}
	if err := c.tags.Close(); err != nil {
		return fmt.Errorf("failed to close tags: %w", err)
	}
	if err := c.groups.Close(); err != nil {
		return fmt.Errorf("failed to close groups: %w", err)
	}

	c.closed = true

	// Release file lock
	if c.lockFile != nil {
		syscall.Flock(int(c.lockFile.Fd()), syscall.LOCK_UN)
		c.lockFile.Close()
	}

	return nil
}

// ClearVectors removes all vectors and updates index to reflect removal
// This is used when changing vector dimensions or explicitly clearing embeddings
func (c *Dataset) ClearVectors() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return ErrDatasetClosed
	}

	// Step 1: Truncate vectors storage (removes all vector data)
	if err := c.vectors.Truncate(); err != nil {
		return fmt.Errorf("failed to truncate vectors: %w", err)
	}

	// Step 2: Update all index rows to mark vectors as removed (Vector = -1)
	for i := 0; i < c.index.Count(); i++ {
		row, err := c.index.Get(i)
		if err != nil {
			return fmt.Errorf("failed to get index row %d: %w", i, err)
		}

		// Only update if row had a vector
		if row.Vector != -1 {
			row.Vector = -1
			if err := c.index.Replace(i, row); err != nil {
				return fmt.Errorf("failed to update index row %d: %w", i, err)
			}
		}
	}

	// Step 3: Ensure all changes are persisted
	if err := c.index.Flush(); err != nil {
		return fmt.Errorf("failed to flush index: %w", err)
	}

	return nil
}

// Truncate removes all data from the dataset
func (c *Dataset) Truncate() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return ErrDatasetClosed
	}

	if err := c.data.Truncate(); err != nil {
		return fmt.Errorf("failed to truncate data: %w", err)
	}
	if err := c.meta.Truncate(); err != nil {
		return fmt.Errorf("failed to truncate metadata: %w", err)
	}
	if err := c.index.Truncate(); err != nil {
		return fmt.Errorf("failed to truncate index: %w", err)
	}
	if err := c.vectors.Truncate(); err != nil {
		return fmt.Errorf("failed to truncate vectors: %w", err)
	}
	if err := c.tags.Truncate(); err != nil {
		return fmt.Errorf("failed to truncate tags: %w", err)
	}
	if err := c.groups.Truncate(); err != nil {
		return fmt.Errorf("failed to truncate groups: %w", err)
	}
	return nil
}

// Count returns the number of records in the collection
func (c *Dataset) Count() int {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.index.Count()
}

// Stats contains dataset statistics
type Stats struct {
	TotalRecords        int
	RecordsWithTags     int
	RecordsWithMetadata int
	RecordsWithGroups   int
	RecordsWithVectors  int
	TagCounts           map[string]int // tag -> count of records
	TotalGroups         int
}

// GetStats returns statistics about the dataset
func (c *Dataset) GetStats() (*Stats, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	stats := &Stats{
		TotalRecords: c.index.Count(),
	}

	// Count records with tags
	tagCount, err := c.tags.Count()
	if err != nil {
		return nil, fmt.Errorf("failed to count tags: %w", err)
	}
	stats.RecordsWithTags = tagCount

	// Get tag counts (tag -> number of records using that tag)
	tagCounts, err := c.tags.GetTagCounts()
	if err != nil {
		return nil, fmt.Errorf("failed to get tag counts: %w", err)
	}
	stats.TagCounts = tagCounts

	// Count records with groups
	groupCount, err := c.groups.Count()
	if err != nil {
		return nil, fmt.Errorf("failed to count groups: %w", err)
	}
	stats.RecordsWithGroups = groupCount

	// Get total number of groups
	totalGroups, err := c.groups.GetGroupCount()
	if err != nil {
		return nil, fmt.Errorf("failed to get group count: %w", err)
	}
	stats.TotalGroups = totalGroups

	// Count records with metadata and vectors (requires index iteration)
	metaCount := 0
	vectorCount := 0
	for _, row := range c.index.Iterator() {
		if row.MetaOffset != -1 {
			metaCount++
		}
		if row.Vector != -1 {
			vectorCount++
		}
	}
	stats.RecordsWithMetadata = metaCount
	stats.RecordsWithVectors = vectorCount

	return stats, nil
}
