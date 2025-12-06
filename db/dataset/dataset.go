package dataset

import (
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
