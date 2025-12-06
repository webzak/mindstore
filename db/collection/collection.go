package collection

import (
	"fmt"
	"path/filepath"

	"github.com/webzak/mindstore/db/dataset"
	"github.com/webzak/mindstore/embeddings"
	"github.com/webzak/mindstore/internal/storage"
)

type DataType uint8

const (
	Text  DataType = 1
	Image DataType = 2
)

// Collection represents a collection that builds on top of Dataset
type Collection struct {
	path string // Collection directory path
	name string // Collection name

	dataset   *dataset.Dataset
	embedders map[string]embeddings.Embedder // Named embedder instances (set by user)
	cfg       config                         // Internal collection configuration
}

// CreateCollection creates a new collection with the given options
func CreateCollection(path, name string, opts Options) (*Collection, error) {
	dir := filepath.Join(path, name)
	if err := storage.EnsureDir(dir); err != nil {
		return nil, err
	}

	// Convert Options to internal config and save to <name>.json
	cfg, err := opts.toConfig()
	if err != nil {
		return nil, fmt.Errorf("failed to convert options to config: %w", err)
	}

	if err := saveConfig(dir, name, cfg); err != nil {
		return nil, err
	}

	// Open dataset with config options
	ds, err := dataset.Open(path, name, opts.DatasetOptions)
	if err != nil {
		return nil, fmt.Errorf("failed to open dataset: %w", err)
	}

	coll := &Collection{
		path:      path,
		name:      name,
		dataset:   ds,
		embedders: make(map[string]embeddings.Embedder),
		cfg:       cfg,
	}

	return coll, nil
}

// OpenCollection opens an existing collection and loads its config
func OpenCollection(path, name string) (*Collection, error) {
	dir := filepath.Join(path, name)

	// Load internal config from <name>.json
	cfg, err := loadConfig(dir, name)
	if err != nil {
		return nil, err
	}

	// Open dataset with loaded config options
	ds, err := dataset.Open(path, name, cfg.DatasetOptions)
	if err != nil {
		return nil, fmt.Errorf("failed to open dataset: %w", err)
	}

	coll := &Collection{
		path:      path,
		name:      name,
		dataset:   ds,
		embedders: make(map[string]embeddings.Embedder),
		cfg:       cfg,
	}

	return coll, nil
}

// Close closes the collection and releases the process lock
func (c *Collection) Close() error {
	if c.dataset != nil {
		return c.dataset.Close()
	}
	return nil
}

// GetEmbeddersConfig returns all embedder configurations as map[string]any
func (c *Collection) GetEmbeddersConfig() (map[string]any, error) {
	opts, err := c.cfg.toOptions()
	if err != nil {
		return nil, fmt.Errorf("failed to convert config to options: %w", err)
	}
	return opts.Embedders, nil
}

// IsPersisted returns true when all data is saved to disk
func (c *Collection) IsPersisted() bool {
	if c.dataset != nil {
		return c.dataset.IsPersisted()
	}
	return true
}

// Count returns the number of records in the collection
func (c *Collection) Count() int {
	if c.dataset != nil {
		return c.dataset.Count()
	}
	return 0
}

// Flush persists all in-memory changes to storage
func (c *Collection) Flush() error {
	if c.dataset != nil {
		return c.dataset.Flush()
	}
	return nil
}

// Truncate removes all data from the collection
func (c *Collection) Truncate() error {
	if c.dataset != nil {
		return c.dataset.Truncate()
	}
	return nil
}
