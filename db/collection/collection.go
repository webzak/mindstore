package collection

import (
	"fmt"
	"path/filepath"

	"github.com/webzak/mindstore/db/dataset"
	"github.com/webzak/mindstore/internal/storage"
)

type DataType uint8

const (
	Text  DataType = 1
	Image DataType = 2
)

// ReadOptions is a bitmask for controlling what data to load when reading items
type ReadOptions uint8

const (
	ReturnVector ReadOptions = 1 << iota // Load vector data
)

func (r ReadOptions) has(flag ReadOptions) bool {
	return r&flag != 0
}

// Collection represents a collection that builds on top of Dataset
type Collection struct {
	path string // Collection directory path
	name string // Collection name

	dataset *dataset.Dataset
	cfg     config // Internal collection configuration
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
		path:    path,
		name:    name,
		dataset: ds,
		cfg:     cfg,
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
		path:    path,
		name:    name,
		dataset: ds,
		cfg:     cfg,
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

// datasetItemToCollectionItem converts a dataset.Item to a collection.Item
// Deserializes JSON metadata to map[string]any
func (c *Collection) datasetItemToCollectionItem(dsItem *dataset.Item) (*Item, error) {
	item := &Item{
		collection:     c,
		data:           dsItem.Data,
		dataDescriptor: DataType(dsItem.DataDescriptor),
		metaDescriptor: dsItem.MetaDescriptor,
		flags:          dsItem.Flags,
		vector:         dsItem.Vector,
		tags:           dsItem.Tags,
		groupID:        dsItem.GroupID,
		groupPlace:     dsItem.GroupPlace,
	}

	// Deserialize metadata from JSON bytes to map
	if len(dsItem.Meta) > 0 {
		meta, err := dsItem.GetMeta()
		if err != nil {
			return nil, fmt.Errorf("failed to deserialize metadata: %w", err)
		}
		item.meta = meta
	}

	return item, nil
}

// Read retrieves an item from the collection by ID
// Returns a collection.Item with data, metadata (as map[string]any), tags, and group info
// Vector is empty by default unless ReturnVector option is specified
func (c *Collection) Read(id int, opts ReadOptions) (*Item, error) {
	// Build dataset read options - always read data, meta, tags, and group
	// but exclude vector unless explicitly requested
	dsOpts := dataset.ReadData | dataset.ReadMeta | dataset.ReadTags | dataset.ReadGroup
	if opts.has(ReturnVector) {
		dsOpts |= dataset.ReadVector
	}

	// Read from underlying dataset
	dsItem, err := c.dataset.Read(id, dsOpts)
	if err != nil {
		return nil, err
	}

	// Convert to collection.Item
	return c.datasetItemToCollectionItem(dsItem)
}

// Stats contains collection-level statistics including metadata key counts
type Stats struct {
	*dataset.Stats
	MetadataKeyCounts map[string]int // metadata key -> count of records
}

// GetStats returns statistics about the collection including metadata key analysis
func (c *Collection) GetStats() (*Stats, error) {
	// Get base dataset stats
	dsStats, err := c.dataset.GetStats()
	if err != nil {
		return nil, err
	}

	collStats := &Stats{
		Stats:             dsStats,
		MetadataKeyCounts: make(map[string]int),
	}

	// Analyze metadata keys (Collection-level only, as Dataset is data-agnostic)
	if dsStats.RecordsWithMetadata > 0 {
		metaKeyCounts, err := c.getMetadataKeyCounts()
		if err != nil {
			return nil, fmt.Errorf("failed to analyze metadata keys: %w", err)
		}
		collStats.MetadataKeyCounts = metaKeyCounts
	}

	return collStats, nil
}

// getMetadataKeyCounts iterates through all records and counts metadata key usage
func (c *Collection) getMetadataKeyCounts() (map[string]int, error) {
	counts := make(map[string]int)
	totalRecords := c.dataset.Count()

	for id := 0; id < totalRecords; id++ {
		// Read only metadata (not data, vector, tags, or groups)
		dsItem, err := c.dataset.Read(id, dataset.ReadMeta)
		if err != nil {
			continue // Skip records that can't be read
		}

		// Skip if no metadata
		if len(dsItem.Meta) == 0 {
			continue
		}

		// Deserialize metadata
		meta, err := dsItem.GetMeta()
		if err != nil {
			continue // Skip records with invalid metadata
		}

		// Count each key
		for key := range meta {
			counts[key]++
		}
	}

	return counts, nil
}

// SetVector updates or sets the vector for an existing record by index
// Delegates to the underlying dataset's SetVector method
func (c *Collection) SetVector(id int, vector []float32) error {
	if c.dataset == nil {
		return fmt.Errorf("collection dataset is not initialized")
	}
	return c.dataset.SetVector(id, vector)
}
