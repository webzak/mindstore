package db

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/webzak/mindstore/internal/engine/data"
	"github.com/webzak/mindstore/internal/engine/groups"
	"github.com/webzak/mindstore/internal/engine/index"
	"github.com/webzak/mindstore/internal/engine/tags"
	"github.com/webzak/mindstore/internal/engine/vectors"
)

var (
	ErrDatasetExists   = errors.New("dataset already exists")
	ErrDatasetNotFound = errors.New("dataset is not found")
	ErrInvalidRecordID = errors.New("invalid record ID")
)

// DatasetConfig defines the configuration for a collection
type DatasetConfig struct {
	// MaxVectorAppendBufferSize max buffer for index to be unsynced
	MaxIndexAppendBufferSize int `json:"max_index_append_buffer_size,omitempty"`
	// VectorSize is the size of the float32 vector
	VectorSize int `json:"vector_size,omitempty"`
	// MaxVectorBufferSize is the maximum amount of vectors in memory buffer
	MaxVectorBufferSize int `json:"max_vector_buffer_size,omitempty"`
	// MaxAppendBufferSize is the maximum amount of appended vectors which triggers flush
	MaxVectorAppendBufferSize int `json:"max_vector_append_buffer_size,omitempty"`
}

// Dataset represents a database collection
type Dataset struct {
	path   string
	name   string
	config DatasetConfig

	data  *data.Data
	meta  *data.Data
	index *index.Index

	vectors *vectors.Vectors
	tags    *tags.Tags
	groups  *groups.Groups
}

// DatasetGroup represents group membership information
type DatasetGroup struct {
	// ID is the group identifier
	ID int
	// Place is the position within the group
	Place int
}

// DatasetItem represents a complete record with all its associated data
type DatasetItem struct {
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
	// Group contains group membership information
	Group *DatasetGroup
}

// ReadOptions specifies which components of an Item to read using bitmask flags.
// If ReadOptions is 0, only the core Data field will be populated.
type ReadOptions uint8

const (
	// ReadData indicates whether to read data
	ReadData ReadOptions = 1 << iota
	// ReadMeta indicates whether to read metadata
	ReadMeta
	// ReadVector indicates whether to read vector data
	ReadVector
	// ReadTags indicates whether to read tags
	ReadTags
	// ReadGroup indicates whether to read group information
	ReadGroup
)

// Has checks if a specific option is set
func (r ReadOptions) Has(flag ReadOptions) bool {
	return r&flag != 0
}

// AllReadOptions returns ReadOptions with all fields set to true
func AllReadOptions() ReadOptions {
	return ReadData | ReadMeta | ReadVector | ReadTags | ReadGroup
}

// CreateDataset creates new directory named as dataset name and saves the config
func CreateDataset(path, name string, config DatasetConfig) error {
	dir := filepath.Join(path, name)

	if _, err := os.Stat(dir); err == nil {
		if err = os.MkdirAll(dir, 0755); err != nil {
			return fmt.Errorf("failed to create directory: %w", err)
		}
	}
	configPath := filepath.Join(dir, name+"ds.json")
	existingConfig, err := loadConfig(configPath)
	if err != nil {
		return saveConfig(configPath, config)
	}
	if config == existingConfig {
		return nil
	}
	return errors.New("collection exists with different configuation")
}

// OpenDataset opens dataset
func OpenDataset(path, name string) (*Dataset, error) {
	dir := filepath.Join(path, name)

	config, err := loadConfig(filepath.Join(dir, name+"ds.json"))
	if err != nil {
		return nil, err
	}

	c := &Dataset{
		path:   path,
		name:   name,
		config: config,
	}

	// init data
	ds, err := data.New(filepath.Join(dir, name+".dat"))
	if err != nil {
		return nil, fmt.Errorf("failed init data storage: %w", err)
	}
	c.data = ds

	// init metadata
	meta, err := data.New(filepath.Join(dir, name+".met"))
	if err != nil {
		return nil, fmt.Errorf("failed init data storage: %w", err)
	}
	c.meta = meta

	// init index
	indexOptions := index.DefaultIndexOptions()
	if config.MaxIndexAppendBufferSize > 0 {
		indexOptions.MaxAppendBufferSize = config.MaxIndexAppendBufferSize
	}

	index, err := index.New(filepath.Join(dir, name+".idx"), &indexOptions)
	if err != nil {
		return nil, fmt.Errorf("failed to create index: %w", err)
	}
	c.index = index

	// init vectors
	vectorsOptions := vectors.DefaultVectorsOptions()
	if config.VectorSize > 0 {
		vectorsOptions.VectorSize = config.VectorSize
	}
	if config.MaxVectorBufferSize > 0 {
		vectorsOptions.MaxBufferSize = config.MaxVectorBufferSize
	}
	if config.MaxVectorAppendBufferSize > 0 {
		vectorsOptions.MaxAppendBufferSize = config.MaxIndexAppendBufferSize
	}

	vectors, err := vectors.New(filepath.Join(dir, name+".vec"), &vectorsOptions)
	if err != nil {
		return nil, fmt.Errorf("failed to init vectors: %w", err)
	}
	c.vectors = vectors

	// init tags
	tags, err := tags.New(filepath.Join(dir, name+".tag"))
	if err != nil {
		return nil, fmt.Errorf("failed to init tags: %w", err)
	}
	c.tags = tags

	// init groups
	groups, err := groups.New(filepath.Join(dir, name+".grp"))
	if err != nil {
		return nil, fmt.Errorf("failed to init groups: %w", err)
	}
	c.groups = groups
	return c, nil
}

// IsPersisted returns true when all data is saved
func (c *Dataset) IsPersisted() bool {
	return c.index.IsPersisted() &&
		c.vectors.IsPersisted() &&
		c.tags.IsPersisted() &&
		c.groups.IsPersisted()
}

// Flush persists all in-memory changes to storage
func (c *Dataset) Flush() error {
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

// Append new item to datset
func (c *Dataset) Append(item *DatasetItem) (int, error) {

	// Append data to storage
	offset, size, err := c.data.Append(item.Data)
	if err != nil {
		return 0, fmt.Errorf("failed to append data: %w", err)
	}

	// Append meta data to storage
	metaOffset, metaSize, err := c.meta.Append(item.Meta)
	if err != nil {
		return 0, fmt.Errorf("failed to append metadata: %w", err)
	}

	// Add index entry
	row := index.Row{
		Offset:             offset,
		Size:               size,
		MetaOffset:         metaOffset,
		MetaSize:           metaSize,
		DataDescriptor:     item.DataDescriptor,
		MetaDataDescriptor: item.MetaDescriptor,
		Flags:              item.Flags,
	}
	id, err := c.index.Append(row)
	if err != nil {
		return 0, fmt.Errorf("failed to add index entry: %w", err)
	}

	// Get the new record ID (current count - 1)
	recordID := c.index.Count() - 1

	if len(item.Vector) > 0 {
		if len(item.Vector) != c.config.VectorSize {
			return 0, fmt.Errorf("vector length mismatch: expected %d, got %d", c.config.VectorSize, len(item.Vector))
		}

		// Append vector
		if err := c.vectors.Append(id, item.Vector); err != nil {
			return 0, fmt.Errorf("failed to append vector: %w", err)
		}
	}

	// Add tags if tags are enabled
	if c.tags != nil && len(item.Tags) > 0 {
		for _, tag := range item.Tags {
			c.tags.Add(recordID, tag)
		}
	}

	// Assign to group if groups are enabled
	if c.groups != nil && item.Group != nil && item.Group.ID > 0 {
		if err := c.groups.Assign(item.Group.ID, recordID, item.Group.Place); err != nil {
			return 0, fmt.Errorf("failed to assign to group: %w", err)
		}
	}

	// Set the item ID to the generated record ID
	item.ID = recordID

	return recordID, nil
}

// Read retrieves a record by ID and returns it as an Item.
// If opts is 0, only the index record (ID, descriptors, flags) will be populated.
// Otherwise, optional components are loaded based on the opts flags.
func (c *Dataset) Read(id int, opts ReadOptions) (*DatasetItem, error) {
	// Get index entry
	row, err := c.index.Get(id)
	if err != nil {
		return nil, fmt.Errorf("failed to get index entry: %w", err)
	}

	// Create the item with index record data
	item := &DatasetItem{
		ID:             id,
		DataDescriptor: row.DataDescriptor,
		MetaDescriptor: row.MetaDataDescriptor,
		Flags:          row.Flags,
	}

	// If opts is 0, return only the index record
	if opts == 0 {
		return item, nil
	}

	// Read data if requested
	if opts.Has(ReadData) {
		payload, err := c.data.Read(row.Offset, row.Size)
		if err != nil {
			return nil, fmt.Errorf("failed to read data: %w", err)
		}
		item.Data = payload
	}

	// Read metadata if requested and enabled
	if opts.Has(ReadMeta) && c.meta != nil {
		payload, err := c.meta.Read(row.Offset, row.Size)
		if err != nil {
			return nil, fmt.Errorf("failed to read data: %w", err)
		}
		item.Meta = payload
	}

	// Read vector if requested and enabled
	if opts.Has(ReadVector) && c.vectors != nil {
		vector, err := c.vectors.Get(id)
		if err != nil {
			return nil, fmt.Errorf("failed to read vector: %w", err)
		}
		item.Vector = vector
	}

	// Read tags if requested and enabled
	if opts.Has(ReadTags) && c.tags != nil {
		item.Tags, err = c.tags.GetTags(id)
		if err != nil {
			return nil, err
		}
		if item.Tags == nil {
			item.Tags = []string{}
		}
	}

	// Read group information if requested and enabled
	if opts.Has(ReadGroup) && c.groups != nil {
		groupID, err := c.groups.GetGroup(id) // -1 means no group assinged
		if err != nil {
			return nil, err
		}
		if groupID >= 0 {
			// Find the place/position within the group
			members, err := c.groups.GetMembers(groupID)
			if err != nil {
				return nil, err
			}
			place := -1
			for i, memberID := range members {
				if memberID == id {
					place = i
					break
				}
			}
			item.Group = &DatasetGroup{
				ID:    groupID,
				Place: place,
			}
		}
	}

	return item, nil
}

// Count returns the number of records in the collection
func (c *Dataset) Count() int {
	return c.index.Count()
}

// Config returns the collection configuration
func (c *Dataset) Config() DatasetConfig {
	return c.config
}
