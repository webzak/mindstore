package mindstore

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/webzak/mindstore/internal/data"
	"github.com/webzak/mindstore/internal/groups"
	"github.com/webzak/mindstore/internal/index"
	"github.com/webzak/mindstore/internal/tags"
	"github.com/webzak/mindstore/internal/vectors"
)

var (
	ErrDatasetExists   = errors.New("dataset already exists")
	ErrDatasetNotFound = errors.New("dataset is not found")
	ErrInvalidRecordID = errors.New("invalid record ID")
)

// Dataset represents indexed set of data with optional support for metadata, embeddings, tags and groups
type Dataset struct {
	path string
	name string

	data  *data.Data
	meta  *data.Data
	index *index.Index

	vectors *vectors.Vectors
	tags    *tags.Tags
	groups  *groups.Groups
}

// Group represents group membership information
type Group struct {
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
	Group *Group
}

// Open opens dataset
func Open(path, name string, opt Options) (*Dataset, error) {

	dir := filepath.Join(path, name)
	if err := ensureDir(dir); err != nil {
		return nil, err
	}

	ds := &Dataset{
		path: dir,
		name: name,
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
			item.Group = &Group{
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

// ensureDir ensures that the given path exists and is a directory
func ensureDir(path string) error {
	fileInfo, err := os.Stat(path)
	// Handle stat errors other than "not exist"
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to stat path: %w", err)
	}

	// Path doesn't exist, create it
	if os.IsNotExist(err) {
		if err = os.MkdirAll(path, 0755); err != nil {
			return fmt.Errorf("failed to create directory: %w", err)
		}
	}

	// Path exists, verify it's a directory
	if err == nil && !fileInfo.IsDir() {
		return fmt.Errorf("path exists but is not a directory: %s", path)
	}

	return nil
}
