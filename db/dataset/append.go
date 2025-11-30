package dataset

import (
	"fmt"

	"github.com/webzak/mindstore/internal/index"
)

// Append new item to datset
func (ds *Dataset) Append(item *Item) (int, error) {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	if ds.closed {
		return 0, ErrDatasetClosed
	}

	// Append data to storage
	offset, size, err := ds.data.Append(item.Data)
	if err != nil {
		return 0, fmt.Errorf("failed to append data: %w", err)
	}

	// Append meta data to storage
	metaOffset, metaSize, err := ds.meta.Append(item.Meta)
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
	id, err := ds.index.Append(row)
	if err != nil {
		return 0, fmt.Errorf("failed to add index entry: %w", err)
	}

	if len(item.Vector) > 0 {
		// Append vector
		if err := ds.vectors.Append(id, item.Vector); err != nil {
			return 0, fmt.Errorf("failed to append vector: %w", err)
		}
	}

	// Add tags if tags are enabled
	if ds.tags != nil && len(item.Tags) > 0 {
		for _, tag := range item.Tags {
			ds.tags.Add(id, tag)
		}
	}

	// Assign to group if groups are enabled
	if ds.groups != nil && item.GroupID > 0 {
		if err := ds.groups.Assign(item.GroupID, id, item.GroupPlace); err != nil {
			return 0, fmt.Errorf("failed to assign to group: %w", err)
		}
	}

	// Set the item ID to the generated record ID
	item.ID = id

	return id, nil
}
