package dataset

import (
	"fmt"

	"github.com/webzak/mindstore/internal/index"
)

// Append new item to datset
func (ds *Dataset) Append(item Item) (*Item, error) {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	if ds.closed {
		return nil, ErrDatasetClosed
	}

	// Append data to storage
	offset, size, err := ds.data.Append(item.Data)
	if err != nil {
		return nil, fmt.Errorf("failed to append data: %w", err)
	}

	// Append meta data to storage
	metaOffset, metaSize, err := ds.meta.Append(item.Meta)
	if err != nil {
		return nil, fmt.Errorf("failed to append metadata: %w", err)
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
		return nil, fmt.Errorf("failed to add index entry: %w", err)
	}

	if len(item.Vector) > 0 {
		// Append vector
		if err := ds.vectors.Append(id, item.Vector); err != nil {
			return nil, fmt.Errorf("failed to append vector: %w", err)
		}
	}

	// Add tags
	if len(item.Tags) > 0 {
		for _, tag := range item.Tags {
			ds.tags.Add(id, tag)
		}
	}

	// Handle group assignment
	groupID := item.GroupID
	groupPlace := item.GroupPlace
	if item.GroupID == -1 {
		// Create a new group with this item as the first member
		newGroupID, err := ds.groups.CreateGroup(id)
		if err != nil {
			return nil, fmt.Errorf("failed to create group: %w", err)
		}
		groupID = newGroupID
		groupPlace = 0
	} else if item.GroupID > 0 {
		// Assign to existing group
		if err := ds.groups.Assign(item.GroupID, id, item.GroupPlace); err != nil {
			return nil, fmt.Errorf("failed to assign to group: %w", err)
		}
	}

	// Create result item with filled ID and GroupID
	result := &Item{
		ID:             id,
		Data:           item.Data,
		Meta:           item.Meta,
		DataDescriptor: item.DataDescriptor,
		MetaDescriptor: item.MetaDescriptor,
		Flags:          item.Flags,
		Vector:         item.Vector,
		Tags:           item.Tags,
		GroupID:        groupID,
		GroupPlace:     groupPlace,
	}

	return result, nil
}
