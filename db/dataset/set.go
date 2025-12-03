package dataset

import (
	"fmt"
)

// SetData updates the data and descriptor for an existing record by index.
// It handles both in-place replacement (when new data fits in existing space)
// and append scenarios (when new data is larger or no data exists).
//
// Special case: if data is nil or empty, the record's data is cleared
// (offset set to -1, size to 0) while preserving the descriptor.
func (ds *Dataset) SetData(id int, data []byte, descriptor uint8) error {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	if ds.closed {
		return ErrDatasetClosed
	}

	// Get the current index record
	row, err := ds.index.Get(id)
	if err != nil {
		return fmt.Errorf("failed to get index entry: %w", err)
	}

	// Handle special case: empty or nil data
	if len(data) == 0 {
		// Update index with sentinel values for empty data
		row.Offset = -1
		row.Size = 0
		row.DataDescriptor = descriptor
		// Preserve other fields: MetaOffset, MetaSize, MetaDataDescriptor, Flags

		if err := ds.index.Replace(id, row); err != nil {
			return fmt.Errorf("failed to update index: %w", err)
		}
		return nil
	}

	// Determine if we can do in-place replacement
	// Can replace in-place when offset is valid (>= 0) and new data fits
	dataLen := int64(len(data))
	canReplaceInPlace := row.Offset >= 0 && row.Size >= dataLen

	if canReplaceInPlace {
		// In-place replacement: data fits in existing space
		if err := ds.data.Replace(data, row.Offset); err != nil {
			return fmt.Errorf("failed to replace data: %w", err)
		}

		// Update index with new size and descriptor, keep same offset
		row.Size = dataLen
		row.DataDescriptor = descriptor
		// Preserve other fields: Offset (unchanged), MetaOffset, MetaSize, MetaDataDescriptor, Flags

		if err := ds.index.Replace(id, row); err != nil {
			return fmt.Errorf("failed to update index: %w", err)
		}
	} else {
		// Append new data: either no data exists or new data is larger
		offset, size, err := ds.data.Append(data)
		if err != nil {
			return fmt.Errorf("failed to append data: %w", err)
		}

		// Update index with new offset, size, and descriptor
		row.Offset = offset
		row.Size = size
		row.DataDescriptor = descriptor
		// Preserve other fields: MetaOffset, MetaSize, MetaDataDescriptor, Flags

		if err := ds.index.Replace(id, row); err != nil {
			return fmt.Errorf("failed to update index: %w", err)
		}
	}

	return nil
}

// SetMetaData updates the metadata and descriptor for an existing record by index.
// It handles both in-place replacement (when new metadata fits in existing space)
// and append scenarios (when new metadata is larger or no metadata exists).
//
// Special case: if data is nil or empty, the record's metadata is cleared
// (offset set to -1, size to 0) while preserving the descriptor.
func (ds *Dataset) SetMetaData(id int, data []byte, descriptor uint8) error {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	if ds.closed {
		return ErrDatasetClosed
	}

	// Get the current index record
	row, err := ds.index.Get(id)
	if err != nil {
		return fmt.Errorf("failed to get index entry: %w", err)
	}

	// Handle special case: empty or nil data
	if len(data) == 0 {
		// Update index with sentinel values for empty metadata
		row.MetaOffset = -1
		row.MetaSize = 0
		row.MetaDataDescriptor = descriptor
		// Preserve other fields: Offset, Size, DataDescriptor, Flags

		if err := ds.index.Replace(id, row); err != nil {
			return fmt.Errorf("failed to update index: %w", err)
		}
		return nil
	}

	// Determine if we can do in-place replacement
	// Can replace in-place when offset is valid (>= 0) and new data fits
	dataLen := int64(len(data))
	canReplaceInPlace := row.MetaOffset >= 0 && row.MetaSize >= dataLen

	if canReplaceInPlace {
		// In-place replacement: metadata fits in existing space
		if err := ds.meta.Replace(data, row.MetaOffset); err != nil {
			return fmt.Errorf("failed to replace metadata: %w", err)
		}

		// Update index with new size and descriptor, keep same offset
		row.MetaSize = dataLen
		row.MetaDataDescriptor = descriptor
		// Preserve other fields: MetaOffset (unchanged), Offset, Size, DataDescriptor, Flags

		if err := ds.index.Replace(id, row); err != nil {
			return fmt.Errorf("failed to update index: %w", err)
		}
	} else {
		// Append new metadata: either no metadata exists or new metadata is larger
		offset, size, err := ds.meta.Append(data)
		if err != nil {
			return fmt.Errorf("failed to append metadata: %w", err)
		}

		// Update index with new offset, size, and descriptor
		row.MetaOffset = offset
		row.MetaSize = size
		row.MetaDataDescriptor = descriptor
		// Preserve other fields: Offset, Size, DataDescriptor, Flags

		if err := ds.index.Replace(id, row); err != nil {
			return fmt.Errorf("failed to update index: %w", err)
		}
	}

	return nil
}

// SetVector updates or sets the vector for an existing record by index.
// If the record already has a vector (Vector >= 0), it replaces it in-place.
// If the record doesn't have a vector (Vector == -1), it appends a new one.
func (ds *Dataset) SetVector(id int, vector []float32) error {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	if ds.closed {
		return ErrDatasetClosed
	}

	// Get the current index record
	row, err := ds.index.Get(id)
	if err != nil {
		return fmt.Errorf("failed to get index entry: %w", err)
	}

	var vectorPos int32
	if row.Vector >= 0 {
		// Replace existing vector in-place
		if err := ds.vectors.Replace(row.Vector, vector); err != nil {
			return fmt.Errorf("failed to replace vector: %w", err)
		}
		vectorPos = row.Vector
	} else {
		// Append new vector
		pos, err := ds.vectors.Append(vector)
		if err != nil {
			return fmt.Errorf("failed to append vector: %w", err)
		}
		vectorPos = pos
	}

	// Update index with vector position
	row.Vector = vectorPos
	if err := ds.index.Replace(id, row); err != nil {
		return fmt.Errorf("failed to update index: %w", err)
	}

	return nil
}
