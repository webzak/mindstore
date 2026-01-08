package dataset

import (
	"fmt"
	"io"
	"os"
	"slices"
)

// Optimize removes deleted records and compacts the dataset file.
// It reorders data chunks by ID and removes unused index capacity.
// Original IDs are preserved.
func (d *Dataset) Optimize() error {
	d.Lock()
	defer d.Unlock()

	if len(d.index) == 0 {
		return nil
	}

	// Collect and sort IDs
	ids := make([]uint32, 0, len(d.index))
	for id := range d.index {
		ids = append(ids, id)
	}
	slices.Sort(ids)

	// Create new header with exact index size
	newLen := uint32(len(ids))
	newHeader := &header{
		magic:      d.header.magic,
		signature:  d.header.signature,
		configSize: d.header.configSize,
		config:     d.header.config,
		indexCap:   newLen,
		indexLen:   newLen,
	}

	// Create temporary file
	tmpPath := d.path + ".tmp"
	tmpFile, err := os.Create(tmpPath)
	if err != nil {
		return fmt.Errorf("failed to create temp file: %w", err)
	}

	// Setup cleanup on error
	success := false
	tmpClosed := false
	defer func() {
		if !tmpClosed {
			tmpFile.Close()
		}
		if !success {
			os.Remove(tmpPath)
		}
	}()

	// Write header
	if _, err := tmpFile.Write(newHeader.blob()); err != nil {
		return fmt.Errorf("failed to write header: %w", err)
	}

	// Reserve index space (placeholder, will be overwritten)
	indexSpace := make([]byte, newLen*sizeIndexRec)
	if _, err := tmpFile.Write(indexSpace); err != nil {
		return fmt.Errorf("failed to reserve index space: %w", err)
	}

	// Copy data chunks in ID order and build new index
	newIndex := make(map[uint32]index, len(ids))
	var dataPos uint64 = 0

	for i, id := range ids {
		oldIdx := d.index[id]

		// Seek to old chunk position
		if _, err := d.f.Seek(d.header.dataSpacePos()+int64(oldIdx.Position), io.SeekStart); err != nil {
			return fmt.Errorf("failed to seek to chunk %d: %w", id, err)
		}

		// Read chunk from old file
		cr, err := readChunk(d.f, oldIdx.Size)
		if err != nil {
			return fmt.Errorf("failed to read chunk %d: %w", id, err)
		}

		// Write chunk to temp file
		if err := cr.write(tmpFile); err != nil {
			return fmt.Errorf("failed to write chunk %d: %w", id, err)
		}

		// Build new index record with same ID but new position
		newIdx := index{
			ID:         id,
			Flags:      oldIdx.Flags,
			DataDesc:   oldIdx.DataDesc,
			MetaDesc:   oldIdx.MetaDesc,
			VectorDesc: oldIdx.VectorDesc,
			Position:   dataPos,
			Size:       oldIdx.Size,
			Date:       oldIdx.Date,
		}

		// Write to index buffer at sequential slot position
		copy(indexSpace[i*int(sizeIndexRec):], newIdx.blob())
		newIndex[id] = newIdx

		dataPos += oldIdx.Size
	}

	// Seek back and write index space
	if _, err := tmpFile.Seek(newHeader.size(), io.SeekStart); err != nil {
		return fmt.Errorf("failed to seek to index space: %w", err)
	}
	if _, err := tmpFile.Write(indexSpace); err != nil {
		return fmt.Errorf("failed to write index space: %w", err)
	}

	// Sync temp file
	if err := tmpFile.Sync(); err != nil {
		return fmt.Errorf("failed to sync temp file: %w", err)
	}

	// Close both files
	tmpClosed = true
	if err := tmpFile.Close(); err != nil {
		return fmt.Errorf("failed to close temp file: %w", err)
	}
	if err := d.f.Close(); err != nil {
		return fmt.Errorf("failed to close original file: %w", err)
	}

	// Atomically replace original with temp
	if err := os.Rename(tmpPath, d.path); err != nil {
		return fmt.Errorf("failed to replace file: %w", err)
	}

	// Reopen the file
	newFile, err := os.OpenFile(d.path, os.O_RDWR, 0644)
	if err != nil {
		return fmt.Errorf("failed to reopen file: %w", err)
	}

	// Update Dataset state
	d.f = newFile
	d.header = newHeader
	d.index = newIndex
	// d.lastID stays unchanged

	success = true
	return nil
}
