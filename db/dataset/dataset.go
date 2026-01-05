package dataset

import (
	"fmt"
	"io"
	"os"
	"sync"
	"time"
)

const (
	sizeMagic    = 4       // magic bytes
	size32       = 4       // 32 bit word size
	sizeIndexRec = 32      // index record size
	maxIndexCap  = 1 << 24 // ~16 million records, ~512MB index space
)

type Dataset struct {
	sync.Mutex
	f      *os.File
	path   string
	header *header
	index  map[uint32]Index
	lastID uint32
}

// NewDataset creates new dataset file
func NewDataset(path string, config []byte, indexCap int) (*Dataset, error) {
	if path == "" {
		return nil, fmt.Errorf("path cannot be empty")
	}
	if indexCap <= 0 || indexCap > maxIndexCap {
		return nil, fmt.Errorf("indexCap must be between 1 and %d", maxIndexCap)
	}

	// Create the file
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}

	// Create header struct
	h := &header{
		magic:      magic,
		configSize: uint32(len(config)),
		config:     config,
		indexCap:   uint32(indexCap),
		indexLen:   0,
	}

	// Write header blob in single operation
	if _, err := f.Write(h.blob()); err != nil {
		f.Close()
		return nil, fmt.Errorf("failed to write header: %w", err)
	}

	// Reserve index space (indexCap * sizeIndexRec bytes) using Truncate
	totalSize := int64(len(h.blob())) + int64(indexCap*sizeIndexRec)
	if err := f.Truncate(totalSize); err != nil {
		f.Close()
		return nil, fmt.Errorf("failed to allocate index space: %w", err)
	}

	// Initialize and return Dataset
	return &Dataset{
		f:      f,
		path:   path,
		header: h,
		index:  make(map[uint32]Index, indexCap),
		lastID: 0,
	}, nil
}

// Close closes the dataset and releases resources.
func (d *Dataset) Close() error {
	d.Lock()
	f := d.f
	d.f = nil
	d.Unlock()
	return f.Close()
}

func (d *Dataset) ChangeIndexCap(newCap int, useLock bool) error {
	if useLock {
		d.Lock()
		defer d.Unlock()
	}

	if newCap == int(d.header.indexCap) {
		return nil
	}
	if newCap <= 0 || newCap > maxIndexCap {
		return fmt.Errorf("capacity must be between 1 and %d", maxIndexCap)
	}
	if newCap < len(d.index) {
		return fmt.Errorf("capacity %d is less than amount of records: %d", newCap, len(d.index))
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

	// Read header using readHeader function
	h, err := readHeader(d.f)
	if err != nil {
		return err
	}

	// Update indexCap in header struct
	h.indexCap = uint32(newCap)

	// Write modified header blob
	if _, err := tmpFile.Write(h.blob()); err != nil {
		return fmt.Errorf("failed to write header blob: %w", err)
	}

	// Create new index space and copy existing records
	newIndexSpace := make([]byte, newCap*sizeIndexRec)
	if len(d.index) > 0 {
		// Seek to index start in original file
		if _, err := d.f.Seek(d.header.size(), io.SeekStart); err != nil {
			return fmt.Errorf("failed to seek to index: %w", err)
		}
		// Read existing index records into the new index space buffer
		if _, err := io.ReadFull(d.f, newIndexSpace[:len(d.index)*sizeIndexRec]); err != nil {
			return fmt.Errorf("failed to read existing index records: %w", err)
		}
	}
	// Write entire new index space (existing records + zero padding)
	if _, err := tmpFile.Write(newIndexSpace); err != nil {
		return fmt.Errorf("failed to write new index space: %w", err)
	}

	// Step 7: Copy data space
	if _, err := d.f.Seek(d.header.dataSpacePos(), io.SeekStart); err != nil {
		return fmt.Errorf("failed to seek to data space: %w", err)
	}
	if _, err := io.Copy(tmpFile, d.f); err != nil {
		return fmt.Errorf("failed to copy data space: %w", err)
	}

	// Step 8: Sync temp file
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

	// Step 10: Atomically replace original with temp
	if err := os.Rename(tmpPath, d.path); err != nil {
		return fmt.Errorf("failed to replace file: %w", err)
	}

	// Step 11: Reopen the file
	newFile, err := os.OpenFile(d.path, os.O_RDWR, 0644)
	if err != nil {
		return fmt.Errorf("failed to reopen file: %w", err)
	}

	// Update Dataset state
	d.f = newFile
	d.header.indexCap = uint32(newCap)

	return nil
}

func OpenDataset(path string) (*Dataset, error) {
	f, err := os.OpenFile(path, os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	h, err := readHeader(f)
	if err != nil {
		f.Close()
		return nil, err
	}

	index, lastID, err := readIndex(f, h)
	if err != nil {
		f.Close()
		return nil, err
	}

	return &Dataset{
		f:      f,
		path:   path,
		header: h,
		index:  index,
		lastID: lastID,
	}, nil
}

// Append adds chunk data to file and returns id of added chunk
func (d *Dataset) Append(c Chunk) (uint32, error) {
	d.Lock()
	defer d.Unlock()

	// Expand capacity if needed
	if d.header.indexLen >= d.header.indexCap {
		if err := d.ChangeIndexCap(int(d.header.indexCap)*2, false); err != nil {
			return 0, fmt.Errorf("failed to expand index capacity: %w", err)
		}
	}

	// Calculate chunk position - seek to end of file
	fileSize, err := d.f.Seek(0, io.SeekEnd)
	if err != nil {
		return 0, fmt.Errorf("failed to seek to end: %w", err)
	}
	chunkPos := uint64(fileSize - d.header.dataSpacePos())

	// Create and write chunk record
	cr := &chunkRecord{
		dataSize:   uint64(len(c.Data)),
		metaSize:   uint32(len(c.Meta)),
		vectorSize: uint32(len(c.Vector)),
		Data:       c.Data,
		Meta:       c.Meta,
		Vector:     c.Vector,
	}
	if err := cr.write(d.f); err != nil {
		return 0, fmt.Errorf("failed to write chunk: %w", err)
	}

	// Create index record
	newID := d.lastID + 1
	idx := Index{
		ID:         newID,
		Flags:      c.Flags,
		DataDesc:   c.DataDesc,
		MetaDesc:   c.MetaDesc,
		VectorDesc: c.VectorDesc,
		Position:   chunkPos,
		Size:       uint64(cr.size()),
		Date:       uint64(time.Now().Unix()),
	}

	// Write index record
	idxPos := d.header.size() + int64(d.header.indexLen)*sizeIndexRec
	if _, err := d.f.WriteAt(idx.blob(), idxPos); err != nil {
		return 0, fmt.Errorf("failed to write index record: %w", err)
	}

	// Update header on disk
	d.header.indexLen++
	if _, err := d.f.WriteAt(d.header.blob(), 0); err != nil {
		d.header.indexLen--
		return 0, fmt.Errorf("failed to update header: %w", err)
	}

	// Sync file to disk
	if err := d.f.Sync(); err != nil {
		return 0, fmt.Errorf("failed to sync file: %w", err)
	}

	// Update in-memory state
	d.index[newID] = idx
	d.lastID = newID

	return newID, nil
}

// Read retrieves a chunk by ID. By default reads all fields.
// Pass specific fields to read selectively (e.g., FieldData, FieldMeta).
// Non-selected fields will be nil in the returned Chunk.
func (d *Dataset) Read(id uint32, fields ...Field) (*Chunk, error) {
	d.Lock()
	defer d.Unlock()

	idx, ok := d.index[id]
	if !ok {
		return nil, fmt.Errorf("chunk with id %d not found", id)
	}

	// Determine which fields to read
	readData, readMeta, readVector := true, true, true
	if len(fields) > 0 {
		readData, readMeta, readVector = false, false, false
		for _, f := range fields {
			switch f {
			case FieldData:
				readData = true
			case FieldMeta:
				readMeta = true
			case FieldVector:
				readVector = true
			}
		}
	}

	// Seek to chunk position
	pos := d.header.dataSpacePos() + int64(idx.Position)
	if _, err := d.f.Seek(pos, io.SeekStart); err != nil {
		return nil, fmt.Errorf("failed to seek to chunk: %w", err)
	}

	// Read chunk record
	cr, err := readChunk(d.f, idx.Size)
	if err != nil {
		return nil, err
	}

	// Build Chunk with selected fields
	chunk := &Chunk{
		Flags:      idx.Flags,
		DataDesc:   idx.DataDesc,
		MetaDesc:   idx.MetaDesc,
		VectorDesc: idx.VectorDesc,
	}
	if readData {
		chunk.Data = cr.Data
	}
	if readMeta {
		chunk.Meta = cr.Meta
	}
	if readVector {
		chunk.Vector = cr.Vector
	}

	return chunk, nil
}
