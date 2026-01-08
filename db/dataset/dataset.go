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
	index  map[uint32]index
	lastID uint32
}

// Info contains dataset header information for inspection without keeping file open.
type Info struct {
	Signature uint32
	Config    []byte
	IndexCap  uint32
	IndexLen  uint32
}

// NewDataset creates new dataset file
func NewDataset(path string, signature uint32, config []byte, indexCap int) (*Dataset, error) {
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
		signature:  signature,
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
		index:  make(map[uint32]index, indexCap),
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

	// Create new header with updated capacity
	newHeader := &header{
		magic:      d.header.magic,
		signature:  d.header.signature,
		configSize: d.header.configSize,
		config:     d.header.config,
		indexCap:   uint32(newCap),
		indexLen:   d.header.indexLen,
	}

	return d.rewriteFile(newHeader, uint32(newCap), d.header.indexLen)
}

// rewriteFile rewrites the dataset file with a new header configuration.
// newIndexCap specifies the index space size in the new file.
// copyIndexCount specifies how many index records to copy from original.
// Caller must hold the lock.
func (d *Dataset) rewriteFile(newHeader *header, newIndexCap uint32, copyIndexCount uint32) error {
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

	// Write new header blob
	if _, err := tmpFile.Write(newHeader.blob()); err != nil {
		return fmt.Errorf("failed to write header blob: %w", err)
	}

	// Create new index space and copy existing records
	newIndexSpace := make([]byte, newIndexCap*sizeIndexRec)
	if copyIndexCount > 0 {
		// Seek to index start in original file
		if _, err := d.f.Seek(d.header.size(), io.SeekStart); err != nil {
			return fmt.Errorf("failed to seek to index: %w", err)
		}
		// Read existing index records into the new index space buffer
		if _, err := io.ReadFull(d.f, newIndexSpace[:copyIndexCount*sizeIndexRec]); err != nil {
			return fmt.Errorf("failed to read existing index records: %w", err)
		}
	}
	// Write entire new index space (existing records + zero padding)
	if _, err := tmpFile.Write(newIndexSpace); err != nil {
		return fmt.Errorf("failed to write index space: %w", err)
	}

	// Copy data space
	if _, err := d.f.Seek(d.header.dataSpacePos(), io.SeekStart); err != nil {
		return fmt.Errorf("failed to seek to data space: %w", err)
	}
	if _, err := io.Copy(tmpFile, d.f); err != nil {
		return fmt.Errorf("failed to copy data space: %w", err)
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
	success = true
	return nil
}

// UpdateConfig updates the dataset configuration.
// This rewrites the file with the new config using atomic rename.
func (d *Dataset) UpdateConfig(config []byte, useLock bool) error {
	if useLock {
		d.Lock()
		defer d.Unlock()
	}

	newHeader := &header{
		magic:      d.header.magic,
		signature:  d.header.signature,
		configSize: uint32(len(config)),
		config:     config,
		indexCap:   d.header.indexCap,
		indexLen:   d.header.indexLen,
	}

	return d.rewriteFile(newHeader, d.header.indexCap, d.header.indexLen)
}

// Config returns a copy of the current config bytes.
func (d *Dataset) Config() []byte {
	d.Lock()
	defer d.Unlock()

	if len(d.header.config) == 0 {
		return nil
	}
	config := make([]byte, len(d.header.config))
	copy(config, d.header.config)
	return config
}

// Signature returns the dataset signature.
func (d *Dataset) Signature() uint32 {
	d.Lock()
	defer d.Unlock()
	return d.header.signature
}

// OpenDataset function opens existing dataset file
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

// ReadInfo reads dataset header information without keeping file open.
func ReadInfo(path string) (*Info, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	h, err := readHeader(f)
	if err != nil {
		return nil, err
	}

	return &Info{
		Signature: h.signature,
		Config:    h.config,
		IndexCap:  h.indexCap,
		IndexLen:  h.indexLen,
	}, nil
}

// Append adds chunk data to file and returns id of added chunk
func (d *Dataset) Append(c ChunkData) (uint32, error) {
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
	idx := index{
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
	if err := idx.writeAt(d.f, d.header.size()); err != nil {
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
		ID:   id,
		Date: idx.Date,
		ChunkData: ChunkData{
			Flags:      idx.Flags,
			DataDesc:   idx.DataDesc,
			MetaDesc:   idx.MetaDesc,
			VectorDesc: idx.VectorDesc,
		},
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

// Delete marks a chunk as deleted by ID.
// Returns true if the chunk was found and marked deleted, false if not found.
// The actual data remains in the file until Optimize() is called.
func (d *Dataset) Delete(id uint32) bool {
	d.Lock()
	defer d.Unlock()

	idx, ok := d.index[id]
	if !ok {
		return false
	}

	idx.setDeleted()
	idx.Date = uint64(time.Now().Unix())

	if err := idx.writeAt(d.f, d.header.size()); err != nil {
		return false
	}
	if err := d.f.Sync(); err != nil {
		return false
	}

	delete(d.index, id)
	return true
}

// Update modifies an existing chunk by ID.
// The update appends the modified chunk to end of file and updates the index record.
// For Data, Meta, Vector fields: nil means preserve current value, []byte{} means overwrite with empty.
// Flags and descriptors are updated: nil fields preserve their descriptors, non-nil fields use new descriptors.
func (d *Dataset) Update(id uint32, c ChunkData) error {
	d.Lock()
	defer d.Unlock()

	idx, ok := d.index[id]
	if !ok {
		return fmt.Errorf("chunk with id %d not found", id)
	}

	// Check if we need to read existing chunk data
	needsRead := c.Data == nil || c.Meta == nil || c.Vector == nil

	var existing *chunkRecord
	if needsRead {
		pos := d.header.dataSpacePos() + int64(idx.Position)
		if _, err := d.f.Seek(pos, io.SeekStart); err != nil {
			return fmt.Errorf("failed to seek to chunk: %w", err)
		}
		var err error
		existing, err = readChunk(d.f, idx.Size)
		if err != nil {
			return fmt.Errorf("failed to read existing chunk: %w", err)
		}
	}

	// Merge values and descriptors
	var newData, newMeta, newVector []byte
	var newDataDesc, newMetaDesc, newVectorDesc uint8

	if c.Data == nil {
		newData = existing.Data
		newDataDesc = idx.DataDesc
	} else {
		newData = c.Data
		newDataDesc = c.DataDesc
	}

	if c.Meta == nil {
		newMeta = existing.Meta
		newMetaDesc = idx.MetaDesc
	} else {
		newMeta = c.Meta
		newMetaDesc = c.MetaDesc
	}

	if c.Vector == nil {
		newVector = existing.Vector
		newVectorDesc = idx.VectorDesc
	} else {
		newVector = c.Vector
		newVectorDesc = c.VectorDesc
	}

	// Seek to end of file to get chunk position
	fileSize, err := d.f.Seek(0, io.SeekEnd)
	if err != nil {
		return fmt.Errorf("failed to seek to end: %w", err)
	}
	chunkPos := uint64(fileSize - d.header.dataSpacePos())

	// Create and write chunk record
	cr := &chunkRecord{
		dataSize:   uint64(len(newData)),
		metaSize:   uint32(len(newMeta)),
		vectorSize: uint32(len(newVector)),
		Data:       newData,
		Meta:       newMeta,
		Vector:     newVector,
	}
	if err := cr.write(d.f); err != nil {
		return fmt.Errorf("failed to write chunk: %w", err)
	}

	// Update index record
	idx.Flags = c.Flags
	idx.DataDesc = newDataDesc
	idx.MetaDesc = newMetaDesc
	idx.VectorDesc = newVectorDesc
	idx.Position = chunkPos
	idx.Size = uint64(cr.size())
	idx.Date = uint64(time.Now().Unix())

	// Write index record
	if err := idx.writeAt(d.f, d.header.size()); err != nil {
		return fmt.Errorf("failed to write index record: %w", err)
	}

	// Sync file to disk
	if err := d.f.Sync(); err != nil {
		return fmt.Errorf("failed to sync file: %w", err)
	}

	// Update in-memory index
	d.index[id] = idx

	return nil
}
