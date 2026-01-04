package dataset

import (
	"fmt"
	"io"
	"os"
	"sync"
)

const (
	sizeMagic    = 4      // magic bytes
	size32       = 4      // 32 bit word size
	sizeIndexRec = 32     // index record size
	maxIndexCap  = 1 << 24 // ~16 million records, ~512MB index space
)

type Index struct {
	ID         uint32
	Flags      uint8
	DataDesc   uint8
	MetaDesc   uint8
	VectorDesc uint8
	// Position in bytes from data region in file
	Position uint64
	// Size of chunk
	Size uint64
	// Date is unix timestamp in seconds
	Date uint64
}

// Chunk represents data chunk available for user
// The meaning of value []byte{} is empty value
// The meaning of value nil is we we ignored it on read operation or did not provide it on update operation
type Chunk struct {
	Data   []byte
	Meta   []byte
	Vector []byte
}

// chunkRecord represents how chunk data is saved to the file
type chunkRecord struct {
	dataSize   uint64
	metaSize   uint32
	vectorSize uint32
	Data       []byte
	Meta       []byte
	Vector     []byte
}

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

func (d *Dataset) ChangeIndexCap(newCap int) error {
	d.Lock()
	defer d.Unlock()

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

//func OpenDataset(path string) (*Dataset, error) {
//
// }
//
// // Append adds chunk data to file and returns id of added chunk
// func Append(c Chunk, dataDesc, metaDesc, vectorDesc, flags uint8) (int, error) {
//
// }
