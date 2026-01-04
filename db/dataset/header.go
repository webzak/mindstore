package dataset

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
)

const (
	magic = 0x19720611
)

// header represents the dataset file starting part up to index space
type header struct {
	magic      uint32
	configSize uint32
	// config is raw messagepack data
	config []byte
	// indexCap defines the capacity as amount of actual and reserved index records
	indexCap uint32
	// indexLen defines the amount of real indeex records
	indexLen uint32
}

// size of header record in bytes
func (h *header) size() int64 {
	return int64(h.configSize + size32*4)
}

// data space position in file
func (h *header) dataSpacePos() int64 {
	return h.size() + int64(h.indexCap*sizeIndexRec)
}

func (h *header) blob() []byte {
	configSize := uint32(len(h.config))
	blobSize := sizeMagic + int(configSize) + size32*3
	blob := make([]byte, blobSize)

	offset := 0
	binary.LittleEndian.PutUint32(blob[offset:], magic)
	offset += sizeMagic

	binary.LittleEndian.PutUint32(blob[offset:], configSize)
	offset += size32

	copy(blob[offset:], h.config)
	offset += int(configSize)

	binary.LittleEndian.PutUint32(blob[offset:], h.indexCap)
	offset += size32

	binary.LittleEndian.PutUint32(blob[offset:], h.indexLen)

	return blob
}

func readHeader(f *os.File) (*header, error) {
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return nil, fmt.Errorf("failed to seek: %w", err)
	}

	// Read magic + configSize to determine total size
	initialBuf := make([]byte, sizeMagic+size32)
	if _, err := io.ReadFull(f, initialBuf); err != nil {
		return nil, fmt.Errorf("failed to read initial header: %w", err)
	}

	// Verify magic bytes
	readMagic := binary.LittleEndian.Uint32(initialBuf[0:sizeMagic])
	if readMagic != magic {
		return nil, fmt.Errorf("invalid magic bytes: expected 0x%08x, got 0x%08x", magic, readMagic)
	}

	// Extract configSize
	configSize := binary.LittleEndian.Uint32(initialBuf[sizeMagic:])

	// Calculate and read remaining header data
	remainingSize := int(configSize) + size32*2
	remainingBuf := make([]byte, remainingSize)
	if _, err := io.ReadFull(f, remainingBuf); err != nil {
		return nil, fmt.Errorf("failed to read header remainder: %w", err)
	}

	// Extract config body (slice directly to avoid extra allocation)
	config := remainingBuf[:configSize:configSize]

	// Parse remaining fields
	offset := int(configSize)

	// Extract indexCap
	indexCap := binary.LittleEndian.Uint32(remainingBuf[offset:])
	offset += size32

	// Extract indexLen
	indexLen := binary.LittleEndian.Uint32(remainingBuf[offset:])

	// Construct header struct
	h := &header{
		magic:      readMagic,
		configSize: configSize,
		config:     config,
		indexCap:   indexCap,
		indexLen:   indexLen,
	}

	return h, nil
}
