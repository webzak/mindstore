package dataset

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
)

// Field specifies which chunk fields to read
type Field int

const (
	FieldData Field = iota
	FieldMeta
	FieldVector
)

// Unit represents a blob with its descriptor
type Unit interface {
	Blob() []byte
	Descriptor() uint8
}

// ByteUnit is the default implementation of Unit
type ByteUnit struct {
	data []byte
	desc uint8
}

// NewByteUnit creates a new ByteUnit with the given data and descriptor
func NewByteUnit(data []byte, descriptor uint8) *ByteUnit {
	return &ByteUnit{data: data, desc: descriptor}
}

func (u *ByteUnit) Blob() []byte      { return u.data }
func (u *ByteUnit) Descriptor() uint8 { return u.desc }

// Chunk represents chunk with metadata for read operations.
type Chunk struct {
	ID     uint32
	Date   uint64
	Flags  uint8
	Data   Unit
	Meta   Unit
	Vector Unit
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

func (cr *chunkRecord) size() int64 {
	return int64(8 + 4 + 4 + cr.dataSize + uint64(cr.metaSize) + uint64(cr.vectorSize))
}

// write writes chunk to file at current position (must be at end of file)
func (cr *chunkRecord) write(f *os.File) error {
	buf := make([]byte, cr.size())
	offset := 0
	binary.LittleEndian.PutUint64(buf[offset:], cr.dataSize)
	offset += 8
	binary.LittleEndian.PutUint32(buf[offset:], cr.metaSize)
	offset += 4
	binary.LittleEndian.PutUint32(buf[offset:], cr.vectorSize)
	offset += 4
	copy(buf[offset:], cr.Data)
	offset += int(cr.dataSize)
	copy(buf[offset:], cr.Meta)
	offset += int(cr.metaSize)
	copy(buf[offset:], cr.Vector)

	_, err := f.Write(buf)
	return err
}

func readChunk(f *os.File, size uint64) (*chunkRecord, error) {
	buf := make([]byte, size)
	if _, err := io.ReadFull(f, buf); err != nil {
		return nil, fmt.Errorf("failed to read chunk: %w", err)
	}

	dataSize := binary.LittleEndian.Uint64(buf[0:])
	metaSize := binary.LittleEndian.Uint32(buf[8:])
	vectorSize := binary.LittleEndian.Uint32(buf[12:])

	offset := uint64(16)
	cr := &chunkRecord{
		dataSize:   dataSize,
		metaSize:   metaSize,
		vectorSize: vectorSize,
		Data:       buf[offset : offset+dataSize],
		Meta:       buf[offset+dataSize : offset+dataSize+uint64(metaSize)],
		Vector:     buf[offset+dataSize+uint64(metaSize) : offset+dataSize+uint64(metaSize)+uint64(vectorSize)],
	}
	return cr, nil
}

// readChunkFields loads specified fields into an existing Chunk.
// The chunk must have index metadata already populated.
// Only fields in the slice are loaded; others remain nil.
// Caller must hold the dataset lock.
func (d *Dataset) readChunkFields(c *Chunk, idx *index, fields []Field) error {
	if len(fields) == 0 {
		return nil
	}

	// Determine which fields to read
	readData, readMeta, readVector := false, false, false
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

	// Seek to chunk position
	pos := d.header.dataSpacePos() + int64(idx.Position)
	if _, err := d.f.Seek(pos, io.SeekStart); err != nil {
		return fmt.Errorf("failed to seek to chunk: %w", err)
	}

	// Read sizes header (16 bytes)
	var sizeBuf [16]byte
	if _, err := io.ReadFull(d.f, sizeBuf[:]); err != nil {
		return fmt.Errorf("failed to read chunk sizes: %w", err)
	}

	dataSize := binary.LittleEndian.Uint64(sizeBuf[0:])
	metaSize := binary.LittleEndian.Uint32(sizeBuf[8:])
	vectorSize := binary.LittleEndian.Uint32(sizeBuf[12:])

	// Read data blob
	if readData && dataSize > 0 {
		dataBlob := make([]byte, dataSize)
		if _, err := io.ReadFull(d.f, dataBlob); err != nil {
			return fmt.Errorf("failed to read data blob: %w", err)
		}
		c.Data = NewByteUnit(dataBlob, idx.DataDesc)
	} else if dataSize > 0 {
		// Skip data blob
		if _, err := d.f.Seek(int64(dataSize), io.SeekCurrent); err != nil {
			return fmt.Errorf("failed to skip data blob: %w", err)
		}
	}

	// Read meta blob
	if readMeta && metaSize > 0 {
		metaBlob := make([]byte, metaSize)
		if _, err := io.ReadFull(d.f, metaBlob); err != nil {
			return fmt.Errorf("failed to read meta blob: %w", err)
		}
		c.Meta = NewByteUnit(metaBlob, idx.MetaDesc)
	} else if metaSize > 0 {
		// Skip meta blob
		if _, err := d.f.Seek(int64(metaSize), io.SeekCurrent); err != nil {
			return fmt.Errorf("failed to skip meta blob: %w", err)
		}
	}

	// Read vector blob
	if readVector && vectorSize > 0 {
		vectorBlob := make([]byte, vectorSize)
		if _, err := io.ReadFull(d.f, vectorBlob); err != nil {
			return fmt.Errorf("failed to read vector blob: %w", err)
		}
		c.Vector = NewByteUnit(vectorBlob, idx.VectorDesc)
	}

	return nil
}
