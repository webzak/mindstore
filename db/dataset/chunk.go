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

// Chunk represents data chunk available for user
// The meaning of value []byte{} is empty value
// The meaning of value nil is we we ignored it on read operation or did not provide it on update operation
type Chunk struct {
	Flags      uint8
	DataDesc   uint8
	MetaDesc   uint8
	VectorDesc uint8
	Data       []byte
	Meta       []byte
	Vector     []byte
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
