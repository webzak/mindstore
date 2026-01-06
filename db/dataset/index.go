package dataset

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
)

// IndexFlag represents bit flags for index records.
type IndexFlag uint8

const (
	// FlagDeleted indicates the record is marked for deletion (bit 0).
	FlagDeleted IndexFlag = 1 << 0
)

type index struct {
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

func (i *index) size() int64 {
	return sizeIndexRec
}

func (i *index) isDeleted() bool {
	return i.Flags&uint8(FlagDeleted) != 0
}

func (i *index) setDeleted() {
	i.Flags |= uint8(FlagDeleted)
}

func (i *index) blob() []byte {
	buf := make([]byte, sizeIndexRec)
	binary.LittleEndian.PutUint32(buf[0:], i.ID)
	buf[4] = i.Flags
	buf[5] = i.DataDesc
	buf[6] = i.MetaDesc
	buf[7] = i.VectorDesc
	binary.LittleEndian.PutUint64(buf[8:], i.Position)
	binary.LittleEndian.PutUint64(buf[16:], i.Size)
	binary.LittleEndian.PutUint64(buf[24:], i.Date)
	return buf
}

func (i *index) writeAt(f *os.File, headerSize int64) error {
	pos := headerSize + int64(i.ID-1)*sizeIndexRec
	_, err := f.WriteAt(i.blob(), pos)
	return err
}

func readIndex(f *os.File, h *header) (map[uint32]index, uint32, error) {
	if h.indexLen == 0 {
		return make(map[uint32]index), 0, nil
	}

	buf := make([]byte, h.indexLen*sizeIndexRec)
	if _, err := io.ReadFull(f, buf); err != nil {
		return nil, 0, fmt.Errorf("failed to read index: %w", err)
	}

	idx := make(map[uint32]index, h.indexLen)
	var lastID uint32

	for i := uint32(0); i < h.indexLen; i++ {
		offset := int(i * sizeIndexRec)
		rec := index{
			ID:         binary.LittleEndian.Uint32(buf[offset:]),
			Flags:      buf[offset+4],
			DataDesc:   buf[offset+5],
			MetaDesc:   buf[offset+6],
			VectorDesc: buf[offset+7],
			Position:   binary.LittleEndian.Uint64(buf[offset+8:]),
			Size:       binary.LittleEndian.Uint64(buf[offset+16:]),
			Date:       binary.LittleEndian.Uint64(buf[offset+24:]),
		}
		if rec.ID > lastID {
			lastID = rec.ID
		}
		if rec.isDeleted() {
			continue
		}
		idx[rec.ID] = rec
	}

	return idx, lastID, nil
}
