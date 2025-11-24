package index

import (
	"errors"
	"fmt"

	"github.com/webzak/mindstore/internal/engine/conv"
	"github.com/webzak/mindstore/internal/engine/storage"
)

const (
	rowSize = conv.Int64Size * 3
)

var (
	ErrIndexOutOfRange = errors.New("index out of range")
)

// Row is the data containing in the index row
type Row struct {
	// Offset is the shift from start in bytes
	Offset int64
	// Size is the data length in bytes
	Size int64
	// DataType is the type of data
	DataType int64
}

// Index is the data index
type Index struct {
	// storage is the storage for index records
	storage *storage.File
	// rows is the in memory index rows
	rows []Row
}

// New creates a new index
func New(path string) (*Index, error) {
	storage := storage.NewFile(path)
	if err := storage.Init(); err != nil {
		return nil, err
	}
	return &Index{
		storage: storage,
		rows:    make([]Row, 0),
	}, nil
}

// Load loads index from storage
func (idx *Index) Load() (int, error) {
	size, err := idx.storage.Size()
	if err != nil {
		return 0, err
	}
	if size == 0 {
		return 0, nil
	}
	idx.rows = make([]Row, int(size)/rowSize)
	reader, err := idx.storage.Reader(0)
	if err != nil {
		return 0, err
	}
	defer reader.Close()
	buf := make([]byte, rowSize)
	for i := range idx.rows {
		n, err := reader.Read(buf)
		if err != nil {
			return 0, err
		}
		if n != rowSize {
			return 0, fmt.Errorf("%w: expected %d, actual %d", storage.ErrFileRead, rowSize, n)
		}
		idx.rows[i].Offset = conv.BytesToInt64(buf[:conv.Int64Size])
		idx.rows[i].Size = conv.BytesToInt64(buf[conv.Int64Size : conv.Int64Size*2])
		idx.rows[i].DataType = conv.BytesToInt64(buf[conv.Int64Size*2 : conv.Int64Size*3])
	}
	return len(idx.rows), nil
}

// Flush flushes index to storage file
func (idx *Index) Flush() (int, error) {
	writer, err := idx.storage.Writer(0)
	if err != nil {
		return 0, err
	}
	defer writer.Close()
	for _, row := range idx.rows {
		buf := make([]byte, rowSize)
		conv.Int64ToBytes(row.Offset, buf[:conv.Int64Size])
		conv.Int64ToBytes(row.Size, buf[conv.Int64Size:conv.Int64Size*2])
		conv.Int64ToBytes(row.DataType, buf[conv.Int64Size*2:conv.Int64Size*3])
		_, err := writer.Write(buf)
		if err != nil {
			return 0, err
		}
	}
	return len(idx.rows), nil
}

// Get returns index record by number
func (idx *Index) Get(n int) (Row, error) {
	if n < 0 || n >= len(idx.rows) {
		return Row{}, ErrIndexOutOfRange
	}
	return idx.rows[n], nil
}

// Add adds new index record
func (idx *Index) Add(offset int64, size int64, dataType int64) error {
	row := Row{
		Offset:   offset,
		Size:     size,
		DataType: dataType,
	}
	idx.rows = append(idx.rows, row)
	return nil
}

// Remove the record by number
func (idx *Index) Remove(n int) error {
	if n < 0 || n >= len(idx.rows) {
		return ErrIndexOutOfRange
	}
	idx.rows = append(idx.rows[:n], idx.rows[n+1:]...)
	return nil
}

func (idx *Index) Replace(n int, offset int64, size int64, dataType int64) error {
	if n < 0 || n >= len(idx.rows) {
		return ErrIndexOutOfRange
	}
	idx.rows[n].Offset = offset
	idx.rows[n].Size = size
	idx.rows[n].DataType = dataType
	return nil
}

// Clear clears the index
func (idx *Index) Clear() {
	idx.rows = idx.rows[:0]
}

// Count returns the number of records in the index
func (idx *Index) Count() int {
	return len(idx.rows)
}
