package index

import (
	"errors"
	"fmt"
	"iter"

	"github.com/webzak/mindstore/internal/conv"
	"github.com/webzak/mindstore/internal/storage"
)

const (
	rowSize                = conv.Int64Size * 5
	MarkedForRemoval uint8 = 1 << 0
)

var (
	ErrIndexOutOfRange       = errors.New("index out of range")
	ErrIndexStorageCorrupted = errors.New("index storage is corrupted")
)

// Row is the data containing in the index row
type Row struct {
	// Offset is the shift from start in bytes in data storage
	Offset int64
	// Size is the data length in bytes
	Size int64
	// MetaOffset is the shift from start in bytes in meta storage
	MetaOffset int64
	// MetaSize is the data length in bytes
	MetaSize int64
	// DataDescriptor is the type of data, engine agnostic
	DataDescriptor uint8
	// MetaDataDescriptor is the type of metadata
	MetaDataDescriptor uint8
	// Flags are bit options
	Flags uint8
	// Reserved space to align the size
	_ uint8
	_ uint32
}

// Options are options for index
type Options struct {
	MaxAppendBufferSize int
}

// Index is the data index
type Index struct {
	// storage is the storage for index records
	storage *storage.File
	// the actual amount persited
	persistedSize int
	// maxAppendBufferSize of unsaved index rows
	maxAppendBufferSize int
	// rows is the in memory index rows
	rows []Row
}

// New creates a new index
func New(path string, opt Options) (*Index, error) {
	storage := storage.NewFile(path)
	if err := storage.Init(true); err != nil {
		return nil, err
	}
	ret := Index{
		storage:             storage,
		maxAppendBufferSize: opt.MaxAppendBufferSize,
		rows:                make([]Row, 0),
	}
	if err := ret.load(); err != nil {
		return nil, err
	}
	return &ret, nil
}

// Load loads index from storage
func (idx *Index) load() error {
	// If file doesn't exist yet (lazy creation), Size() will return error - just use 0
	size, _ := idx.storage.Size()
	if size == 0 {
		return nil
	}
	if size%int64(rowSize) != 0 {
		return fmt.Errorf("%w: the size does not align with vector size", ErrIndexStorageCorrupted)
	}
	idx.rows = make([]Row, int(size)/rowSize)
	reader, err := idx.storage.Reader(0)
	if err != nil {
		return err
	}
	defer reader.Close()
	buf := make([]byte, rowSize)
	for i := range idx.rows {
		n, err := reader.Read(buf)
		if err != nil {
			return err
		}
		if n != rowSize {
			return fmt.Errorf("%w: expected %d, actual %d", storage.ErrFileRead, rowSize, n)
		}
		idx.rows[i] = unmarshalRow(buf)
	}
	idx.persistedSize = len(idx.rows)
	return nil
}

// marshalRow serializes a Row into the provided buffer
func marshalRow(row Row, buf []byte) {
	conv.Int64ToBytes(row.Offset, buf[:conv.Int64Size])
	conv.Int64ToBytes(row.Size, buf[conv.Int64Size:conv.Int64Size*2])
	conv.Int64ToBytes(row.MetaOffset, buf[conv.Int64Size*2:conv.Int64Size*3])
	conv.Int64ToBytes(row.MetaSize, buf[conv.Int64Size*3:conv.Int64Size*4])
	buf[conv.Int64Size*4] = row.DataDescriptor
	buf[conv.Int64Size*4+1] = row.MetaDataDescriptor
	buf[conv.Int64Size*4+2] = row.Flags
}

// unmarshalRow deserializes a Row from the provided buffer
func unmarshalRow(buf []byte) Row {
	return Row{
		Offset:             conv.BytesToInt64(buf[:conv.Int64Size]),
		Size:               conv.BytesToInt64(buf[conv.Int64Size : conv.Int64Size*2]),
		MetaOffset:         conv.BytesToInt64(buf[conv.Int64Size*2 : conv.Int64Size*3]),
		MetaSize:           conv.BytesToInt64(buf[conv.Int64Size*3 : conv.Int64Size*4]),
		DataDescriptor:     buf[conv.Int64Size*4],
		MetaDataDescriptor: buf[conv.Int64Size*4+1],
		Flags:              buf[conv.Int64Size*4+2],
	}
}

// IsPersisted returns true if there are no pending writes
func (idx *Index) IsPersisted() bool {
	return len(idx.rows) == idx.persistedSize
}

// Flush flushes index to storage file
func (idx *Index) Flush() error {
	// Check if there are any unsaved rows
	if len(idx.rows) <= idx.persistedSize {
		return nil
	}

	// Use appender to add new rows to the end of the file
	appender, err := idx.storage.Appender()
	if err != nil {
		return err
	}
	defer appender.Close()

	buf := make([]byte, rowSize)

	// Write only the unsaved rows
	for i := idx.persistedSize; i < len(idx.rows); i++ {
		marshalRow(idx.rows[i], buf)
		_, err := appender.Write(buf)
		if err != nil {
			return err
		}
	}

	// Update persisted size
	idx.persistedSize = len(idx.rows)
	return nil
}

// Get returns index record by number
func (idx *Index) Get(n int) (Row, error) {
	if n < 0 || n >= len(idx.rows) {
		return Row{}, ErrIndexOutOfRange
	}
	return idx.rows[n], nil
}

// Append adds new index record and flushes if buffer is full
func (idx *Index) Append(row Row) (int, error) {
	idx.rows = append(idx.rows, row)
	id := len(idx.rows) - 1

	// Calculate the number of unsaved rows
	unsavedCount := len(idx.rows) - idx.persistedSize

	// Special case: maxAppendBufferSize = 0 means immediate saving
	// Otherwise, flush when unsaved count reaches the buffer size
	if unsavedCount >= idx.maxAppendBufferSize {
		return id, idx.Flush()
	}
	return id, nil
}

// Replace replaces the index record by number
func (idx *Index) Replace(n int, row Row) error {
	if n < 0 || n >= len(idx.rows) {
		return ErrIndexOutOfRange
	}

	// Replace the row in memory
	idx.rows[n] = row

	// Update storage if needed
	return idx.updatePersistedRow(n)
}

// updatePersistedRow writes the row at index n to storage if it's already persisted,
// otherwise flushes all pending rows
func (idx *Index) updatePersistedRow(n int) error {
	// If the row is already persisted, write it directly to storage
	if n < idx.persistedSize {
		offset := int64(n * rowSize)
		writer, err := idx.storage.Writer(offset)
		if err != nil {
			return err
		}
		defer writer.Close()

		buf := make([]byte, rowSize)
		marshalRow(idx.rows[n], buf)

		_, err = writer.Write(buf)
		if err != nil {
			return err
		}
	} else {
		// Row is in the append buffer, flush all pending rows
		return idx.Flush()
	}

	return nil
}

// SetFlags sets the specified flags for record using bitwise OR operation
func (idx *Index) SetFlags(n int, flags uint8) error {
	if n < 0 || n >= len(idx.rows) {
		return ErrIndexOutOfRange
	}

	// Set flags using bitwise OR
	idx.rows[n].Flags |= flags

	// Update storage if needed
	return idx.updatePersistedRow(n)
}

// ResetFlags clears the specified flags for record using bitwise AND NOT operation
func (idx *Index) ResetFlags(n int, flags uint8) error {
	if n < 0 || n >= len(idx.rows) {
		return ErrIndexOutOfRange
	}

	// Reset flags using bitwise AND NOT
	idx.rows[n].Flags &^= flags

	// Update storage if needed
	return idx.updatePersistedRow(n)
}

// Truncate truncates the index by truncating storage and clearing all rows
func (idx *Index) Truncate() error {
	// Truncate the storage to zero bytes
	if err := idx.storage.Truncate(); err != nil {
		return err
	}

	// Clear the in-memory rows
	idx.rows = idx.rows[:0]

	// Reset the persisted size counter
	idx.persistedSize = 0

	return nil
}

// Count returns the number of records in the index
func (idx *Index) Count() int {
	return len(idx.rows)
}

// Optimise optimises the index by compacting the storage
func (idx *Index) Optimise() error {
	// Create a new temporary array to hold rows without MarkedForRemoval flag
	rows := make([]Row, 0, len(idx.rows))

	// Iterate through current rows and skip rows marked for removal
	for _, row := range idx.rows {
		if row.Flags&MarkedForRemoval == 0 {
			rows = append(rows, row)
		}
	}

	// Truncate the storage to zero
	if err := idx.storage.Truncate(); err != nil {
		return err
	}

	// Reassign rows to the compacted array
	idx.rows = rows

	// Reset persisted size since we truncated the storage
	idx.persistedSize = 0

	// Flush to save the compacted rows
	return idx.Flush()
}

// Iterator returns an iterator over index rows that yields (position, row) pairs.
// This uses Go 1.23's iter.Seq2 for idiomatic range-over-function iteration.
//
// Example usage:
//
//	for pos, row := range idx.Iterator() {
//	    // use pos and row
//	}
func (idx *Index) Iterator() iter.Seq2[int, *Row] {
	return func(yield func(int, *Row) bool) {
		for i := range idx.rows {
			if !yield(i, &idx.rows[i]) {
				return
			}
		}
	}
}

// Close flushes any unsaved data to storage
func (idx *Index) Close() error {
	return idx.Flush()
}
