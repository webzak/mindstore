package index

import (
	"errors"
	"fmt"
	"io"

	"github.com/webzak/mindstore/internal/engine/conv"
	"github.com/webzak/mindstore/internal/engine/storage"
)

const (
	rowSize                          = conv.Int64Size * 2
	DefaultMaxAppendBufferSize       = 64
	MarkedForRemoval           uint8 = 1 << 0
)

var (
	ErrIndexOutOfRange = errors.New("index out of range")
)

// Row is the data containing in the index row
type Row struct {
	// Offset is the shift from start in bytes
	Offset int64
	// Size is the data length in bytes
	Size int32
	// Type is the type of data text, image, video, audio.. (data.Type)
	Type uint8
	// Flags are bit options
	Flags uint8
	// Reserved space to align the size
	Reserved1 uint8
	Reserved2 uint8
}

// IndexOptions are options for index
type IndexOptions struct {
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
func New(path string, opt *IndexOptions) (*Index, error) {
	storage := storage.NewFile(path)
	if err := storage.Init(); err != nil {
		return nil, err
	}
	ret := Index{
		storage:             storage,
		maxAppendBufferSize: DefaultMaxAppendBufferSize,
		rows:                make([]Row, 0),
	}
	if opt != nil {
		ret.maxAppendBufferSize = opt.MaxAppendBufferSize
	}
	if err := ret.load(); err != nil {
		return nil, err
	}
	return &ret, nil
}

// Load loads index from storage
func (idx *Index) load() error {
	size, err := idx.storage.Size()
	if err != nil {
		return err
	}
	if size == 0 {
		return nil
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
		idx.rows[i].Offset = conv.BytesToInt64(buf[:conv.Int64Size])
		idx.rows[i].Size = conv.BytesToInt32(buf[conv.Int64Size : conv.Int64Size+conv.Int32Size])
		idx.rows[i].Type = buf[conv.Int64Size+conv.Int32Size]
		idx.rows[i].Flags = buf[conv.Int64Size+conv.Int32Size+1]
	}
	idx.persistedSize = len(idx.rows)
	return nil
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

	// Write only the unsaved rows
	for i := idx.persistedSize; i < len(idx.rows); i++ {
		row := idx.rows[i]
		buf := make([]byte, rowSize)
		conv.Int64ToBytes(row.Offset, buf[:conv.Int64Size])
		conv.Int32ToBytes(row.Size, buf[conv.Int64Size:conv.Int64Size+conv.Int32Size])
		buf[conv.Int64Size+conv.Int32Size] = row.Type
		buf[conv.Int64Size+conv.Int32Size+1] = row.Flags
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
func (idx *Index) Append(row Row) error {
	idx.rows = append(idx.rows, row)

	// Calculate the number of unsaved rows
	unsavedCount := len(idx.rows) - idx.persistedSize

	// Special case: maxAppendBufferSize = 0 means immediate saving
	// Otherwise, flush when unsaved count reaches the buffer size
	if unsavedCount >= idx.maxAppendBufferSize {
		return idx.Flush()
	}

	return nil
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

		row := idx.rows[n]
		buf := make([]byte, rowSize)
		conv.Int64ToBytes(row.Offset, buf[:conv.Int64Size])
		conv.Int32ToBytes(row.Size, buf[conv.Int64Size:conv.Int64Size+conv.Int32Size])
		buf[conv.Int64Size+conv.Int32Size] = row.Type
		buf[conv.Int64Size+conv.Int32Size+1] = row.Flags

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

// Destroy destroys the index by truncating storage and clearing all rows
func (idx *Index) Destroy() error {
	// Truncate the storage to zero bytes
	if err := idx.storage.Truncate(0); err != nil {
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
	if err := idx.storage.Truncate(0); err != nil {
		return err
	}

	// Reassign rows to the compacted array
	idx.rows = rows

	// Reset persisted size since we truncated the storage
	idx.persistedSize = 0

	// Flush to save the compacted rows
	return idx.Flush()
}

// Iterator is an iterator over index rows
type Iterator struct {
	idx      *Index
	position int
}

// Iterator creates a new iterator over index rows
func (idx *Index) Iterator() *Iterator {
	return &Iterator{
		idx:      idx,
		position: 0,
	}
}

// Next returns the next row and error
// Returns nil and io.EOF when iteration is complete
func (it *Iterator) Next() (*Row, error) {
	if it.position >= len(it.idx.rows) {
		return nil, io.EOF
	}

	row := it.idx.rows[it.position]
	it.position++

	return &row, nil
}
