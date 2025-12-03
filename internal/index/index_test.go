package index

import (
	"path/filepath"
	"testing"

	"github.com/webzak/mindstore/internal/testutil/assert"
)

var opts Options = Options{
	MaxAppendBufferSize: 64,
}

func TestNew(t *testing.T) {
	dir := t.TempDir()

	path := filepath.Join(dir, "test.idx")
	idx, err := New(path, opts)
	assert.NilError(t, err)
	assert.NotNil(t, idx, "New() returned nil index")
	assert.Equal(t, 0, idx.Count())
}

func TestNewWithOptions(t *testing.T) {
	dir := t.TempDir()

	path := filepath.Join(dir, "test.idx")
	opt := Options{
		MaxAppendBufferSize: 10,
	}
	idx, err := New(path, opt)
	assert.NilError(t, err)
	assert.Equal(t, 10, idx.maxAppendBufferSize)
}

func TestAppend(t *testing.T) {
	dir := t.TempDir()

	path := filepath.Join(dir, "test.idx")
	idx, err := New(path, opts)
	assert.NilError(t, err)

	row := Row{
		Offset:             100,
		Size:               200,
		MetaOffset:         300,
		MetaSize:           400,
		DataDescriptor:     1,
		MetaDataDescriptor: 2,
		Flags:              0,
	}

	id, err := idx.Append(row)
	assert.NilError(t, err)
	assert.Equal(t, 0, id)
	assert.Equal(t, 1, idx.Count())
}

func TestGet(t *testing.T) {
	dir := t.TempDir()

	path := filepath.Join(dir, "test.idx")
	idx, err := New(path, opts)
	assert.NilError(t, err)

	row := Row{
		Offset:             100,
		Size:               200,
		MetaOffset:         300,
		MetaSize:           400,
		DataDescriptor:     1,
		MetaDataDescriptor: 2,
		Flags:              0,
	}

	idx.Append(row)

	got, err := idx.Get(0)
	assert.NilError(t, err)
	assert.Equal(t, row.Offset, got.Offset)
	assert.Equal(t, row.Size, got.Size)
}

func TestGetOutOfRange(t *testing.T) {
	dir := t.TempDir()

	path := filepath.Join(dir, "test.idx")
	idx, err := New(path, opts)
	assert.NilError(t, err)

	_, err = idx.Get(0)
	assert.ErrorIs(t, ErrIndexOutOfRange, err)
}

func TestFlush(t *testing.T) {
	dir := t.TempDir()

	path := filepath.Join(dir, "test.idx")
	idx, err := New(path, opts)
	assert.NilError(t, err)

	row := Row{Offset: 100, Size: 200}
	idx.Append(row)

	assert.Equal(t, false, idx.IsPersisted())

	err = idx.Flush()
	assert.NilError(t, err)

	assert.Equal(t, true, idx.IsPersisted())
}

func TestPersistence(t *testing.T) {
	dir := t.TempDir()

	path := filepath.Join(dir, "test.idx")

	// Create index and add rows
	idx, err := New(path, opts)
	assert.NilError(t, err)

	row1 := Row{Offset: 100, Size: 200, DataDescriptor: 1}
	row2 := Row{Offset: 300, Size: 400, DataDescriptor: 2}

	idx.Append(row1)
	idx.Append(row2)
	idx.Flush()

	// Reload index from storage
	idx2, err := New(path, opts)
	assert.NilError(t, err)

	assert.Equal(t, 2, idx2.Count())

	got, _ := idx2.Get(0)
	assert.Equal(t, row1.Offset, got.Offset)
	assert.Equal(t, row1.Size, got.Size)
}

func TestReplace(t *testing.T) {
	dir := t.TempDir()

	path := filepath.Join(dir, "test.idx")
	idx, err := New(path, opts)
	assert.NilError(t, err)

	row := Row{Offset: 100, Size: 200}
	idx.Append(row)
	idx.Flush()

	newRow := Row{Offset: 500, Size: 600}
	err = idx.Replace(0, newRow)
	assert.NilError(t, err)

	got, _ := idx.Get(0)
	assert.Equal(t, newRow.Offset, got.Offset)
	assert.Equal(t, newRow.Size, got.Size)
}

func TestSetFlags(t *testing.T) {
	dir := t.TempDir()

	path := filepath.Join(dir, "test.idx")
	idx, err := New(path, opts)
	assert.NilError(t, err)

	row := Row{Offset: 100, Size: 200, Flags: 0}
	idx.Append(row)
	idx.Flush()

	err = idx.SetFlags(0, MarkedForRemoval)
	assert.NilError(t, err)

	got, _ := idx.Get(0)
	if got.Flags&MarkedForRemoval == 0 {
		t.Error("SetFlags() did not set MarkedForRemoval flag")
	}
}

func TestResetFlags(t *testing.T) {
	dir := t.TempDir()

	path := filepath.Join(dir, "test.idx")
	idx, err := New(path, opts)
	assert.NilError(t, err)

	row := Row{Offset: 100, Size: 200, Flags: MarkedForRemoval}
	idx.Append(row)
	idx.Flush()

	err = idx.ResetFlags(0, MarkedForRemoval)
	assert.NilError(t, err)

	got, _ := idx.Get(0)
	if got.Flags&MarkedForRemoval != 0 {
		t.Error("ResetFlags() did not clear MarkedForRemoval flag")
	}
}

func TestTruncate(t *testing.T) {
	dir := t.TempDir()

	path := filepath.Join(dir, "test.idx")
	idx, err := New(path, opts)
	assert.NilError(t, err)

	row := Row{Offset: 100, Size: 200}
	idx.Append(row)
	idx.Flush()

	err = idx.Truncate()
	assert.NilError(t, err)

	assert.Equal(t, 0, idx.Count())
}

func TestOptimise(t *testing.T) {
	dir := t.TempDir()

	path := filepath.Join(dir, "test.idx")
	idx, err := New(path, opts)
	assert.NilError(t, err)

	// Add multiple rows
	idx.Append(Row{Offset: 100, Size: 200})
	idx.Append(Row{Offset: 300, Size: 400})
	idx.Append(Row{Offset: 500, Size: 600})
	idx.Flush()

	// Mark middle row for removal
	idx.SetFlags(1, MarkedForRemoval)

	// Optimise
	err = idx.Optimise()
	assert.NilError(t, err)

	assert.Equal(t, 2, idx.Count())

	// Verify remaining rows
	row0, _ := idx.Get(0)
	assert.Equal(t, int64(100), row0.Offset)

	row1, _ := idx.Get(1)
	assert.Equal(t, int64(500), row1.Offset)
}

func TestIterator(t *testing.T) {
	dir := t.TempDir()

	path := filepath.Join(dir, "test.idx")
	idx, err := New(path, opts)
	assert.NilError(t, err)

	// Add multiple rows
	idx.Append(Row{Offset: 100, Size: 200})
	idx.Append(Row{Offset: 300, Size: 400})
	idx.Append(Row{Offset: 500, Size: 600})

	count := 0
	for pos, row := range idx.Iterator() {
		assert.NotNil(t, row, "Iterator returned nil row")
		assert.Equal(t, count, pos)
		count++
	}

	assert.Equal(t, 3, count)
}

func TestIteratorEmpty(t *testing.T) {
	dir := t.TempDir()

	path := filepath.Join(dir, "test.idx")
	idx, err := New(path, opts)
	assert.NilError(t, err)

	count := 0
	for range idx.Iterator() {
		count++
	}
	assert.Equal(t, 0, count)
}

func TestVectorsMap(t *testing.T) {
	dir := t.TempDir()

	path := filepath.Join(dir, "test.idx")
	idx, err := New(path, opts)
	assert.NilError(t, err)

	// Add rows with vectors at different positions
	idx.Append(Row{Offset: 100, Size: 200, Vector: 0})  // Index 0 -> Vector pos 0
	idx.Append(Row{Offset: 300, Size: 400, Vector: -1}) // Index 1 -> No vector
	idx.Append(Row{Offset: 500, Size: 600, Vector: 1})  // Index 2 -> Vector pos 1
	idx.Append(Row{Offset: 700, Size: 800, Vector: 2})  // Index 3 -> Vector pos 2

	m := idx.VectorsMap()

	// Verify the map has correct mappings
	assert.Equal(t, 3, len(m))
	assert.Equal(t, 0, m[0])
	assert.Equal(t, 2, m[1])
	assert.Equal(t, 3, m[2])
}

func TestVectorsMapEmpty(t *testing.T) {
	dir := t.TempDir()

	path := filepath.Join(dir, "test.idx")
	idx, err := New(path, opts)
	assert.NilError(t, err)

	m := idx.VectorsMap()
	assert.Equal(t, 0, len(m))
}

func TestVectorsMapNoVectors(t *testing.T) {
	dir := t.TempDir()

	path := filepath.Join(dir, "test.idx")
	idx, err := New(path, opts)
	assert.NilError(t, err)

	// Add rows without vectors (Vector = -1)
	idx.Append(Row{Offset: 100, Size: 200, Vector: -1})
	idx.Append(Row{Offset: 300, Size: 400, Vector: -1})
	idx.Append(Row{Offset: 500, Size: 600, Vector: -1})

	m := idx.VectorsMap()
	assert.Equal(t, 0, len(m))
}

func TestVectorsMapAfterPersistence(t *testing.T) {
	dir := t.TempDir()

	path := filepath.Join(dir, "test.idx")
	idx, err := New(path, opts)
	assert.NilError(t, err)

	// Add rows with vectors
	idx.Append(Row{Offset: 100, Size: 200, Vector: 5})
	idx.Append(Row{Offset: 300, Size: 400, Vector: -1})
	idx.Append(Row{Offset: 500, Size: 600, Vector: 10})
	idx.Flush()

	// Reload index
	idx2, err := New(path, opts)
	assert.NilError(t, err)

	m := idx2.VectorsMap()
	assert.Equal(t, 2, len(m))
	assert.Equal(t, 0, m[5])
	assert.Equal(t, 2, m[10])
}
