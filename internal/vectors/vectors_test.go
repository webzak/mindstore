package vectors

import (
	"path/filepath"
	"testing"

	"github.com/webzak/mindstore/internal/testutil/assert"
)

func TestNew(t *testing.T) {
	dir := t.TempDir()

	path := filepath.Join(dir, "vectors.dat")
	opt := Options{
		VectorSize:          128,
		MaxBufferSize:       100,
		MaxAppendBufferSize: 10,
	}

	v, err := New(path, opt)
	assert.NilError(t, err)

	assert.NotNil(t, v, "New() returned nil vectors")
	assert.Equal(t, 0, v.Count())
	assert.Equal(t, 128, v.vectorSize)
	assert.Equal(t, 100, v.maxBufferSize)
	assert.Equal(t, 10, v.maxAppendSize)
}

func TestCount(t *testing.T) {
	dir := t.TempDir()

	path := filepath.Join(dir, "vectors.dat")
	opt := Options{
		VectorSize:          4,
		MaxBufferSize:       100,
		MaxAppendBufferSize: 10,
	}

	v, err := New(path, opt)
	assert.NilError(t, err)

	// Initially empty
	assert.Equal(t, 0, v.Count())

	// Append some vectors
	vec1 := []float32{1.0, 2.0, 3.0, 4.0}
	_, err = v.Append(vec1)
	assert.NilError(t, err)
	assert.Equal(t, 1, v.Count())

	vec2 := []float32{5.0, 6.0, 7.0, 8.0}
	_, err = v.Append(vec2)
	assert.NilError(t, err)
	assert.Equal(t, 2, v.Count())
}

func TestAppend(t *testing.T) {
	dir := t.TempDir()

	path := filepath.Join(dir, "vectors.dat")
	opt := Options{
		VectorSize:          4,
		MaxBufferSize:       100,
		MaxAppendBufferSize: 10,
	}

	v, err := New(path, opt)
	assert.NilError(t, err)

	// Test appending a valid vector
	vec := []float32{1.0, 2.0, 3.0, 4.0}
	_, err = v.Append(vec)
	assert.NilError(t, err)
	assert.Equal(t, 1, v.Count())
	assert.Equal(t, false, v.IsPersisted())

	// Test appending with wrong vector size
	wrongVec := []float32{1.0, 2.0}
	_, err = v.Append(wrongVec)
	assert.NotNilError(t, err)
}

func TestGet(t *testing.T) {
	dir := t.TempDir()

	path := filepath.Join(dir, "vectors.dat")
	opt := Options{
		VectorSize:          4,
		MaxBufferSize:       100,
		MaxAppendBufferSize: 10,
	}

	v, err := New(path, opt)
	assert.NilError(t, err)

	// Test getting from empty storage
	_, err = v.Get(0)
	assert.NotNilError(t, err)

	// Add a vector to append buffer
	vec1 := []float32{1.0, 2.0, 3.0, 4.0}
	_, err = v.Append( vec1)
	assert.NilError(t, err)

	// Get from append buffer
	got, err := v.Get(0)
	assert.NilError(t, err)
	assert.DeepEqual(t, vec1, got)

	// Flush to storage
	err = v.Flush()
	assert.NilError(t, err)

	// Get from persisted storage
	got, err = v.Get(0)
	assert.NilError(t, err)
	assert.DeepEqual(t, vec1, got)

	// Test out of bounds
	_, err = v.Get(10)
	assert.NotNilError(t, err)

	_, err = v.Get(-1)
	assert.NotNilError(t, err)
}

func TestFlush(t *testing.T) {
	dir := t.TempDir()

	path := filepath.Join(dir, "vectors.dat")
	opt := Options{
		VectorSize:          4,
		MaxBufferSize:       100,
		MaxAppendBufferSize: 10,
	}

	v, err := New(path, opt)
	assert.NilError(t, err)

	// Initially persisted (no data)
	assert.Equal(t, true, v.IsPersisted())

	// Add vectors
	vec1 := []float32{1.0, 2.0, 3.0, 4.0}
	vec2 := []float32{5.0, 6.0, 7.0, 8.0}
	_, err = v.Append( vec1)
	assert.NilError(t, err)
	_, err = v.Append( vec2)
	assert.NilError(t, err)

	// Not persisted now
	assert.Equal(t, false, v.IsPersisted())

	// Flush
	err = v.Flush()
	assert.NilError(t, err)

	// Should be persisted
	assert.Equal(t, true, v.IsPersisted())
	assert.Equal(t, 2, v.persistedSize)
	assert.Equal(t, 0, len(v.appendBuffer))

	// Verify data after flush
	got1, err := v.Get(0)
	assert.NilError(t, err)
	assert.DeepEqual(t, vec1, got1)

	got2, err := v.Get(1)
	assert.NilError(t, err)
	assert.DeepEqual(t, vec2, got2)
}

func TestAutoFlush(t *testing.T) {
	dir := t.TempDir()

	path := filepath.Join(dir, "vectors.dat")
	opt := Options{
		VectorSize:          4,
		MaxBufferSize:       100,
		MaxAppendBufferSize: 2, // Auto-flush after 2 vectors
	}

	v, err := New(path, opt)
	assert.NilError(t, err)

	// Add first vector
	vec1 := []float32{1.0, 2.0, 3.0, 4.0}
	_, err = v.Append( vec1)
	assert.NilError(t, err)
	assert.Equal(t, false, v.IsPersisted())

	// Add second vector - should trigger auto-flush
	vec2 := []float32{5.0, 6.0, 7.0, 8.0}
	_, err = v.Append( vec2)
	assert.NilError(t, err)
	assert.Equal(t, true, v.IsPersisted())
	assert.Equal(t, 2, v.persistedSize)
}

func TestReplace(t *testing.T) {
	dir := t.TempDir()

	path := filepath.Join(dir, "vectors.dat")
	opt := Options{
		VectorSize:          4,
		MaxBufferSize:       100,
		MaxAppendBufferSize: 10,
	}

	v, err := New(path, opt)
	assert.NilError(t, err)

	// Add and flush a vector
	vec1 := []float32{1.0, 2.0, 3.0, 4.0}
	_, err = v.Append( vec1)
	assert.NilError(t, err)
	err = v.Flush()
	assert.NilError(t, err)

	// Replace in persisted storage
	newVec := []float32{9.0, 8.0, 7.0, 6.0}
	err = v.Replace(0, newVec)
	assert.NilError(t, err)

	// Verify replacement
	got, err := v.Get(0)
	assert.NilError(t, err)
	assert.DeepEqual(t, newVec, got)

	// Add vector to append buffer
	vec2 := []float32{5.0, 6.0, 7.0, 8.0}
	_, err = v.Append( vec2)
	assert.NilError(t, err)

	// Replace in append buffer
	newVec2 := []float32{10.0, 11.0, 12.0, 13.0}
	err = v.Replace(1, newVec2)
	assert.NilError(t, err)

	// Verify replacement in append buffer (should be flushed)
	got2, err := v.Get(1)
	assert.NilError(t, err)
	assert.DeepEqual(t, newVec2, got2)

	// Test replace with wrong vector size
	wrongVec := []float32{1.0, 2.0}
	err = v.Replace(0, wrongVec)
	assert.NotNilError(t, err)

	// Test replace out of bounds
	err = v.Replace(10, newVec)
	assert.NotNilError(t, err)
}

func TestDelete(t *testing.T) {
	dir := t.TempDir()

	path := filepath.Join(dir, "vectors.dat")
	opt := Options{
		VectorSize:          4,
		MaxBufferSize:       100,
		MaxAppendBufferSize: 10,
	}

	v, err := New(path, opt)
	assert.NilError(t, err)

	// Add multiple vectors and flush
	vec1 := []float32{1.0, 2.0, 3.0, 4.0}
	vec2 := []float32{5.0, 6.0, 7.0, 8.0}
	vec3 := []float32{9.0, 10.0, 11.0, 12.0}
	vec4 := []float32{13.0, 14.0, 15.0, 16.0}

	_, err = v.Append( vec1)
	assert.NilError(t, err)
	_, err = v.Append( vec2)
	assert.NilError(t, err)
	_, err = v.Append( vec3)
	assert.NilError(t, err)
	_, err = v.Append( vec4)
	assert.NilError(t, err)
	err = v.Flush()
	assert.NilError(t, err)

	assert.Equal(t, 4, v.Count())

	// Delete middle vectors (index 1 and 2)
	err = v.Delete([]int32{1, 2})
	assert.NilError(t, err)

	// Verify count
	assert.Equal(t, 2, v.Count())

	// Verify remaining vectors
	got0, err := v.Get(0)
	assert.NilError(t, err)
	assert.DeepEqual(t, vec1, got0)

	got1, err := v.Get(1)
	assert.NilError(t, err)
	assert.DeepEqual(t, vec4, got1)

	// Test deleting out of bounds
	err = v.Delete([]int32{10})
	assert.NotNilError(t, err)

	// Test deleting empty list
	err = v.Delete([]int32{})
	assert.NilError(t, err)
}

func TestDeleteWithAppendBuffer(t *testing.T) {
	dir := t.TempDir()

	path := filepath.Join(dir, "vectors.dat")
	opt := Options{
		VectorSize:          4,
		MaxBufferSize:       100,
		MaxAppendBufferSize: 10,
	}

	v, err := New(path, opt)
	assert.NilError(t, err)

	// Add vector to append buffer
	vec1 := []float32{1.0, 2.0, 3.0, 4.0}
	_, err = v.Append( vec1)
	assert.NilError(t, err)

	// Delete should flush append buffer first
	vec2 := []float32{5.0, 6.0, 7.0, 8.0}
	_, err = v.Append( vec2)
	assert.NilError(t, err)

	err = v.Delete([]int32{0})
	assert.NilError(t, err)

	// Should be persisted after delete
	assert.Equal(t, true, v.IsPersisted())
	assert.Equal(t, 1, v.Count())
}

func TestClose(t *testing.T) {
	dir := t.TempDir()

	path := filepath.Join(dir, "vectors.dat")
	opt := Options{
		VectorSize:          4,
		MaxBufferSize:       100,
		MaxAppendBufferSize: 10,
	}

	v, err := New(path, opt)
	assert.NilError(t, err)

	// Add vector to append buffer
	vec1 := []float32{1.0, 2.0, 3.0, 4.0}
	_, err = v.Append( vec1)
	assert.NilError(t, err)

	// Close should flush
	err = v.Close()
	assert.NilError(t, err)
	assert.Equal(t, true, v.IsPersisted())

	// Reopen and verify data was flushed
	v2, err := New(path, opt)
	assert.NilError(t, err)
	assert.Equal(t, 1, v2.Count())

	got, err := v2.Get(0)
	assert.NilError(t, err)
	assert.DeepEqual(t, vec1, got)
}

func TestCloseWithReaderFD(t *testing.T) {
	dir := t.TempDir()

	path := filepath.Join(dir, "vectors.dat")
	opt := Options{
		VectorSize:          4,
		MaxBufferSize:       100,
		MaxAppendBufferSize: 10,
	}

	v, err := New(path, opt)
	assert.NilError(t, err)

	// Add and flush vector
	vec1 := []float32{1.0, 2.0, 3.0, 4.0}
	_, err = v.Append( vec1)
	assert.NilError(t, err)
	err = v.Flush()
	assert.NilError(t, err)

	// Get will open readerFD
	_, err = v.Get(0)
	assert.NilError(t, err)

	// readerFD should be open
	assert.NotNil(t, v.readerFD, "readerFD should be open after Get")

	// Close should close readerFD
	err = v.Close()
	assert.NilError(t, err)

	// readerFD should be nil
	if v.readerFD != nil {
		t.Error("readerFD should be nil after Close")
	}
}

func TestIterator(t *testing.T) {
	dir := t.TempDir()

	path := filepath.Join(dir, "vectors.dat")
	opt := Options{
		VectorSize:          4,
		MaxBufferSize:       2, // Small buffer for chunked reading
		MaxAppendBufferSize: 10,
	}

	v, err := New(path, opt)
	assert.NilError(t, err)

	// Add and flush some vectors
	vec1 := []float32{1.0, 2.0, 3.0, 4.0}
	vec2 := []float32{5.0, 6.0, 7.0, 8.0}
	vec3 := []float32{9.0, 10.0, 11.0, 12.0}

	_, err = v.Append( vec1)
	assert.NilError(t, err)
	_, err = v.Append( vec2)
	assert.NilError(t, err)
	err = v.Flush()
	assert.NilError(t, err)

	// Add one to append buffer
	_, err = v.Append( vec3)
	assert.NilError(t, err)

	// Iterate and verify
	expected := [][]float32{vec1, vec2, vec3}
	count := 0
	for idx, vec := range v.Iterator() {
		assert.Equal(t, count, idx)
		assert.DeepEqual(t, expected[count], vec)
		count++
	}

	assert.Equal(t, 3, count)
}

func TestIteratorEmpty(t *testing.T) {
	dir := t.TempDir()

	path := filepath.Join(dir, "vectors.dat")
	opt := Options{
		VectorSize:          4,
		MaxBufferSize:       100,
		MaxAppendBufferSize: 10,
	}

	v, err := New(path, opt)
	assert.NilError(t, err)

	count := 0
	for range v.Iterator() {
		count++
	}
	assert.Equal(t, 0, count)
}

func TestIteratorOnlyAppendBuffer(t *testing.T) {
	dir := t.TempDir()

	path := filepath.Join(dir, "vectors.dat")
	opt := Options{
		VectorSize:          4,
		MaxBufferSize:       100,
		MaxAppendBufferSize: 10,
	}

	v, err := New(path, opt)
	assert.NilError(t, err)

	// Add vectors only to append buffer
	vec1 := []float32{1.0, 2.0, 3.0, 4.0}
	vec2 := []float32{5.0, 6.0, 7.0, 8.0}

	_, err = v.Append( vec1)
	assert.NilError(t, err)
	_, err = v.Append( vec2)
	assert.NilError(t, err)

	// Iterate without flushing
	expected := [][]float32{vec1, vec2}
	count := 0
	for idx, vec := range v.Iterator() {
		assert.Equal(t, count, idx)
		assert.DeepEqual(t, expected[count], vec)
		count++
	}

	assert.Equal(t, 2, count)
}

func TestPersistence(t *testing.T) {
	dir := t.TempDir()

	path := filepath.Join(dir, "vectors.dat")
	opt := Options{
		VectorSize:          4,
		MaxBufferSize:       100,
		MaxAppendBufferSize: 10,
	}

	// Create and add vectors
	v, err := New(path, opt)
	assert.NilError(t, err)

	vec1 := []float32{1.0, 2.0, 3.0, 4.0}
	vec2 := []float32{5.0, 6.0, 7.0, 8.0}

	_, err = v.Append( vec1)
	assert.NilError(t, err)
	_, err = v.Append( vec2)
	assert.NilError(t, err)
	err = v.Flush()
	assert.NilError(t, err)

	// Reopen and verify
	v2, err := New(path, opt)
	assert.NilError(t, err)

	assert.Equal(t, 2, v2.Count())
	assert.Equal(t, 2, v2.persistedSize)

	got1, err := v2.Get(0)
	assert.NilError(t, err)
	assert.DeepEqual(t, vec1, got1)

	got2, err := v2.Get(1)
	assert.NilError(t, err)
	assert.DeepEqual(t, vec2, got2)
}

func TestReaderFDReuse(t *testing.T) {
	dir := t.TempDir()

	path := filepath.Join(dir, "vectors.dat")
	opt := Options{
		VectorSize:          4,
		MaxBufferSize:       100,
		MaxAppendBufferSize: 10,
	}

	v, err := New(path, opt)
	assert.NilError(t, err)

	// Add and flush vectors
	vec1 := []float32{1.0, 2.0, 3.0, 4.0}
	vec2 := []float32{5.0, 6.0, 7.0, 8.0}
	vec3 := []float32{9.0, 10.0, 11.0, 12.0}

	_, err = v.Append( vec1)
	assert.NilError(t, err)
	_, err = v.Append( vec2)
	assert.NilError(t, err)
	_, err = v.Append( vec3)
	assert.NilError(t, err)
	err = v.Flush()
	assert.NilError(t, err)

	// First Get should open readerFD
	_, err = v.Get(0)
	assert.NilError(t, err)

	assert.NotNil(t, v.readerFD, "readerFD should be open after first Get")
	firstFD := v.readerFD

	// Second Get should reuse the same FD
	_, err = v.Get(1)
	assert.NilError(t, err)

	assert.Equal(t, firstFD, v.readerFD)

	// Third Get should still reuse the same FD
	_, err = v.Get(2)
	assert.NilError(t, err)

	assert.Equal(t, firstFD, v.readerFD)
}
