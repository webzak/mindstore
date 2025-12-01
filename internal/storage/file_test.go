package storage

import (
	"io"
	"path/filepath"
	"testing"

	"github.com/webzak/mindstore/internal/testutil/assert"
)

func TestFileInit(t *testing.T) {
	dir := t.TempDir()

	f := NewFile(filepath.Join(dir, "storage.bin"))
	assert.NilError(t, f.Init(true))
}

func TestFileSize(t *testing.T) {
	dir := t.TempDir()

	f := NewFile(filepath.Join(dir, "storage.bin"))
	// File doesn't exist yet (lazy creation), Size() returns 0
	size, err := f.Size()
	assert.NilError(t, err)
	assert.Equal(t, int64(0), size)

	// After Init(true), file is created
	assert.NilError(t, f.Init(true))
	size, err = f.Size()
	assert.NilError(t, err)
	assert.Equal(t, int64(0), size)
}

func TestFileAppend(t *testing.T) {
	dir := t.TempDir()

	f := NewFile(filepath.Join(dir, "storage.bin"))
	// File doesn't exist yet (lazy creation), Size() returns 0
	size, err := f.Size()
	assert.NilError(t, err)
	assert.Equal(t, int64(0), size)

	assert.NilError(t, f.Init(true))
	size, err = f.Size()
	assert.NilError(t, err)
	assert.Equal(t, int64(0), size)

	_, err = f.Reader(0)
	assert.NilError(t, err)

	appender, err := f.Appender()
	assert.NilError(t, err)
	data := []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}
	n, err := appender.Write(data)
	assert.NilError(t, err)
	assert.Equal(t, 16, n)
	n, err = appender.Write(data)
	assert.NilError(t, err)
	assert.Equal(t, 16, n)
	err = appender.Close()
	assert.NilError(t, err)
	size, err = f.Size()
	assert.NilError(t, err)
	assert.Equal(t, int64(32), size)
	rb := make([]byte, 64)
	reader, err := f.Reader(14)
	assert.NilError(t, err)
	n, err = reader.Read(rb)
	assert.NilError(t, err)
	assert.Equal(t, 18, n)
	n, err = reader.Read(rb)
	assert.ErrorIs(t, io.EOF, err)
	assert.Equal(t, 0, n)
	assert.Equal(t, byte(14), rb[0])
	assert.Equal(t, byte(15), rb[17])
	err = reader.Close()
	assert.NilError(t, err)
	_, err = f.Reader(333)
	assert.NilError(t, err)
}

func TestFileWriter(t *testing.T) {
	dir := t.TempDir()

	f := NewFile(filepath.Join(dir, "storage.bin"))
	assert.NilError(t, f.Init(true))

	writer, err := f.Writer(0)
	assert.NilError(t, err)
	data := []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}
	n, err := writer.Write(data)
	assert.NilError(t, err)
	assert.Equal(t, 16, n)
	err = writer.Close()
	assert.NilError(t, err)

	size, err := f.Size()
	assert.NilError(t, err)
	assert.Equal(t, int64(16), size)

	// Overwrite part of the file
	writer, err = f.Writer(8)
	assert.NilError(t, err)
	data2 := []byte{20, 21, 22, 23}
	n, err = writer.Write(data2)
	assert.NilError(t, err)
	assert.Equal(t, 4, n)
	err = writer.Close()
	assert.NilError(t, err)

	// Verify content
	rb := make([]byte, 16)
	reader, err := f.Reader(0)
	assert.NilError(t, err)
	n, err = reader.Read(rb)
	assert.NilError(t, err)
	assert.Equal(t, 16, n)
	assert.Equal(t, byte(20), rb[8])
	assert.Equal(t, byte(23), rb[11])
	err = reader.Close()
	assert.NilError(t, err)
}

func TestFileWriterSeekEnd(t *testing.T) {
	dir := t.TempDir()

	f := NewFile(filepath.Join(dir, "storage.bin"))
	assert.NilError(t, f.Init(true))

	// Write initial data
	writer, err := f.Writer(0)
	assert.NilError(t, err)
	data := []byte{0, 1, 2, 3, 4, 5, 6, 7}
	n, err := writer.Write(data)
	assert.NilError(t, err)
	assert.Equal(t, 8, n)
	err = writer.Close()
	assert.NilError(t, err)

	size, err := f.Size()
	assert.NilError(t, err)
	assert.Equal(t, int64(8), size)

	// Use Writer(-1) to append at end
	writer, err = f.Writer(-1)
	assert.NilError(t, err)
	data2 := []byte{10, 11, 12, 13}
	n, err = writer.Write(data2)
	assert.NilError(t, err)
	assert.Equal(t, 4, n)
	err = writer.Close()
	assert.NilError(t, err)

	// Verify size increased
	size, err = f.Size()
	assert.NilError(t, err)
	assert.Equal(t, int64(12), size)

	// Verify content - read all
	rb := make([]byte, 12)
	reader, err := f.Reader(0)
	assert.NilError(t, err)
	n, err = reader.Read(rb)
	assert.NilError(t, err)
	assert.Equal(t, 12, n)
	assert.Equal(t, byte(0), rb[0])
	assert.Equal(t, byte(7), rb[7])
	assert.Equal(t, byte(10), rb[8])
	assert.Equal(t, byte(13), rb[11])
	err = reader.Close()
	assert.NilError(t, err)

	// Test that Writer with offset < -1 returns error
	_, err = f.Writer(-2)
	assert.ErrorIs(t, ErrFileInvalidOffset, err)
}

func TestFileInitNoCreate(t *testing.T) {
	dir := t.TempDir()

	// Test Init(false) when file doesn't exist - should succeed without creating file
	f := NewFile(filepath.Join(dir, "storage.bin"))
	assert.NilError(t, f.Init(false))

	// File doesn't exist yet (lazy creation), Size() returns 0
	size, err := f.Size()
	assert.NilError(t, err)
	assert.Equal(t, int64(0), size)

	// Writer should create the file automatically
	writer, err := f.Writer(0)
	assert.NilError(t, err)
	data := []byte{1, 2, 3, 4}
	n, err := writer.Write(data)
	assert.NilError(t, err)
	assert.Equal(t, 4, n)
	err = writer.Close()
	assert.NilError(t, err)

	// Now Size should work
	size, err = f.Size()
	assert.NilError(t, err)
	assert.Equal(t, int64(4), size)
}

func TestFileInitNoCreateExistingFile(t *testing.T) {
	dir := t.TempDir()

	// First create a file with Init(true)
	f := NewFile(filepath.Join(dir, "storage.bin"))
	assert.NilError(t, f.Init(true))

	// Write some data
	writer, err := f.Writer(0)
	assert.NilError(t, err)
	data := []byte{10, 20, 30}
	_, err = writer.Write(data)
	assert.NilError(t, err)
	err = writer.Close()
	assert.NilError(t, err)

	// Create new File instance with same path and call Init(false)
	f2 := NewFile(filepath.Join(dir, "storage.bin"))
	assert.NilError(t, f2.Init(false))

	// File should exist and have correct size
	size, err := f2.Size()
	assert.NilError(t, err)
	assert.Equal(t, int64(3), size)
}

func TestFileTruncate(t *testing.T) {
	dir := t.TempDir()

	f := NewFile(filepath.Join(dir, "storage.bin"))
	assert.NilError(t, f.Init(true))

	// Write some data
	writer, err := f.Writer(0)
	assert.NilError(t, err)
	data := []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}
	n, err := writer.Write(data)
	assert.NilError(t, err)
	assert.Equal(t, 10, n)
	err = writer.Close()
	assert.NilError(t, err)

	// Verify size
	size, err := f.Size()
	assert.NilError(t, err)
	assert.Equal(t, int64(10), size)

	// Truncate to zero
	err = f.Truncate()
	assert.NilError(t, err)

	// Verify size is now zero
	size, err = f.Size()
	assert.NilError(t, err)
	assert.Equal(t, int64(0), size)
}
