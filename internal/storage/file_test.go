package storage

import (
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/webzak/mindstore/internal/testutil"
	"github.com/webzak/mindstore/internal/testutil/assert"
)

func TestFileInit(t *testing.T) {
	tmpDir := testutil.MakeTempDir(t)
	defer os.RemoveAll(tmpDir)

	f := NewFile(filepath.Join(tmpDir, "storage.bin"))
	assert.NilError(t, f.Init())
}

func TestFileSize(t *testing.T) {
	tmpDir := testutil.MakeTempDir(t)
	defer os.RemoveAll(tmpDir)

	f := NewFile(filepath.Join(tmpDir, "storage.bin"))
	_, err := f.Size()
	assert.ErrorIs(t, ErrFileStat, err)
	assert.NilError(t, f.Init())
	size, err := f.Size()
	assert.NilError(t, err)
	assert.Equal(t, int64(0), size)
}

func TestFileAppend(t *testing.T) {
	tmpDir := testutil.MakeTempDir(t)
	defer os.RemoveAll(tmpDir)

	f := NewFile(filepath.Join(tmpDir, "storage.bin"))
	_, err := f.Size()
	assert.ErrorIs(t, ErrFileStat, err)
	assert.NilError(t, f.Init())
	size, err := f.Size()
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
	tmpDir := testutil.MakeTempDir(t)
	defer os.RemoveAll(tmpDir)

	f := NewFile(filepath.Join(tmpDir, "storage.bin"))
	assert.NilError(t, f.Init())

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
	if rb[8] != 20 || rb[11] != 23 {
		t.Fatalf("expected overwritten values, got: %v", rb)
	}
	err = reader.Close()
	assert.NilError(t, err)
}

func TestFileWriterSeekEnd(t *testing.T) {
	tmpDir := testutil.MakeTempDir(t)
	defer os.RemoveAll(tmpDir)

	f := NewFile(filepath.Join(tmpDir, "storage.bin"))
	assert.NilError(t, f.Init())

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
