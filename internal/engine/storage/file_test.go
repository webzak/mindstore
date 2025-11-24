package storage

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/webzak/mindstore/internal/testutil"
)

func TestFileInit(t *testing.T) {
	tmpDir := testutil.MakeTempDir(t)
	defer os.RemoveAll(tmpDir)

	f := NewFile(filepath.Join(tmpDir, "storage.bin"))
	if err := f.Init(); err != nil {
		t.Fatal(err)
	}
}

func TestFileSize(t *testing.T) {
	tmpDir := testutil.MakeTempDir(t)
	defer os.RemoveAll(tmpDir)

	f := NewFile(filepath.Join(tmpDir, "storage.bin"))
	_, err := f.Size()
	if err == nil || !errors.Is(err, ErrFileStat) {
		t.Fatalf("error expected to be ErrFileStat, returned: %v", err)
	}
	if err := f.Init(); err != nil {
		t.Fatal(err)
	}
	size, err := f.Size()
	if err != nil {
		t.Fatal(err)
	}
	if size != 0 {
		t.Fatalf("size expected to be 0, returned: %d", size)
	}
}

func TestFileAppend(t *testing.T) {
	tmpDir := testutil.MakeTempDir(t)
	defer os.RemoveAll(tmpDir)

	f := NewFile(filepath.Join(tmpDir, "storage.bin"))
	_, err := f.Size()
	if err == nil || !errors.Is(err, ErrFileStat) {
		t.Fatalf("error expected to be ErrFileStat, returned: %v", err)
	}
	if err := f.Init(); err != nil {
		t.Fatal(err)
	}
	size, err := f.Size()
	if err != nil {
		t.Fatal(err)
	}
	if size != 0 {
		t.Fatal("size expected to be zero on new memory storage")
	}

	_, err = f.Reader(0)
	if err != nil {
		t.Fatalf("error expected to be nil, returned: %v", err)
	}

	appender, err := f.Appender()
	if err != nil {
		t.Fatalf("error expected to be nil, returned: %v", err)
	}
	data := []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}
	n, err := appender.Write(data)
	if err != nil {
		t.Fatalf("error expected to be nil, returned: %v", err)
	}
	if n != 16 {
		t.Fatalf("expected to write 16 bytes, actual: %d", n)
	}
	n, err = appender.Write(data)
	if err != nil {
		t.Fatalf("error expected to be nil, returned: %v", err)
	}
	if n != 16 {
		t.Fatalf("expected to write 16 bytes, actual: %d", n)
	}
	err = appender.Close()
	if err != nil {
		t.Fatal(err)
	}
	size, err = f.Size()
	if err != nil {
		t.Fatal(err)
	}
	if size != 32 {
		t.Fatalf("size expected to be 32, actual: %d", size)
	}
	rb := make([]byte, 64)
	reader, err := f.Reader(14)
	if err != nil {
		t.Fatalf("error expected to be nil, returned: %v", err)
	}
	n, err = reader.Read(rb)
	if err != nil {
		t.Fatalf("error expected to be nil, returned %v", err)
	}
	if n != 18 {
		t.Fatalf("expected to read 18 bytes, actual: %d", n)
	}
	n, err = reader.Read(rb)
	if err != io.EOF {
		t.Fatalf("error expected to be io.EOF, returned %v", err)
	}
	if n != 0 {
		t.Fatalf("expected to read 0 bytes, actual: %d", n)
	}
	if rb[0] != 14 {
		t.Fatalf("expected value 14, actual: %d", rb[0])
	}
	if rb[17] != 15 {
		t.Fatalf("expected value 14, actual: %d", rb[17])
	}
	err = reader.Close()
	if err != nil {
		t.Fatal(err)
	}
	_, err = f.Reader(333)
	if err != nil {
		t.Fatalf("error expected to be nil, returned: %v", err)
	}
}

func TestFileWriter(t *testing.T) {
	tmpDir := testutil.MakeTempDir(t)
	defer os.RemoveAll(tmpDir)

	f := NewFile(filepath.Join(tmpDir, "storage.bin"))
	if err := f.Init(); err != nil {
		t.Fatal(err)
	}

	writer, err := f.Writer(0)
	if err != nil {
		t.Fatalf("error expected to be nil, returned: %v", err)
	}
	data := []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}
	n, err := writer.Write(data)
	if err != nil {
		t.Fatalf("error expected to be nil, returned: %v", err)
	}
	if n != 16 {
		t.Fatalf("expected to write 16 bytes, actual: %d", n)
	}
	err = writer.Close()
	if err != nil {
		t.Fatal(err)
	}

	size, err := f.Size()
	if err != nil {
		t.Fatal(err)
	}
	if size != 16 {
		t.Fatalf("size expected to be 16, actual: %d", size)
	}

	// Overwrite part of the file
	writer, err = f.Writer(8)
	if err != nil {
		t.Fatalf("error expected to be nil, returned: %v", err)
	}
	data2 := []byte{20, 21, 22, 23}
	n, err = writer.Write(data2)
	if err != nil {
		t.Fatalf("error expected to be nil, returned: %v", err)
	}
	if n != 4 {
		t.Fatalf("expected to write 4 bytes, actual: %d", n)
	}
	err = writer.Close()
	if err != nil {
		t.Fatal(err)
	}

	// Verify content
	rb := make([]byte, 16)
	reader, err := f.Reader(0)
	if err != nil {
		t.Fatalf("error expected to be nil, returned: %v", err)
	}
	n, err = reader.Read(rb)
	if err != nil {
		t.Fatalf("error expected to be nil, returned %v", err)
	}
	if n != 16 {
		t.Fatalf("expected to read 16 bytes, actual: %d", n)
	}
	if rb[8] != 20 || rb[11] != 23 {
		t.Fatalf("expected overwritten values, got: %v", rb)
	}
	err = reader.Close()
	if err != nil {
		t.Fatal(err)
	}
}
