package data

import (
	"bytes"
	"path/filepath"
	"testing"

	"github.com/webzak/mindstore/internal/testutil"
)

func TestData(t *testing.T) {
	dir := testutil.MakeTempDir(t)
	path := filepath.Join(dir, "data.bin")

	d, err := New(path)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	// Test Append
	data1 := []byte("hello world")
	off1, len1, err := d.Append(data1)
	if err != nil {
		t.Fatalf("Append() error = %v", err)
	}
	if off1 != 0 {
		t.Errorf("expected offset 0, got %d", off1)
	}
	if len1 != len(data1) {
		t.Errorf("expected length %d, got %d", len(data1), len1)
	}

	data2 := []byte("another chunk")
	off2, len2, err := d.Append(data2)
	if err != nil {
		t.Fatalf("Append() error = %v", err)
	}
	if off2 != int64(len(data1)) {
		t.Errorf("expected offset %d, got %d", len(data1), off2)
	}
	if len2 != len(data2) {
		t.Errorf("expected length %d, got %d", len(data2), len2)
	}

	// Test Read
	read1, err := d.Read(off1, len1)
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}
	if !bytes.Equal(read1, data1) {
		t.Errorf("expected %s, got %s", data1, read1)
	}

	read2, err := d.Read(off2, len2)
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}
	if !bytes.Equal(read2, data2) {
		t.Errorf("expected %s, got %s", data2, read2)
	}

	// Test Read out of bounds
	_, err = d.Read(off2+100, 10)
	if err == nil {
		t.Error("expected error for out of bounds read, got nil")
	}
}

func TestDataPersistence(t *testing.T) {
	dir := testutil.MakeTempDir(t)
	path := filepath.Join(dir, "data_persist.bin")

	d1, err := New(path)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	data := []byte("persistent data")
	off, length, err := d1.Append(data)
	if err != nil {
		t.Fatalf("Append() error = %v", err)
	}

	// Re-open
	d2, err := New(path)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	read, err := d2.Read(off, length)
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}
	if !bytes.Equal(read, data) {
		t.Errorf("expected %s, got %s", data, read)
	}
}
