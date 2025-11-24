package index

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/webzak/mindstore/internal/testutil"
)

func TestIndex_BasicOperations(t *testing.T) {
	tmpDir := testutil.MakeTempDir(t)
	defer os.RemoveAll(tmpDir)

	indexPath := filepath.Join(tmpDir, "test.idx")
	idx, err := New(indexPath)
	if err != nil {
		t.Fatalf("NewIndex failed: %v", err)
	}

	// Test Add
	if err := idx.Add(100, 50, 1); err != nil {
		t.Fatalf("Add failed: %v", err)
	}

	if idx.Count() != 1 {
		t.Errorf("Expected count 1, got %d", idx.Count())
	}

	// Test Get
	rec, err := idx.Get(0)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if rec.Offset != 100 {
		t.Errorf("Expected offset 100, got %d", rec.Offset)
	}
	if rec.Size != 50 {
		t.Errorf("Expected size 50, got %d", rec.Size)
	}
	if rec.DataType != 1 {
		t.Errorf("Expected dataType 1, got %d", rec.DataType)
	}

	// Test Replace
	if err := idx.Replace(0, 200, 60, 2); err != nil {
		t.Fatalf("Replace failed: %v", err)
	}
	rec, err = idx.Get(0)
	if err != nil {
		t.Fatalf("Get after replace failed: %v", err)
	}
	if rec.Offset != 200 {
		t.Errorf("Expected offset 200, got %d", rec.Offset)
	}
	if rec.Size != 60 {
		t.Errorf("Expected size 60, got %d", rec.Size)
	}
	if rec.DataType != 2 {
		t.Errorf("Expected dataType 2, got %d", rec.DataType)
	}

	// Test Remove
	if err := idx.Remove(0); err != nil {
		t.Fatalf("Remove failed: %v", err)
	}
	if idx.Count() != 0 {
		t.Errorf("Expected count 0 after remove, got %d", idx.Count())
	}
}

func TestIndex_SaveLoad(t *testing.T) {
	tmpDir := testutil.MakeTempDir(t)
	defer os.RemoveAll(tmpDir)

	indexPath := filepath.Join(tmpDir, "test.idx")
	idx, err := New(indexPath)
	if err != nil {
		t.Fatalf("NewIndex failed: %v", err)
	}

	// Add some data
	idx.Add(10, 20, 1)
	idx.Add(30, 40, 2)

	// Flush
	if _, err := idx.Flush(); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	// Create new index instance and load
	idx2, err := New(indexPath)
	if err != nil {
		t.Fatalf("NewIndex (2) failed: %v", err)
	}
	n, err := idx2.Load()
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	if n != 2 {
		t.Errorf("Expected loaded count 2, got %d", n)
	}
	if idx2.Count() != 2 {
		t.Errorf("Expected count 2, got %d", idx2.Count())
	}

	rec1, _ := idx2.Get(0)
	if rec1.Offset != 10 || rec1.Size != 20 || rec1.DataType != 1 {
		t.Errorf("Record 1 mismatch: %+v", rec1)
	}
	rec2, _ := idx2.Get(1)
	if rec2.Offset != 30 || rec2.Size != 40 || rec2.DataType != 2 {
		t.Errorf("Record 2 mismatch: %+v", rec2)
	}
}

func TestIndex_Errors(t *testing.T) {
	tmpDir := testutil.MakeTempDir(t)
	defer os.RemoveAll(tmpDir)

	idx, err := New(filepath.Join(tmpDir, "test.idx"))
	if err != nil {
		t.Fatalf("NewIndex failed: %v", err)
	}

	// Test Get out of range
	if _, err := idx.Get(0); err != ErrIndexOutOfRange {
		t.Errorf("Expected ErrIndexOutOfRange for Get(0) on empty index, got %v", err)
	}
	if _, err := idx.Get(-1); err != ErrIndexOutOfRange {
		t.Errorf("Expected ErrIndexOutOfRange for Get(-1), got %v", err)
	}

	// Test Remove out of range
	if err := idx.Remove(0); err != ErrIndexOutOfRange {
		t.Errorf("Expected ErrIndexOutOfRange for Remove(0) on empty index, got %v", err)
	}

	// Test Replace out of range
	if err := idx.Replace(0, 1, 1, 0); err != ErrIndexOutOfRange {
		t.Errorf("Expected ErrIndexOutOfRange for Replace(0) on empty index, got %v", err)
	}
}

func TestIndex_Clear(t *testing.T) {
	tmpDir := testutil.MakeTempDir(t)
	defer os.RemoveAll(tmpDir)

	idx, err := New(filepath.Join(tmpDir, "test.idx"))
	if err != nil {
		t.Fatalf("NewIndex failed: %v", err)
	}

	idx.Add(1, 1, 0)
	idx.Add(2, 2, 0)

	idx.Clear()
	if idx.Count() != 0 {
		t.Errorf("Expected count 0 after Clear, got %d", idx.Count())
	}
}
