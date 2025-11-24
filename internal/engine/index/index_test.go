package index

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/webzak/mindstore/internal/testutil"
)

// TestNew tests the New function
func TestNew(t *testing.T) {
	t.Run("creates new index with default options", func(t *testing.T) {
		tmpDir := testutil.MakeTempDir(t)
		defer os.RemoveAll(tmpDir)
		path := filepath.Join(tmpDir, "test.idx")

		idx, err := New(path, nil)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		if idx == nil {
			t.Fatal("expected index to be created")
		}

		if idx.maxAppendBufferSize != DefaultMaxAppendBufferSize {
			t.Errorf("expected maxAppendBufferSize to be %d, got %d", DefaultMaxAppendBufferSize, idx.maxAppendBufferSize)
		}

		if idx.Count() != 0 {
			t.Errorf("expected count to be 0, got %d", idx.Count())
		}

		if !idx.IsPersisted() {
			t.Error("expected new index to be persisted")
		}
	})

	t.Run("creates new index with custom options", func(t *testing.T) {
		tmpDir := testutil.MakeTempDir(t)
		defer os.RemoveAll(tmpDir)
		path := filepath.Join(tmpDir, "test.idx")

		customBufferSize := 128
		idx, err := New(path, &IndexOptions{MaxAppendBufferSize: customBufferSize})
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		if idx.maxAppendBufferSize != customBufferSize {
			t.Errorf("expected maxAppendBufferSize to be %d, got %d", customBufferSize, idx.maxAppendBufferSize)
		}
	})

	t.Run("loads existing index from storage", func(t *testing.T) {
		tmpDir := testutil.MakeTempDir(t)
		defer os.RemoveAll(tmpDir)
		path := filepath.Join(tmpDir, "test.idx")

		// Create and populate index
		idx1, err := New(path, nil)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		row1 := Row{Offset: 100, Size: 50, Type: 1, Flags: 0}
		row2 := Row{Offset: 200, Size: 75, Type: 2, Flags: 0}
		if err := idx1.Append(row1); err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if err := idx1.Append(row2); err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if err := idx1.Flush(); err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		// Reopen index
		idx2, err := New(path, nil)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		if idx2.Count() != 2 {
			t.Errorf("expected count to be 2, got %d", idx2.Count())
		}

		got1, err := idx2.Get(0)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if got1 != row1 {
			t.Errorf("expected row %+v, got %+v", row1, got1)
		}

		got2, err := idx2.Get(1)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if got2 != row2 {
			t.Errorf("expected row %+v, got %+v", row2, got2)
		}
	})
}

// TestGet tests the Get method
func TestGet(t *testing.T) {
	t.Run("retrieves row from persisted storage", func(t *testing.T) {
		tmpDir := testutil.MakeTempDir(t)
		defer os.RemoveAll(tmpDir)
		path := filepath.Join(tmpDir, "test.idx")

		idx, err := New(path, nil)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		row := Row{Offset: 100, Size: 50, Type: 1, Flags: 0}
		if err := idx.Append(row); err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if err := idx.Flush(); err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		got, err := idx.Get(0)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if got != row {
			t.Errorf("expected row %+v, got %+v", row, got)
		}
	})

	t.Run("retrieves row from append buffer", func(t *testing.T) {
		tmpDir := testutil.MakeTempDir(t)
		defer os.RemoveAll(tmpDir)
		path := filepath.Join(tmpDir, "test.idx")

		idx, err := New(path, &IndexOptions{MaxAppendBufferSize: 10})
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		row := Row{Offset: 100, Size: 50, Type: 1, Flags: 0}
		if err := idx.Append(row); err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		if idx.IsPersisted() {
			t.Error("expected row to be in append buffer")
		}

		got, err := idx.Get(0)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if got != row {
			t.Errorf("expected row %+v, got %+v", row, got)
		}
	})

	t.Run("returns error for negative index", func(t *testing.T) {
		tmpDir := testutil.MakeTempDir(t)
		defer os.RemoveAll(tmpDir)
		path := filepath.Join(tmpDir, "test.idx")

		idx, err := New(path, nil)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		_, err = idx.Get(-1)
		if err != ErrIndexOutOfRange {
			t.Errorf("expected ErrIndexOutOfRange, got %v", err)
		}
	})

	t.Run("returns error for index out of bounds", func(t *testing.T) {
		tmpDir := testutil.MakeTempDir(t)
		defer os.RemoveAll(tmpDir)
		path := filepath.Join(tmpDir, "test.idx")

		idx, err := New(path, nil)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		row := Row{Offset: 100, Size: 50, Type: 1, Flags: 0}
		if err := idx.Append(row); err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		_, err = idx.Get(1)
		if err != ErrIndexOutOfRange {
			t.Errorf("expected ErrIndexOutOfRange, got %v", err)
		}
	})
}

// TestAppend tests the Append method
func TestAppend(t *testing.T) {
	t.Run("appends row to empty index", func(t *testing.T) {
		tmpDir := testutil.MakeTempDir(t)
		defer os.RemoveAll(tmpDir)
		path := filepath.Join(tmpDir, "test.idx")

		idx, err := New(path, &IndexOptions{MaxAppendBufferSize: 10})
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		row := Row{Offset: 100, Size: 50, Type: 1, Flags: 0}
		if err := idx.Append(row); err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		if idx.Count() != 1 {
			t.Errorf("expected count to be 1, got %d", idx.Count())
		}

		got, err := idx.Get(0)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if got != row {
			t.Errorf("expected row %+v, got %+v", row, got)
		}
	})

	t.Run("auto-flushes when buffer is full", func(t *testing.T) {
		tmpDir := testutil.MakeTempDir(t)
		defer os.RemoveAll(tmpDir)
		path := filepath.Join(tmpDir, "test.idx")

		bufferSize := 3
		idx, err := New(path, &IndexOptions{MaxAppendBufferSize: bufferSize})
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		// Append rows up to buffer size
		for i := 0; i < bufferSize; i++ {
			row := Row{Offset: int64(i * 100), Size: 50, Type: uint8(i), Flags: 0}
			if err := idx.Append(row); err != nil {
				t.Fatalf("expected no error on append %d, got %v", i, err)
			}
		}

		// After reaching buffer size, should be persisted
		if !idx.IsPersisted() {
			t.Error("expected index to be persisted after buffer is full")
		}

		if idx.Count() != bufferSize {
			t.Errorf("expected count to be %d, got %d", bufferSize, idx.Count())
		}
	})

	t.Run("immediate flush when maxAppendBufferSize is 0", func(t *testing.T) {
		tmpDir := testutil.MakeTempDir(t)
		defer os.RemoveAll(tmpDir)
		path := filepath.Join(tmpDir, "test.idx")

		idx, err := New(path, &IndexOptions{MaxAppendBufferSize: 0})
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		row := Row{Offset: 100, Size: 50, Type: 1, Flags: 0}
		if err := idx.Append(row); err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		if !idx.IsPersisted() {
			t.Error("expected immediate flush when maxAppendBufferSize is 0")
		}
	})

	t.Run("persists data after reopening", func(t *testing.T) {
		tmpDir := testutil.MakeTempDir(t)
		defer os.RemoveAll(tmpDir)
		path := filepath.Join(tmpDir, "test.idx")

		idx1, err := New(path, &IndexOptions{MaxAppendBufferSize: 2})
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		rows := []Row{
			{Offset: 100, Size: 50, Type: 1, Flags: 0},
			{Offset: 200, Size: 75, Type: 2, Flags: 0},
		}

		for _, row := range rows {
			if err := idx1.Append(row); err != nil {
				t.Fatalf("expected no error, got %v", err)
			}
		}

		// Reopen index
		idx2, err := New(path, nil)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		if idx2.Count() != len(rows) {
			t.Errorf("expected count to be %d, got %d", len(rows), idx2.Count())
		}

		for i, expected := range rows {
			got, err := idx2.Get(i)
			if err != nil {
				t.Fatalf("expected no error, got %v", err)
			}
			if got != expected {
				t.Errorf("row %d: expected %+v, got %+v", i, expected, got)
			}
		}
	})
}

// TestReplace tests the Replace method
func TestReplace(t *testing.T) {
	t.Run("replaces persisted row", func(t *testing.T) {
		tmpDir := testutil.MakeTempDir(t)
		defer os.RemoveAll(tmpDir)
		path := filepath.Join(tmpDir, "test.idx")

		idx, err := New(path, nil)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		original := Row{Offset: 100, Size: 50, Type: 1, Flags: 0}
		if err := idx.Append(original); err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if err := idx.Flush(); err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		replacement := Row{Offset: 200, Size: 75, Type: 2, Flags: 0}
		if err := idx.Replace(0, replacement); err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		got, err := idx.Get(0)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if got != replacement {
			t.Errorf("expected row %+v, got %+v", replacement, got)
		}
	})

	t.Run("replaces row in append buffer", func(t *testing.T) {
		tmpDir := testutil.MakeTempDir(t)
		defer os.RemoveAll(tmpDir)
		path := filepath.Join(tmpDir, "test.idx")

		idx, err := New(path, &IndexOptions{MaxAppendBufferSize: 10})
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		original := Row{Offset: 100, Size: 50, Type: 1, Flags: 0}
		if err := idx.Append(original); err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		replacement := Row{Offset: 200, Size: 75, Type: 2, Flags: 0}
		if err := idx.Replace(0, replacement); err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		got, err := idx.Get(0)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if got != replacement {
			t.Errorf("expected row %+v, got %+v", replacement, got)
		}

		// Should trigger flush since row is in append buffer
		if !idx.IsPersisted() {
			t.Error("expected replace to flush append buffer")
		}
	})

	t.Run("persists replacement after reopening", func(t *testing.T) {
		tmpDir := testutil.MakeTempDir(t)
		defer os.RemoveAll(tmpDir)
		path := filepath.Join(tmpDir, "test.idx")

		idx1, err := New(path, nil)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		original := Row{Offset: 100, Size: 50, Type: 1, Flags: 0}
		if err := idx1.Append(original); err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if err := idx1.Flush(); err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		replacement := Row{Offset: 200, Size: 75, Type: 2, Flags: 0}
		if err := idx1.Replace(0, replacement); err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		// Reopen index
		idx2, err := New(path, nil)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		got, err := idx2.Get(0)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if got != replacement {
			t.Errorf("expected row %+v, got %+v", replacement, got)
		}
	})

	t.Run("returns error for negative index", func(t *testing.T) {
		tmpDir := testutil.MakeTempDir(t)
		defer os.RemoveAll(tmpDir)
		path := filepath.Join(tmpDir, "test.idx")

		idx, err := New(path, nil)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		row := Row{Offset: 100, Size: 50, Type: 1, Flags: 0}
		err = idx.Replace(-1, row)
		if err != ErrIndexOutOfRange {
			t.Errorf("expected ErrIndexOutOfRange, got %v", err)
		}
	})

	t.Run("returns error for index out of bounds", func(t *testing.T) {
		tmpDir := testutil.MakeTempDir(t)
		defer os.RemoveAll(tmpDir)
		path := filepath.Join(tmpDir, "test.idx")

		idx, err := New(path, nil)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		row := Row{Offset: 100, Size: 50, Type: 1, Flags: 0}
		err = idx.Replace(0, row)
		if err != ErrIndexOutOfRange {
			t.Errorf("expected ErrIndexOutOfRange, got %v", err)
		}
	})
}

// TestSetFlags tests the SetFlags method
func TestSetFlags(t *testing.T) {
	t.Run("sets flags on persisted row", func(t *testing.T) {
		tmpDir := testutil.MakeTempDir(t)
		defer os.RemoveAll(tmpDir)
		path := filepath.Join(tmpDir, "test.idx")

		idx, err := New(path, nil)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		row := Row{Offset: 100, Size: 50, Type: 1, Flags: 0}
		if err := idx.Append(row); err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if err := idx.Flush(); err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		if err := idx.SetFlags(0, MarkedForRemoval); err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		got, err := idx.Get(0)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if got.Flags&MarkedForRemoval == 0 {
			t.Error("expected MarkedForRemoval flag to be set")
		}
	})

	t.Run("sets multiple flags using OR operation", func(t *testing.T) {
		tmpDir := testutil.MakeTempDir(t)
		defer os.RemoveAll(tmpDir)
		path := filepath.Join(tmpDir, "test.idx")

		idx, err := New(path, nil)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		row := Row{Offset: 100, Size: 50, Type: 1, Flags: 0b00000001}
		if err := idx.Append(row); err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if err := idx.Flush(); err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		// Set additional flags
		if err := idx.SetFlags(0, 0b00000010); err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		got, err := idx.Get(0)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		expected := uint8(0b00000011)
		if got.Flags != expected {
			t.Errorf("expected flags %08b, got %08b", expected, got.Flags)
		}
	})

	t.Run("persists flags after reopening", func(t *testing.T) {
		tmpDir := testutil.MakeTempDir(t)
		defer os.RemoveAll(tmpDir)
		path := filepath.Join(tmpDir, "test.idx")

		idx1, err := New(path, nil)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		row := Row{Offset: 100, Size: 50, Type: 1, Flags: 0}
		if err := idx1.Append(row); err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if err := idx1.Flush(); err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		if err := idx1.SetFlags(0, MarkedForRemoval); err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		// Reopen index
		idx2, err := New(path, nil)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		got, err := idx2.Get(0)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if got.Flags&MarkedForRemoval == 0 {
			t.Error("expected MarkedForRemoval flag to be persisted")
		}
	})

	t.Run("returns error for index out of bounds", func(t *testing.T) {
		tmpDir := testutil.MakeTempDir(t)
		defer os.RemoveAll(tmpDir)
		path := filepath.Join(tmpDir, "test.idx")

		idx, err := New(path, nil)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		err = idx.SetFlags(0, MarkedForRemoval)
		if err != ErrIndexOutOfRange {
			t.Errorf("expected ErrIndexOutOfRange, got %v", err)
		}
	})
}

// TestResetFlags tests the ResetFlags method
func TestResetFlags(t *testing.T) {
	t.Run("resets flags on persisted row", func(t *testing.T) {
		tmpDir := testutil.MakeTempDir(t)
		defer os.RemoveAll(tmpDir)
		path := filepath.Join(tmpDir, "test.idx")

		idx, err := New(path, nil)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		row := Row{Offset: 100, Size: 50, Type: 1, Flags: MarkedForRemoval}
		if err := idx.Append(row); err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if err := idx.Flush(); err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		if err := idx.ResetFlags(0, MarkedForRemoval); err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		got, err := idx.Get(0)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if got.Flags&MarkedForRemoval != 0 {
			t.Error("expected MarkedForRemoval flag to be cleared")
		}
	})

	t.Run("resets specific flags using AND NOT operation", func(t *testing.T) {
		tmpDir := testutil.MakeTempDir(t)
		defer os.RemoveAll(tmpDir)
		path := filepath.Join(tmpDir, "test.idx")

		idx, err := New(path, nil)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		row := Row{Offset: 100, Size: 50, Type: 1, Flags: 0b00000111}
		if err := idx.Append(row); err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if err := idx.Flush(); err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		// Reset specific flags
		if err := idx.ResetFlags(0, 0b00000101); err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		got, err := idx.Get(0)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		expected := uint8(0b00000010)
		if got.Flags != expected {
			t.Errorf("expected flags %08b, got %08b", expected, got.Flags)
		}
	})

	t.Run("persists flag reset after reopening", func(t *testing.T) {
		tmpDir := testutil.MakeTempDir(t)
		defer os.RemoveAll(tmpDir)
		path := filepath.Join(tmpDir, "test.idx")

		idx1, err := New(path, nil)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		row := Row{Offset: 100, Size: 50, Type: 1, Flags: MarkedForRemoval}
		if err := idx1.Append(row); err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if err := idx1.Flush(); err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		if err := idx1.ResetFlags(0, MarkedForRemoval); err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		// Reopen index
		idx2, err := New(path, nil)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		got, err := idx2.Get(0)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if got.Flags&MarkedForRemoval != 0 {
			t.Error("expected MarkedForRemoval flag to remain cleared")
		}
	})

	t.Run("returns error for index out of bounds", func(t *testing.T) {
		tmpDir := testutil.MakeTempDir(t)
		defer os.RemoveAll(tmpDir)
		path := filepath.Join(tmpDir, "test.idx")

		idx, err := New(path, nil)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		err = idx.ResetFlags(0, MarkedForRemoval)
		if err != ErrIndexOutOfRange {
			t.Errorf("expected ErrIndexOutOfRange, got %v", err)
		}
	})
}

// TestDestroy tests the Destroy method
func TestDestroy(t *testing.T) {
	t.Run("destroys index and clears all data", func(t *testing.T) {
		tmpDir := testutil.MakeTempDir(t)
		defer os.RemoveAll(tmpDir)
		path := filepath.Join(tmpDir, "test.idx")

		idx, err := New(path, nil)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		// Add some rows
		for i := 0; i < 5; i++ {
			row := Row{Offset: int64(i * 100), Size: 50, Type: uint8(i), Flags: 0}
			if err := idx.Append(row); err != nil {
				t.Fatalf("expected no error, got %v", err)
			}
		}
		if err := idx.Flush(); err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		if err := idx.Destroy(); err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		if idx.Count() != 0 {
			t.Errorf("expected count to be 0 after destroy, got %d", idx.Count())
		}

		if !idx.IsPersisted() {
			t.Error("expected index to be persisted after destroy")
		}
	})

	t.Run("storage file is empty after destroy", func(t *testing.T) {
		tmpDir := testutil.MakeTempDir(t)
		defer os.RemoveAll(tmpDir)
		path := filepath.Join(tmpDir, "test.idx")

		idx1, err := New(path, nil)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		row := Row{Offset: 100, Size: 50, Type: 1, Flags: 0}
		if err := idx1.Append(row); err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if err := idx1.Flush(); err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		if err := idx1.Destroy(); err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		// Check file size
		info, err := os.Stat(path)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if info.Size() != 0 {
			t.Errorf("expected file size to be 0, got %d", info.Size())
		}
	})

	t.Run("reopening after destroy creates empty index", func(t *testing.T) {
		tmpDir := testutil.MakeTempDir(t)
		defer os.RemoveAll(tmpDir)
		path := filepath.Join(tmpDir, "test.idx")

		idx1, err := New(path, nil)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		row := Row{Offset: 100, Size: 50, Type: 1, Flags: 0}
		if err := idx1.Append(row); err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if err := idx1.Flush(); err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		if err := idx1.Destroy(); err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		// Reopen index
		idx2, err := New(path, nil)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		if idx2.Count() != 0 {
			t.Errorf("expected count to be 0 after reopening destroyed index, got %d", idx2.Count())
		}
	})
}

// TestOptimise tests the Optimise method
func TestOptimise(t *testing.T) {
	t.Run("removes rows marked for removal", func(t *testing.T) {
		tmpDir := testutil.MakeTempDir(t)
		defer os.RemoveAll(tmpDir)
		path := filepath.Join(tmpDir, "test.idx")

		idx, err := New(path, nil)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		// Add rows with some marked for removal
		rows := []Row{
			{Offset: 100, Size: 50, Type: 1, Flags: 0},
			{Offset: 200, Size: 75, Type: 2, Flags: MarkedForRemoval},
			{Offset: 300, Size: 60, Type: 3, Flags: 0},
			{Offset: 400, Size: 80, Type: 4, Flags: MarkedForRemoval},
			{Offset: 500, Size: 90, Type: 5, Flags: 0},
		}

		for _, row := range rows {
			if err := idx.Append(row); err != nil {
				t.Fatalf("expected no error, got %v", err)
			}
		}
		if err := idx.Flush(); err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		if err := idx.Optimise(); err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		// Should have 3 rows remaining (indices 0, 2, 4)
		if idx.Count() != 3 {
			t.Errorf("expected count to be 3, got %d", idx.Count())
		}

		// Verify remaining rows
		expectedRows := []Row{rows[0], rows[2], rows[4]}
		for i, expected := range expectedRows {
			got, err := idx.Get(i)
			if err != nil {
				t.Fatalf("expected no error, got %v", err)
			}
			if got != expected {
				t.Errorf("row %d: expected %+v, got %+v", i, expected, got)
			}
		}
	})

	t.Run("optimise persists changes", func(t *testing.T) {
		tmpDir := testutil.MakeTempDir(t)
		defer os.RemoveAll(tmpDir)
		path := filepath.Join(tmpDir, "test.idx")

		idx1, err := New(path, nil)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		rows := []Row{
			{Offset: 100, Size: 50, Type: 1, Flags: 0},
			{Offset: 200, Size: 75, Type: 2, Flags: MarkedForRemoval},
			{Offset: 300, Size: 60, Type: 3, Flags: 0},
		}

		for _, row := range rows {
			if err := idx1.Append(row); err != nil {
				t.Fatalf("expected no error, got %v", err)
			}
		}
		if err := idx1.Flush(); err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		if err := idx1.Optimise(); err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		// Reopen index
		idx2, err := New(path, nil)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		if idx2.Count() != 2 {
			t.Errorf("expected count to be 2 after reopening, got %d", idx2.Count())
		}

		expectedRows := []Row{rows[0], rows[2]}
		for i, expected := range expectedRows {
			got, err := idx2.Get(i)
			if err != nil {
				t.Fatalf("expected no error, got %v", err)
			}
			if got != expected {
				t.Errorf("row %d: expected %+v, got %+v", i, expected, got)
			}
		}
	})

	t.Run("optimise with no marked rows keeps all rows", func(t *testing.T) {
		tmpDir := testutil.MakeTempDir(t)
		defer os.RemoveAll(tmpDir)
		path := filepath.Join(tmpDir, "test.idx")

		idx, err := New(path, nil)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		rows := []Row{
			{Offset: 100, Size: 50, Type: 1, Flags: 0},
			{Offset: 200, Size: 75, Type: 2, Flags: 0},
			{Offset: 300, Size: 60, Type: 3, Flags: 0},
		}

		for _, row := range rows {
			if err := idx.Append(row); err != nil {
				t.Fatalf("expected no error, got %v", err)
			}
		}
		if err := idx.Flush(); err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		if err := idx.Optimise(); err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		if idx.Count() != 3 {
			t.Errorf("expected count to be 3, got %d", idx.Count())
		}
	})

	t.Run("optimise with all rows marked removes all", func(t *testing.T) {
		tmpDir := testutil.MakeTempDir(t)
		defer os.RemoveAll(tmpDir)
		path := filepath.Join(tmpDir, "test.idx")

		idx, err := New(path, nil)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		rows := []Row{
			{Offset: 100, Size: 50, Type: 1, Flags: MarkedForRemoval},
			{Offset: 200, Size: 75, Type: 2, Flags: MarkedForRemoval},
		}

		for _, row := range rows {
			if err := idx.Append(row); err != nil {
				t.Fatalf("expected no error, got %v", err)
			}
		}
		if err := idx.Flush(); err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		if err := idx.Optimise(); err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		if idx.Count() != 0 {
			t.Errorf("expected count to be 0, got %d", idx.Count())
		}
	})
}

// TestCount tests the Count method
func TestCount(t *testing.T) {
	t.Run("returns 0 for empty index", func(t *testing.T) {
		tmpDir := testutil.MakeTempDir(t)
		defer os.RemoveAll(tmpDir)
		path := filepath.Join(tmpDir, "test.idx")

		idx, err := New(path, nil)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		if idx.Count() != 0 {
			t.Errorf("expected count to be 0, got %d", idx.Count())
		}
	})

	t.Run("returns correct count after appends", func(t *testing.T) {
		tmpDir := testutil.MakeTempDir(t)
		defer os.RemoveAll(tmpDir)
		path := filepath.Join(tmpDir, "test.idx")

		idx, err := New(path, &IndexOptions{MaxAppendBufferSize: 10})
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		for i := 0; i < 5; i++ {
			row := Row{Offset: int64(i * 100), Size: 50, Type: uint8(i), Flags: 0}
			if err := idx.Append(row); err != nil {
				t.Fatalf("expected no error, got %v", err)
			}

			if idx.Count() != i+1 {
				t.Errorf("expected count to be %d, got %d", i+1, idx.Count())
			}
		}
	})

	t.Run("count remains correct after flush", func(t *testing.T) {
		tmpDir := testutil.MakeTempDir(t)
		defer os.RemoveAll(tmpDir)
		path := filepath.Join(tmpDir, "test.idx")

		idx, err := New(path, &IndexOptions{MaxAppendBufferSize: 10})
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		for i := 0; i < 5; i++ {
			row := Row{Offset: int64(i * 100), Size: 50, Type: uint8(i), Flags: 0}
			if err := idx.Append(row); err != nil {
				t.Fatalf("expected no error, got %v", err)
			}
		}

		if err := idx.Flush(); err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		if idx.Count() != 5 {
			t.Errorf("expected count to be 5, got %d", idx.Count())
		}
	})
}

// TestIsPersisted tests the IsPersisted method
func TestIsPersisted(t *testing.T) {
	t.Run("returns true for empty index", func(t *testing.T) {
		tmpDir := testutil.MakeTempDir(t)
		defer os.RemoveAll(tmpDir)
		path := filepath.Join(tmpDir, "test.idx")

		idx, err := New(path, nil)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		if !idx.IsPersisted() {
			t.Error("expected empty index to be persisted")
		}
	})

	t.Run("returns false after append to buffer", func(t *testing.T) {
		tmpDir := testutil.MakeTempDir(t)
		defer os.RemoveAll(tmpDir)
		path := filepath.Join(tmpDir, "test.idx")

		idx, err := New(path, &IndexOptions{MaxAppendBufferSize: 10})
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		row := Row{Offset: 100, Size: 50, Type: 1, Flags: 0}
		if err := idx.Append(row); err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		if idx.IsPersisted() {
			t.Error("expected index to not be persisted after append")
		}
	})

	t.Run("returns true after flush", func(t *testing.T) {
		tmpDir := testutil.MakeTempDir(t)
		defer os.RemoveAll(tmpDir)
		path := filepath.Join(tmpDir, "test.idx")

		idx, err := New(path, &IndexOptions{MaxAppendBufferSize: 10})
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		row := Row{Offset: 100, Size: 50, Type: 1, Flags: 0}
		if err := idx.Append(row); err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		if err := idx.Flush(); err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		if !idx.IsPersisted() {
			t.Error("expected index to be persisted after flush")
		}
	})

	t.Run("returns true after auto-flush", func(t *testing.T) {
		tmpDir := testutil.MakeTempDir(t)
		defer os.RemoveAll(tmpDir)
		path := filepath.Join(tmpDir, "test.idx")

		bufferSize := 2
		idx, err := New(path, &IndexOptions{MaxAppendBufferSize: bufferSize})
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		for i := 0; i < bufferSize; i++ {
			row := Row{Offset: int64(i * 100), Size: 50, Type: uint8(i), Flags: 0}
			if err := idx.Append(row); err != nil {
				t.Fatalf("expected no error, got %v", err)
			}
		}

		if !idx.IsPersisted() {
			t.Error("expected index to be persisted after auto-flush")
		}
	})
}

// TestFlush tests the Flush method
func TestFlush(t *testing.T) {
	t.Run("flush does nothing when already persisted", func(t *testing.T) {
		tmpDir := testutil.MakeTempDir(t)
		defer os.RemoveAll(tmpDir)
		path := filepath.Join(tmpDir, "test.idx")

		idx, err := New(path, nil)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		row := Row{Offset: 100, Size: 50, Type: 1, Flags: 0}
		if err := idx.Append(row); err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if err := idx.Flush(); err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		// Second flush should be no-op
		if err := idx.Flush(); err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		if !idx.IsPersisted() {
			t.Error("expected index to remain persisted")
		}
	})

	t.Run("flush writes pending rows to storage", func(t *testing.T) {
		tmpDir := testutil.MakeTempDir(t)
		defer os.RemoveAll(tmpDir)
		path := filepath.Join(tmpDir, "test.idx")

		idx, err := New(path, &IndexOptions{MaxAppendBufferSize: 10})
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		rows := []Row{
			{Offset: 100, Size: 50, Type: 1, Flags: 0},
			{Offset: 200, Size: 75, Type: 2, Flags: 0},
		}

		for _, row := range rows {
			if err := idx.Append(row); err != nil {
				t.Fatalf("expected no error, got %v", err)
			}
		}

		if idx.IsPersisted() {
			t.Error("expected index to not be persisted before flush")
		}

		if err := idx.Flush(); err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		if !idx.IsPersisted() {
			t.Error("expected index to be persisted after flush")
		}

		// Verify file size
		info, err := os.Stat(path)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		expectedSize := int64(len(rows) * rowSize)
		if info.Size() != expectedSize {
			t.Errorf("expected file size to be %d, got %d", expectedSize, info.Size())
		}
	})

	t.Run("incremental flush appends only new rows", func(t *testing.T) {
		tmpDir := testutil.MakeTempDir(t)
		defer os.RemoveAll(tmpDir)
		path := filepath.Join(tmpDir, "test.idx")

		idx, err := New(path, &IndexOptions{MaxAppendBufferSize: 10})
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		// First batch
		row1 := Row{Offset: 100, Size: 50, Type: 1, Flags: 0}
		if err := idx.Append(row1); err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if err := idx.Flush(); err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		// Second batch
		row2 := Row{Offset: 200, Size: 75, Type: 2, Flags: 0}
		if err := idx.Append(row2); err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if err := idx.Flush(); err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		// Verify file size
		info, err := os.Stat(path)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		expectedSize := int64(2 * rowSize)
		if info.Size() != expectedSize {
			t.Errorf("expected file size to be %d, got %d", expectedSize, info.Size())
		}

		// Verify both rows are accessible
		if idx.Count() != 2 {
			t.Errorf("expected count to be 2, got %d", idx.Count())
		}
	})
}
