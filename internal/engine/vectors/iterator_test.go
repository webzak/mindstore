package vectors

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/webzak/mindstore/internal/testutil"
)

func TestIterator(t *testing.T) {
	dir := testutil.MakeTempDir(t)
	defer os.RemoveAll(dir)

	path := filepath.Join(dir, "test.vec")
	opt := &VectorsOptions{
		VectorSize:          4,
		MaxBufferSize:       2,
		MaxAppendBufferSize: 2,
	}
	v, err := New(path, opt)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	// Add some vectors
	vectors := [][]float32{
		{1.0, 2.0, 3.0, 4.0},
		{5.0, 6.0, 7.0, 8.0},
		{9.0, 10.0, 11.0, 12.0},
	}

	for i, vec := range vectors {
		if err := v.Append(i, vec); err != nil {
			t.Fatalf("Append() error = %v", err)
		}
	}

	// Flush to ensure some are persisted
	v.Flush()

	// Add one more to append buffer
	if err := v.Append(3, []float32{13.0, 14.0, 15.0, 16.0}); err != nil {
		t.Fatalf("Append() error = %v", err)
	}

	// Test iterator
	count := 0
	for index, vector := range v.Iterator() {
		if index != count {
			t.Errorf("Iterator index = %d, want %d", index, count)
		}
		if len(vector) != 4 {
			t.Errorf("Iterator vector length = %d, want 4", len(vector))
		}
		// Verify it's a copy by modifying it
		originalValue := vector[0]
		vector[0] = 999.0
		// Get the vector again and verify it wasn't modified
		retrieved, _ := v.Get(index)
		if retrieved[0] != originalValue {
			t.Errorf("Iterator did not return a copy, mutation affected storage")
		}
		count++
	}

	if count != 4 {
		t.Errorf("Iterator counted %d vectors, want 4", count)
	}
}

func TestIteratorEmpty(t *testing.T) {
	dir := testutil.MakeTempDir(t)
	defer os.RemoveAll(dir)

	path := filepath.Join(dir, "test.vec")
	v, err := New(path, nil)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	count := 0
	for range v.Iterator() {
		count++
	}
	if count != 0 {
		t.Errorf("Iterator on empty vectors iterated %d times, want 0", count)
	}
}

func TestIteratorEarlyTermination(t *testing.T) {
	dir := testutil.MakeTempDir(t)
	defer os.RemoveAll(dir)

	path := filepath.Join(dir, "test.vec")
	opt := &VectorsOptions{
		VectorSize:          4,
		MaxBufferSize:       2,
		MaxAppendBufferSize: 2,
	}
	v, err := New(path, opt)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	// Add multiple vectors
	for i := 0; i < 5; i++ {
		vec := []float32{float32(i), float32(i + 1), float32(i + 2), float32(i + 3)}
		if err := v.Append(i, vec); err != nil {
			t.Fatalf("Append() error = %v", err)
		}
	}

	// Test early termination with break
	count := 0
	for index := range v.Iterator() {
		count++
		if index == 2 {
			break
		}
	}

	if count != 3 {
		t.Errorf("Iterator with early termination counted %d iterations, want 3", count)
	}
}
