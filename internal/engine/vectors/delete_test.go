package vectors

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/webzak/mindstore/internal/testutil"
)

func TestDelete(t *testing.T) {
	tmpDir := testutil.MakeTempDir(t)
	defer os.RemoveAll(tmpDir)

	t.Run("delete with empty indexes slice", func(t *testing.T) {
		path := filepath.Join(tmpDir, "vectors_delete_empty.bin")
		v, err := New(path, nil)
		if err != nil {
			t.Fatalf("failed to create vectors: %v", err)
		}

		// Append and flush a vector
		vector := make([]float32, DefaultVectorSize)
		for i := range vector {
			vector[i] = float32(i)
		}
		_, err = v.Append(vector)
		if err != nil {
			t.Fatalf("failed to append vector: %v", err)
		}
		err = v.Flush()
		if err != nil {
			t.Fatalf("failed to flush: %v", err)
		}

		// Delete with empty slice should be no-op
		err = v.Delete([]int{})
		if err != nil {
			t.Errorf("expected no error for empty indexes, got: %v", err)
		}

		// Verify count unchanged
		if v.Count() != 1 {
			t.Errorf("expected count 1, got %d", v.Count())
		}
	})

	t.Run("delete with negative index", func(t *testing.T) {
		path := filepath.Join(tmpDir, "vectors_delete_negative.bin")
		v, err := New(path, nil)
		if err != nil {
			t.Fatalf("failed to create vectors: %v", err)
		}

		// Append and flush a vector
		vector := make([]float32, DefaultVectorSize)
		_, err = v.Append(vector)
		if err != nil {
			t.Fatalf("failed to append vector: %v", err)
		}
		err = v.Flush()
		if err != nil {
			t.Fatalf("failed to flush: %v", err)
		}

		// Try to delete with negative index
		err = v.Delete([]int{-1})
		if err == nil {
			t.Error("expected error for negative index, got nil")
		}
	})

	t.Run("delete with index out of bounds", func(t *testing.T) {
		path := filepath.Join(tmpDir, "vectors_delete_out_of_bounds.bin")
		v, err := New(path, nil)
		if err != nil {
			t.Fatalf("failed to create vectors: %v", err)
		}

		// Append and flush 3 vectors
		for i := 0; i < 3; i++ {
			vector := make([]float32, DefaultVectorSize)
			for j := range vector {
				vector[j] = float32(i*100 + j)
			}
			_, err = v.Append(vector)
			if err != nil {
				t.Fatalf("failed to append vector %d: %v", i, err)
			}
		}
		err = v.Flush()
		if err != nil {
			t.Fatalf("failed to flush: %v", err)
		}

		// Try to delete index 5 (only 0-2 exist)
		err = v.Delete([]int{5})
		if err == nil {
			t.Error("expected error for index out of bounds, got nil")
		}

		// Try to delete index equal to count
		err = v.Delete([]int{3})
		if err == nil {
			t.Error("expected error for index equal to count, got nil")
		}
	})

	t.Run("delete single vector", func(t *testing.T) {
		path := filepath.Join(tmpDir, "vectors_delete_single.bin")
		v, err := New(path, nil)
		if err != nil {
			t.Fatalf("failed to create vectors: %v", err)
		}

		// Append and flush 5 vectors
		vectors := make([][]float32, 5)
		for i := 0; i < 5; i++ {
			vectors[i] = make([]float32, DefaultVectorSize)
			for j := range vectors[i] {
				vectors[i][j] = float32(i*100 + j)
			}
			_, err = v.Append(vectors[i])
			if err != nil {
				t.Fatalf("failed to append vector %d: %v", i, err)
			}
		}
		err = v.Flush()
		if err != nil {
			t.Fatalf("failed to flush: %v", err)
		}

		// Delete vector at index 2
		err = v.Delete([]int{2})
		if err != nil {
			t.Fatalf("failed to delete vector: %v", err)
		}

		// Verify count
		if v.Count() != 4 {
			t.Errorf("expected count 4, got %d", v.Count())
		}

		// Verify remaining vectors
		// Original: [0, 1, 2, 3, 4]
		// After deleting index 2: [0, 1, 3, 4]
		expectedIndexes := []int{0, 1, 3, 4}
		for newIdx, oldIdx := range expectedIndexes {
			retrieved, err := v.Get(newIdx)
			if err != nil {
				t.Fatalf("failed to get vector at new index %d: %v", newIdx, err)
			}
			for j := range vectors[oldIdx] {
				if retrieved[j] != vectors[oldIdx][j] {
					t.Errorf("new index %d (old index %d), element %d: expected %f, got %f",
						newIdx, oldIdx, j, vectors[oldIdx][j], retrieved[j])
				}
			}
		}
	})

	t.Run("delete multiple vectors", func(t *testing.T) {
		path := filepath.Join(tmpDir, "vectors_delete_multiple.bin")
		v, err := New(path, nil)
		if err != nil {
			t.Fatalf("failed to create vectors: %v", err)
		}

		// Append and flush 10 vectors
		vectors := make([][]float32, 10)
		for i := 0; i < 10; i++ {
			vectors[i] = make([]float32, DefaultVectorSize)
			for j := range vectors[i] {
				vectors[i][j] = float32(i*100 + j)
			}
			_, err = v.Append(vectors[i])
			if err != nil {
				t.Fatalf("failed to append vector %d: %v", i, err)
			}
		}
		err = v.Flush()
		if err != nil {
			t.Fatalf("failed to flush: %v", err)
		}

		// Delete vectors at indexes 1, 3, 5, 7
		err = v.Delete([]int{1, 3, 5, 7})
		if err != nil {
			t.Fatalf("failed to delete vectors: %v", err)
		}

		// Verify count
		if v.Count() != 6 {
			t.Errorf("expected count 6, got %d", v.Count())
		}

		// Verify remaining vectors
		// Original: [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
		// After deleting 1, 3, 5, 7: [0, 2, 4, 6, 8, 9]
		expectedIndexes := []int{0, 2, 4, 6, 8, 9}
		for newIdx, oldIdx := range expectedIndexes {
			retrieved, err := v.Get(newIdx)
			if err != nil {
				t.Fatalf("failed to get vector at new index %d: %v", newIdx, err)
			}
			for j := range vectors[oldIdx] {
				if retrieved[j] != vectors[oldIdx][j] {
					t.Errorf("new index %d (old index %d), element %d: expected %f, got %f",
						newIdx, oldIdx, j, vectors[oldIdx][j], retrieved[j])
				}
			}
		}
	})

	t.Run("delete all vectors", func(t *testing.T) {
		path := filepath.Join(tmpDir, "vectors_delete_all.bin")
		v, err := New(path, nil)
		if err != nil {
			t.Fatalf("failed to create vectors: %v", err)
		}

		// Append and flush 3 vectors
		for i := 0; i < 3; i++ {
			vector := make([]float32, DefaultVectorSize)
			for j := range vector {
				vector[j] = float32(i*100 + j)
			}
			_, err = v.Append(vector)
			if err != nil {
				t.Fatalf("failed to append vector %d: %v", i, err)
			}
		}
		err = v.Flush()
		if err != nil {
			t.Fatalf("failed to flush: %v", err)
		}

		// Delete all vectors
		err = v.Delete([]int{0, 1, 2})
		if err != nil {
			t.Fatalf("failed to delete all vectors: %v", err)
		}

		// Verify count is 0
		if v.Count() != 0 {
			t.Errorf("expected count 0, got %d", v.Count())
		}

		// Verify persistedSize is 0
		if v.persistedSize != 0 {
			t.Errorf("expected persistedSize 0, got %d", v.persistedSize)
		}

		// Try to get any vector should fail
		_, err = v.Get(0)
		if err == nil {
			t.Error("expected error when getting from empty storage, got nil")
		}
	})

	t.Run("delete with append buffer triggers flush", func(t *testing.T) {
		path := filepath.Join(tmpDir, "vectors_delete_with_append.bin")
		v, err := New(path, nil)
		if err != nil {
			t.Fatalf("failed to create vectors: %v", err)
		}

		// Append and flush 3 vectors
		vectors := make([][]float32, 3)
		for i := 0; i < 3; i++ {
			vectors[i] = make([]float32, DefaultVectorSize)
			for j := range vectors[i] {
				vectors[i][j] = float32(i*100 + j)
			}
			_, err = v.Append(vectors[i])
			if err != nil {
				t.Fatalf("failed to append vector %d: %v", i, err)
			}
		}
		err = v.Flush()
		if err != nil {
			t.Fatalf("failed to flush: %v", err)
		}

		// Append 2 more vectors (not flushed)
		for i := 3; i < 5; i++ {
			vector := make([]float32, DefaultVectorSize)
			for j := range vector {
				vector[j] = float32(i*100 + j)
			}
			_, err = v.Append(vector)
			if err != nil {
				t.Fatalf("failed to append vector %d: %v", i, err)
			}
		}

		// Verify append buffer has 2 vectors
		if len(v.appendBuffer) != 2 {
			t.Errorf("expected appendBuffer length 2, got %d", len(v.appendBuffer))
		}

		// Delete vector at index 1
		err = v.Delete([]int{1})
		if err != nil {
			t.Fatalf("failed to delete vector: %v", err)
		}

		// Verify append buffer was flushed
		if len(v.appendBuffer) != 0 {
			t.Errorf("expected appendBuffer to be empty after delete, got length %d", len(v.appendBuffer))
		}

		// Verify count (5 total - 1 deleted = 4)
		if v.Count() != 4 {
			t.Errorf("expected count 4, got %d", v.Count())
		}

		// Verify persistedSize
		if v.persistedSize != 4 {
			t.Errorf("expected persistedSize 4, got %d", v.persistedSize)
		}
	})

	t.Run("delete clears memory buffer", func(t *testing.T) {
		path := filepath.Join(tmpDir, "vectors_delete_clears_buffer.bin")
		opts := &VectorsOptions{
			VectorSize:          DefaultVectorSize,
			MaxBufferSize:       10,
			MaxAppendBufferSize: 5,
		}
		v, err := New(path, opts)
		if err != nil {
			t.Fatalf("failed to create vectors: %v", err)
		}

		// Append and flush 20 vectors
		vectors := make([][]float32, 20)
		for i := 0; i < 20; i++ {
			vectors[i] = make([]float32, DefaultVectorSize)
			for j := range vectors[i] {
				vectors[i][j] = float32(i*100 + j)
			}
			_, err = v.Append(vectors[i])
			if err != nil {
				t.Fatalf("failed to append vector %d: %v", i, err)
			}
		}
		err = v.Flush()
		if err != nil {
			t.Fatalf("failed to flush: %v", err)
		}

		// Load buffer manually
		v.buffer, err = v.loadBuffer(5, 10)
		if err != nil {
			t.Fatalf("failed to load buffer: %v", err)
		}

		// Verify buffer is loaded
		if len(v.buffer.rows) == 0 {
			t.Fatal("expected buffer to be loaded")
		}

		// Delete some vectors
		err = v.Delete([]int{0, 10, 15})
		if err != nil {
			t.Fatalf("failed to delete vectors: %v", err)
		}

		// Verify buffer was cleared
		if len(v.buffer.rows) != 0 {
			t.Errorf("expected buffer to be cleared after delete, got length %d", len(v.buffer.rows))
		}
		if v.buffer.start != 0 {
			t.Errorf("expected buffer.start to be 0 after delete, got %d", v.buffer.start)
		}
		if v.buffer.data != nil {
			t.Error("expected buffer.data to be nil after delete")
		}
	})

	t.Run("delete first vector", func(t *testing.T) {
		path := filepath.Join(tmpDir, "vectors_delete_first.bin")
		v, err := New(path, nil)
		if err != nil {
			t.Fatalf("failed to create vectors: %v", err)
		}

		// Append and flush 5 vectors
		vectors := make([][]float32, 5)
		for i := 0; i < 5; i++ {
			vectors[i] = make([]float32, DefaultVectorSize)
			for j := range vectors[i] {
				vectors[i][j] = float32(i*100 + j)
			}
			_, err = v.Append(vectors[i])
			if err != nil {
				t.Fatalf("failed to append vector %d: %v", i, err)
			}
		}
		err = v.Flush()
		if err != nil {
			t.Fatalf("failed to flush: %v", err)
		}

		// Delete first vector
		err = v.Delete([]int{0})
		if err != nil {
			t.Fatalf("failed to delete first vector: %v", err)
		}

		// Verify count
		if v.Count() != 4 {
			t.Errorf("expected count 4, got %d", v.Count())
		}

		// Verify remaining vectors
		// Original: [0, 1, 2, 3, 4]
		// After deleting index 0: [1, 2, 3, 4]
		expectedIndexes := []int{1, 2, 3, 4}
		for newIdx, oldIdx := range expectedIndexes {
			retrieved, err := v.Get(newIdx)
			if err != nil {
				t.Fatalf("failed to get vector at new index %d: %v", newIdx, err)
			}
			for j := range vectors[oldIdx] {
				if retrieved[j] != vectors[oldIdx][j] {
					t.Errorf("new index %d (old index %d), element %d: expected %f, got %f",
						newIdx, oldIdx, j, vectors[oldIdx][j], retrieved[j])
				}
			}
		}
	})

	t.Run("delete last vector", func(t *testing.T) {
		path := filepath.Join(tmpDir, "vectors_delete_last.bin")
		v, err := New(path, nil)
		if err != nil {
			t.Fatalf("failed to create vectors: %v", err)
		}

		// Append and flush 5 vectors
		vectors := make([][]float32, 5)
		for i := 0; i < 5; i++ {
			vectors[i] = make([]float32, DefaultVectorSize)
			for j := range vectors[i] {
				vectors[i][j] = float32(i*100 + j)
			}
			_, err = v.Append(vectors[i])
			if err != nil {
				t.Fatalf("failed to append vector %d: %v", i, err)
			}
		}
		err = v.Flush()
		if err != nil {
			t.Fatalf("failed to flush: %v", err)
		}

		// Delete last vector
		err = v.Delete([]int{4})
		if err != nil {
			t.Fatalf("failed to delete last vector: %v", err)
		}

		// Verify count
		if v.Count() != 4 {
			t.Errorf("expected count 4, got %d", v.Count())
		}

		// Verify remaining vectors
		// Original: [0, 1, 2, 3, 4]
		// After deleting index 4: [0, 1, 2, 3]
		for i := 0; i < 4; i++ {
			retrieved, err := v.Get(i)
			if err != nil {
				t.Fatalf("failed to get vector at index %d: %v", i, err)
			}
			for j := range vectors[i] {
				if retrieved[j] != vectors[i][j] {
					t.Errorf("index %d, element %d: expected %f, got %f",
						i, j, vectors[i][j], retrieved[j])
				}
			}
		}
	})

	t.Run("delete persists after reopening storage", func(t *testing.T) {
		path := filepath.Join(tmpDir, "vectors_delete_reopen.bin")

		// Create and populate vectors
		v1, err := New(path, nil)
		if err != nil {
			t.Fatalf("failed to create vectors: %v", err)
		}

		vectors := make([][]float32, 5)
		for i := 0; i < 5; i++ {
			vectors[i] = make([]float32, DefaultVectorSize)
			for j := range vectors[i] {
				vectors[i][j] = float32(i*100 + j)
			}
			_, err = v1.Append(vectors[i])
			if err != nil {
				t.Fatalf("failed to append vector %d: %v", i, err)
			}
		}
		err = v1.Flush()
		if err != nil {
			t.Fatalf("failed to flush: %v", err)
		}

		// Delete vectors at indexes 1 and 3
		err = v1.Delete([]int{1, 3})
		if err != nil {
			t.Fatalf("failed to delete vectors: %v", err)
		}

		// Reopen storage
		v2, err := New(path, nil)
		if err != nil {
			t.Fatalf("failed to reopen vectors: %v", err)
		}

		// Verify count
		if v2.Count() != 3 {
			t.Errorf("expected count 3 after reopen, got %d", v2.Count())
		}

		// Verify remaining vectors
		// Original: [0, 1, 2, 3, 4]
		// After deleting 1, 3: [0, 2, 4]
		expectedIndexes := []int{0, 2, 4}
		for newIdx, oldIdx := range expectedIndexes {
			retrieved, err := v2.Get(newIdx)
			if err != nil {
				t.Fatalf("failed to get vector at new index %d after reopen: %v", newIdx, err)
			}
			for j := range vectors[oldIdx] {
				if retrieved[j] != vectors[oldIdx][j] {
					t.Errorf("new index %d (old index %d), element %d: expected %f, got %f",
						newIdx, oldIdx, j, vectors[oldIdx][j], retrieved[j])
				}
			}
		}
	})

	t.Run("delete with custom vector size", func(t *testing.T) {
		path := filepath.Join(tmpDir, "vectors_delete_custom_size.bin")
		customSize := 128
		opts := &VectorsOptions{
			VectorSize:          customSize,
			MaxBufferSize:       10,
			MaxAppendBufferSize: 5,
		}
		v, err := New(path, opts)
		if err != nil {
			t.Fatalf("failed to create vectors: %v", err)
		}

		// Append and flush vectors with custom size
		vectors := make([][]float32, 5)
		for i := 0; i < 5; i++ {
			vectors[i] = make([]float32, customSize)
			for j := range vectors[i] {
				vectors[i][j] = float32(i*50 + j)
			}
			_, err = v.Append(vectors[i])
			if err != nil {
				t.Fatalf("failed to append vector %d: %v", i, err)
			}
		}
		err = v.Flush()
		if err != nil {
			t.Fatalf("failed to flush: %v", err)
		}

		// Delete vector at index 2
		err = v.Delete([]int{2})
		if err != nil {
			t.Fatalf("failed to delete vector: %v", err)
		}

		// Verify count
		if v.Count() != 4 {
			t.Errorf("expected count 4, got %d", v.Count())
		}

		// Verify remaining vectors have correct size and content
		expectedIndexes := []int{0, 1, 3, 4}
		for newIdx, oldIdx := range expectedIndexes {
			retrieved, err := v.Get(newIdx)
			if err != nil {
				t.Fatalf("failed to get vector at new index %d: %v", newIdx, err)
			}
			if len(retrieved) != customSize {
				t.Errorf("expected vector length %d, got %d", customSize, len(retrieved))
			}
			for j := range vectors[oldIdx] {
				if retrieved[j] != vectors[oldIdx][j] {
					t.Errorf("new index %d (old index %d), element %d: expected %f, got %f",
						newIdx, oldIdx, j, vectors[oldIdx][j], retrieved[j])
				}
			}
		}
	})

	t.Run("delete with duplicate indexes", func(t *testing.T) {
		path := filepath.Join(tmpDir, "vectors_delete_duplicates.bin")
		v, err := New(path, nil)
		if err != nil {
			t.Fatalf("failed to create vectors: %v", err)
		}

		// Append and flush 5 vectors
		vectors := make([][]float32, 5)
		for i := 0; i < 5; i++ {
			vectors[i] = make([]float32, DefaultVectorSize)
			for j := range vectors[i] {
				vectors[i][j] = float32(i*100 + j)
			}
			_, err = v.Append(vectors[i])
			if err != nil {
				t.Fatalf("failed to append vector %d: %v", i, err)
			}
		}
		err = v.Flush()
		if err != nil {
			t.Fatalf("failed to flush: %v", err)
		}

		// Delete with duplicate indexes (should handle gracefully)
		err = v.Delete([]int{1, 3, 1, 3})
		if err != nil {
			t.Fatalf("failed to delete vectors with duplicates: %v", err)
		}

		// Verify count (should delete 1 and 3 only once)
		if v.Count() != 3 {
			t.Errorf("expected count 3, got %d", v.Count())
		}

		// Verify remaining vectors
		// Original: [0, 1, 2, 3, 4]
		// After deleting 1, 3: [0, 2, 4]
		expectedIndexes := []int{0, 2, 4}
		for newIdx, oldIdx := range expectedIndexes {
			retrieved, err := v.Get(newIdx)
			if err != nil {
				t.Fatalf("failed to get vector at new index %d: %v", newIdx, err)
			}
			for j := range vectors[oldIdx] {
				if retrieved[j] != vectors[oldIdx][j] {
					t.Errorf("new index %d (old index %d), element %d: expected %f, got %f",
						newIdx, oldIdx, j, vectors[oldIdx][j], retrieved[j])
				}
			}
		}
	})

	t.Run("delete non-contiguous indexes", func(t *testing.T) {
		path := filepath.Join(tmpDir, "vectors_delete_non_contiguous.bin")
		v, err := New(path, nil)
		if err != nil {
			t.Fatalf("failed to create vectors: %v", err)
		}

		// Append and flush 10 vectors
		vectors := make([][]float32, 10)
		for i := 0; i < 10; i++ {
			vectors[i] = make([]float32, DefaultVectorSize)
			for j := range vectors[i] {
				vectors[i][j] = float32(i*100 + j)
			}
			_, err = v.Append(vectors[i])
			if err != nil {
				t.Fatalf("failed to append vector %d: %v", i, err)
			}
		}
		err = v.Flush()
		if err != nil {
			t.Fatalf("failed to flush: %v", err)
		}

		// Delete non-contiguous indexes (0, 2, 5, 9)
		err = v.Delete([]int{0, 2, 5, 9})
		if err != nil {
			t.Fatalf("failed to delete vectors: %v", err)
		}

		// Verify count
		if v.Count() != 6 {
			t.Errorf("expected count 6, got %d", v.Count())
		}

		// Verify remaining vectors
		// Original: [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
		// After deleting 0, 2, 5, 9: [1, 3, 4, 6, 7, 8]
		expectedIndexes := []int{1, 3, 4, 6, 7, 8}
		for newIdx, oldIdx := range expectedIndexes {
			retrieved, err := v.Get(newIdx)
			if err != nil {
				t.Fatalf("failed to get vector at new index %d: %v", newIdx, err)
			}
			for j := range vectors[oldIdx] {
				if retrieved[j] != vectors[oldIdx][j] {
					t.Errorf("new index %d (old index %d), element %d: expected %f, got %f",
						newIdx, oldIdx, j, vectors[oldIdx][j], retrieved[j])
				}
			}
		}
	})

	t.Run("delete and then append new vectors", func(t *testing.T) {
		path := filepath.Join(tmpDir, "vectors_delete_then_append.bin")
		v, err := New(path, nil)
		if err != nil {
			t.Fatalf("failed to create vectors: %v", err)
		}

		// Append and flush 5 vectors
		vectors := make([][]float32, 5)
		for i := 0; i < 5; i++ {
			vectors[i] = make([]float32, DefaultVectorSize)
			for j := range vectors[i] {
				vectors[i][j] = float32(i*100 + j)
			}
			_, err = v.Append(vectors[i])
			if err != nil {
				t.Fatalf("failed to append vector %d: %v", i, err)
			}
		}
		err = v.Flush()
		if err != nil {
			t.Fatalf("failed to flush: %v", err)
		}

		// Delete some vectors
		err = v.Delete([]int{1, 3})
		if err != nil {
			t.Fatalf("failed to delete vectors: %v", err)
		}

		// Verify count after delete
		if v.Count() != 3 {
			t.Errorf("expected count 3 after delete, got %d", v.Count())
		}

		// Append new vectors
		newVectors := make([][]float32, 2)
		for i := 0; i < 2; i++ {
			newVectors[i] = make([]float32, DefaultVectorSize)
			for j := range newVectors[i] {
				newVectors[i][j] = float32(9999 + i)
			}
			_, err = v.Append(newVectors[i])
			if err != nil {
				t.Fatalf("failed to append new vector %d: %v", i, err)
			}
		}

		// Verify count after append
		if v.Count() != 5 {
			t.Errorf("expected count 5 after append, got %d", v.Count())
		}

		// Flush and verify
		err = v.Flush()
		if err != nil {
			t.Fatalf("failed to flush: %v", err)
		}

		// Verify old vectors are still correct
		// After delete: [0, 2, 4] at indexes [0, 1, 2]
		oldExpectedIndexes := []int{0, 2, 4}
		for newIdx, oldIdx := range oldExpectedIndexes {
			retrieved, err := v.Get(newIdx)
			if err != nil {
				t.Fatalf("failed to get old vector at new index %d: %v", newIdx, err)
			}
			for j := range vectors[oldIdx] {
				if retrieved[j] != vectors[oldIdx][j] {
					t.Errorf("old vector new index %d (old index %d), element %d: expected %f, got %f",
						newIdx, oldIdx, j, vectors[oldIdx][j], retrieved[j])
				}
			}
		}

		// Verify new vectors
		for i := 0; i < 2; i++ {
			retrieved, err := v.Get(3 + i)
			if err != nil {
				t.Fatalf("failed to get new vector at index %d: %v", 3+i, err)
			}
			for j := range newVectors[i] {
				if retrieved[j] != newVectors[i][j] {
					t.Errorf("new vector index %d, element %d: expected %f, got %f",
						3+i, j, newVectors[i][j], retrieved[j])
				}
			}
		}
	})

	t.Run("delete unordered indexes", func(t *testing.T) {
		path := filepath.Join(tmpDir, "vectors_delete_unordered.bin")
		v, err := New(path, nil)
		if err != nil {
			t.Fatalf("failed to create vectors: %v", err)
		}

		// Append and flush 7 vectors
		vectors := make([][]float32, 7)
		for i := 0; i < 7; i++ {
			vectors[i] = make([]float32, DefaultVectorSize)
			for j := range vectors[i] {
				vectors[i][j] = float32(i*100 + j)
			}
			_, err = v.Append(vectors[i])
			if err != nil {
				t.Fatalf("failed to append vector %d: %v", i, err)
			}
		}
		err = v.Flush()
		if err != nil {
			t.Fatalf("failed to flush: %v", err)
		}

		// Delete with unordered indexes (5, 1, 3)
		err = v.Delete([]int{5, 1, 3})
		if err != nil {
			t.Fatalf("failed to delete vectors: %v", err)
		}

		// Verify count
		if v.Count() != 4 {
			t.Errorf("expected count 4, got %d", v.Count())
		}

		// Verify remaining vectors
		// Original: [0, 1, 2, 3, 4, 5, 6]
		// After deleting 1, 3, 5: [0, 2, 4, 6]
		expectedIndexes := []int{0, 2, 4, 6}
		for newIdx, oldIdx := range expectedIndexes {
			retrieved, err := v.Get(newIdx)
			if err != nil {
				t.Fatalf("failed to get vector at new index %d: %v", newIdx, err)
			}
			for j := range vectors[oldIdx] {
				if retrieved[j] != vectors[oldIdx][j] {
					t.Errorf("new index %d (old index %d), element %d: expected %f, got %f",
						newIdx, oldIdx, j, vectors[oldIdx][j], retrieved[j])
				}
			}
		}
	})
}
