package vectors

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/webzak/mindstore/internal/testutil"
)

func TestReplace(t *testing.T) {
	tmpDir := testutil.MakeTempDir(t)
	defer os.RemoveAll(tmpDir)

	t.Run("replace with invalid vector length", func(t *testing.T) {
		path := filepath.Join(tmpDir, "vectors_replace_invalid.bin")
		v, err := New(path, nil)
		if err != nil {
			t.Fatalf("failed to create vectors: %v", err)
		}

		// Append a vector first
		vector := make([]float32, DefaultVectorSize)
		for i := range vector {
			vector[i] = float32(i)
		}
		idx, err := v.Append(vector)
		if err != nil {
			t.Fatalf("failed to append vector: %v", err)
		}

		// Try to replace with wrong size
		wrongSizeVector := make([]float32, DefaultVectorSize+10)
		err = v.Replace(idx, wrongSizeVector)
		if err == nil {
			t.Error("expected error for invalid vector length, got nil")
		}
	})

	t.Run("replace with index out of bounds - negative", func(t *testing.T) {
		path := filepath.Join(tmpDir, "vectors_replace_negative.bin")
		v, err := New(path, nil)
		if err != nil {
			t.Fatalf("failed to create vectors: %v", err)
		}

		vector := make([]float32, DefaultVectorSize)
		err = v.Replace(-1, vector)
		if err == nil {
			t.Error("expected error for negative index, got nil")
		}
	})

	t.Run("replace with index out of bounds - beyond count", func(t *testing.T) {
		path := filepath.Join(tmpDir, "vectors_replace_beyond.bin")
		v, err := New(path, nil)
		if err != nil {
			t.Fatalf("failed to create vectors: %v", err)
		}

		// Append one vector
		vector := make([]float32, DefaultVectorSize)
		_, err = v.Append(vector)
		if err != nil {
			t.Fatalf("failed to append vector: %v", err)
		}

		// Try to replace at index 5 (only index 0 exists)
		newVector := make([]float32, DefaultVectorSize)
		err = v.Replace(5, newVector)
		if err == nil {
			t.Error("expected error for index beyond count, got nil")
		}
	})

	t.Run("replace vector in persisted storage", func(t *testing.T) {
		path := filepath.Join(tmpDir, "vectors_replace_persisted.bin")
		v, err := New(path, nil)
		if err != nil {
			t.Fatalf("failed to create vectors: %v", err)
		}

		// Append and flush a vector
		originalVector := make([]float32, DefaultVectorSize)
		for i := range originalVector {
			originalVector[i] = float32(i)
		}
		idx, err := v.Append(originalVector)
		if err != nil {
			t.Fatalf("failed to append vector: %v", err)
		}
		err = v.Flush()
		if err != nil {
			t.Fatalf("failed to flush: %v", err)
		}

		// Verify persisted
		if v.persistedSize != 1 {
			t.Errorf("expected persistedSize 1, got %d", v.persistedSize)
		}

		// Replace the vector
		newVector := make([]float32, DefaultVectorSize)
		for i := range newVector {
			newVector[i] = float32(i * 10)
		}
		err = v.Replace(idx, newVector)
		if err != nil {
			t.Fatalf("failed to replace vector: %v", err)
		}

		// Retrieve and verify the replacement
		retrieved, err := v.Get(idx)
		if err != nil {
			t.Fatalf("failed to get vector: %v", err)
		}
		for i := range newVector {
			if retrieved[i] != newVector[i] {
				t.Errorf("at index %d: expected %f, got %f", i, newVector[i], retrieved[i])
			}
		}
	})

	t.Run("replace vector in persisted storage also updates memory buffer", func(t *testing.T) {
		path := filepath.Join(tmpDir, "vectors_replace_buffer_update.bin")
		opts := &VectorsOptions{
			VectorSize:          DefaultVectorSize,
			MaxBufferSize:       10,
			MaxAppendBufferSize: 5,
		}
		v, err := New(path, opts)
		if err != nil {
			t.Fatalf("failed to create vectors: %v", err)
		}

		// Append and flush multiple vectors
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

		// Manually load buffer with vectors 5-14 (but we only have 10 vectors, so 5-9)
		v.buffer, err = v.loadBuffer(5, 5)
		if err != nil {
			t.Fatalf("failed to load buffer: %v", err)
		}

		// Verify buffer is loaded
		if len(v.buffer.rows) == 0 {
			t.Fatal("expected buffer to be loaded")
		}
		if v.buffer.start != 5 {
			t.Errorf("expected buffer.start=5, got %d", v.buffer.start)
		}

		// Replace vector at index 6 (which should be in the buffer)
		newVector := make([]float32, DefaultVectorSize)
		for i := range newVector {
			newVector[i] = float32(999)
		}
		err = v.Replace(6, newVector)
		if err != nil {
			t.Fatalf("failed to replace vector: %v", err)
		}

		// Verify the buffer was updated
		bufferIndex := 6 - v.buffer.start
		for i := range newVector {
			if v.buffer.rows[bufferIndex][i] != newVector[i] {
				t.Errorf("buffer not updated: at index %d: expected %f, got %f", i, newVector[i], v.buffer.rows[bufferIndex][i])
			}
		}

		// Verify Get returns the new value
		retrieved, err := v.Get(6)
		if err != nil {
			t.Fatalf("failed to get vector: %v", err)
		}
		for i := range newVector {
			if retrieved[i] != newVector[i] {
				t.Errorf("at index %d: expected %f, got %f", i, newVector[i], retrieved[i])
			}
		}
	})

	t.Run("replace vector in append buffer triggers flush", func(t *testing.T) {
		path := filepath.Join(tmpDir, "vectors_replace_append.bin")
		v, err := New(path, nil)
		if err != nil {
			t.Fatalf("failed to create vectors: %v", err)
		}

		// Append a vector (not flushed)
		originalVector := make([]float32, DefaultVectorSize)
		for i := range originalVector {
			originalVector[i] = float32(i)
		}
		idx, err := v.Append(originalVector)
		if err != nil {
			t.Fatalf("failed to append vector: %v", err)
		}

		// Verify it's in append buffer
		if len(v.appendBuffer) != 1 {
			t.Errorf("expected appendBuffer length 1, got %d", len(v.appendBuffer))
		}
		if v.persistedSize != 0 {
			t.Errorf("expected persistedSize 0, got %d", v.persistedSize)
		}

		// Replace the vector in append buffer
		newVector := make([]float32, DefaultVectorSize)
		for i := range newVector {
			newVector[i] = float32(i * 20)
		}
		err = v.Replace(idx, newVector)
		if err != nil {
			t.Fatalf("failed to replace vector: %v", err)
		}

		// Verify automatic flush occurred
		if len(v.appendBuffer) != 0 {
			t.Errorf("expected appendBuffer to be empty after replace, got length %d", len(v.appendBuffer))
		}
		if v.persistedSize != 1 {
			t.Errorf("expected persistedSize 1 after replace, got %d", v.persistedSize)
		}

		// Retrieve and verify the replacement
		retrieved, err := v.Get(idx)
		if err != nil {
			t.Fatalf("failed to get vector: %v", err)
		}
		for i := range newVector {
			if retrieved[i] != newVector[i] {
				t.Errorf("at index %d: expected %f, got %f", i, newVector[i], retrieved[i])
			}
		}
	})

	t.Run("replace multiple vectors in append buffer", func(t *testing.T) {
		path := filepath.Join(tmpDir, "vectors_replace_multiple_append.bin")
		v, err := New(path, nil)
		if err != nil {
			t.Fatalf("failed to create vectors: %v", err)
		}

		// Append 3 vectors
		vectors := make([][]float32, 3)
		for i := 0; i < 3; i++ {
			vectors[i] = make([]float32, DefaultVectorSize)
			for j := range vectors[i] {
				vectors[i][j] = float32(i*50 + j)
			}
			_, err = v.Append(vectors[i])
			if err != nil {
				t.Fatalf("failed to append vector %d: %v", i, err)
			}
		}

		// Replace vector at index 1
		newVector1 := make([]float32, DefaultVectorSize)
		for i := range newVector1 {
			newVector1[i] = float32(1111)
		}
		err = v.Replace(1, newVector1)
		if err != nil {
			t.Fatalf("failed to replace vector 1: %v", err)
		}

		// After first replace, all should be flushed
		if v.persistedSize != 3 {
			t.Errorf("expected persistedSize 3 after first replace, got %d", v.persistedSize)
		}

		// Verify the replacement
		retrieved1, err := v.Get(1)
		if err != nil {
			t.Fatalf("failed to get vector 1: %v", err)
		}
		for i := range newVector1 {
			if retrieved1[i] != newVector1[i] {
				t.Errorf("vector 1, index %d: expected %f, got %f", i, newVector1[i], retrieved1[i])
			}
		}

		// Verify other vectors are unchanged
		retrieved0, err := v.Get(0)
		if err != nil {
			t.Fatalf("failed to get vector 0: %v", err)
		}
		for i := range vectors[0] {
			if retrieved0[i] != vectors[0][i] {
				t.Errorf("vector 0, index %d: expected %f, got %f", i, vectors[0][i], retrieved0[i])
			}
		}

		retrieved2, err := v.Get(2)
		if err != nil {
			t.Fatalf("failed to get vector 2: %v", err)
		}
		for i := range vectors[2] {
			if retrieved2[i] != vectors[2][i] {
				t.Errorf("vector 2, index %d: expected %f, got %f", i, vectors[2][i], retrieved2[i])
			}
		}
	})

	t.Run("replace persists after reopening storage", func(t *testing.T) {
		path := filepath.Join(tmpDir, "vectors_replace_reopen.bin")

		// Create and append vectors
		v1, err := New(path, nil)
		if err != nil {
			t.Fatalf("failed to create vectors: %v", err)
		}

		originalVector := make([]float32, DefaultVectorSize)
		for i := range originalVector {
			originalVector[i] = float32(i)
		}
		idx, err := v1.Append(originalVector)
		if err != nil {
			t.Fatalf("failed to append vector: %v", err)
		}
		err = v1.Flush()
		if err != nil {
			t.Fatalf("failed to flush: %v", err)
		}

		// Replace the vector
		newVector := make([]float32, DefaultVectorSize)
		for i := range newVector {
			newVector[i] = float32(i * 100)
		}
		err = v1.Replace(idx, newVector)
		if err != nil {
			t.Fatalf("failed to replace vector: %v", err)
		}

		// Reopen storage
		v2, err := New(path, nil)
		if err != nil {
			t.Fatalf("failed to reopen vectors: %v", err)
		}

		// Verify the replacement persisted
		retrieved, err := v2.Get(idx)
		if err != nil {
			t.Fatalf("failed to get vector after reopen: %v", err)
		}
		for i := range newVector {
			if retrieved[i] != newVector[i] {
				t.Errorf("at index %d: expected %f, got %f", i, newVector[i], retrieved[i])
			}
		}
	})

	t.Run("replace mixed locations - persisted and append buffer", func(t *testing.T) {
		path := filepath.Join(tmpDir, "vectors_replace_mixed.bin")
		opts := &VectorsOptions{
			VectorSize:          DefaultVectorSize,
			MaxBufferSize:       5,
			MaxAppendBufferSize: 10,
		}
		v, err := New(path, opts)
		if err != nil {
			t.Fatalf("failed to create vectors: %v", err)
		}

		// Append and flush 5 vectors
		persistedVectors := make([][]float32, 5)
		for i := 0; i < 5; i++ {
			persistedVectors[i] = make([]float32, DefaultVectorSize)
			for j := range persistedVectors[i] {
				persistedVectors[i][j] = float32(i*100 + j)
			}
			_, err = v.Append(persistedVectors[i])
			if err != nil {
				t.Fatalf("failed to append vector %d: %v", i, err)
			}
		}
		err = v.Flush()
		if err != nil {
			t.Fatalf("failed to flush: %v", err)
		}

		// Append 3 more vectors (in append buffer)
		appendVectors := make([][]float32, 3)
		for i := 0; i < 3; i++ {
			appendVectors[i] = make([]float32, DefaultVectorSize)
			for j := range appendVectors[i] {
				appendVectors[i][j] = float32((i+5)*100 + j)
			}
			_, err = v.Append(appendVectors[i])
			if err != nil {
				t.Fatalf("failed to append vector %d: %v", i+5, err)
			}
		}

		// Verify state before replacements
		if v.persistedSize != 5 {
			t.Errorf("expected persistedSize 5, got %d", v.persistedSize)
		}
		if len(v.appendBuffer) != 3 {
			t.Errorf("expected appendBuffer length 3, got %d", len(v.appendBuffer))
		}

		// Replace a persisted vector (index 2)
		newPersistedVector := make([]float32, DefaultVectorSize)
		for i := range newPersistedVector {
			newPersistedVector[i] = float32(2222)
		}
		err = v.Replace(2, newPersistedVector)
		if err != nil {
			t.Fatalf("failed to replace persisted vector: %v", err)
		}

		// Verify persisted vector was replaced
		retrieved2, err := v.Get(2)
		if err != nil {
			t.Fatalf("failed to get vector 2: %v", err)
		}
		for i := range newPersistedVector {
			if retrieved2[i] != newPersistedVector[i] {
				t.Errorf("vector 2, index %d: expected %f, got %f", i, newPersistedVector[i], retrieved2[i])
			}
		}

		// Replace a vector in append buffer (index 6)
		newAppendVector := make([]float32, DefaultVectorSize)
		for i := range newAppendVector {
			newAppendVector[i] = float32(6666)
		}
		err = v.Replace(6, newAppendVector)
		if err != nil {
			t.Fatalf("failed to replace append buffer vector: %v", err)
		}

		// After replacing in append buffer, it should be flushed
		if len(v.appendBuffer) != 0 {
			t.Errorf("expected appendBuffer to be empty after replace, got length %d", len(v.appendBuffer))
		}
		if v.persistedSize != 8 {
			t.Errorf("expected persistedSize 8 after replace, got %d", v.persistedSize)
		}

		// Verify append buffer vector was replaced
		retrieved6, err := v.Get(6)
		if err != nil {
			t.Fatalf("failed to get vector 6: %v", err)
		}
		for i := range newAppendVector {
			if retrieved6[i] != newAppendVector[i] {
				t.Errorf("vector 6, index %d: expected %f, got %f", i, newAppendVector[i], retrieved6[i])
			}
		}

		// Verify other vectors are unchanged
		retrieved0, err := v.Get(0)
		if err != nil {
			t.Fatalf("failed to get vector 0: %v", err)
		}
		for i := range persistedVectors[0] {
			if retrieved0[i] != persistedVectors[0][i] {
				t.Errorf("vector 0, index %d: expected %f, got %f", i, persistedVectors[0][i], retrieved0[i])
			}
		}
	})

	t.Run("replace with custom vector size", func(t *testing.T) {
		path := filepath.Join(tmpDir, "vectors_replace_custom_size.bin")
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

		// Append a vector with custom size
		originalVector := make([]float32, customSize)
		for i := range originalVector {
			originalVector[i] = float32(i)
		}
		idx, err := v.Append(originalVector)
		if err != nil {
			t.Fatalf("failed to append vector: %v", err)
		}
		err = v.Flush()
		if err != nil {
			t.Fatalf("failed to flush: %v", err)
		}

		// Replace with new vector
		newVector := make([]float32, customSize)
		for i := range newVector {
			newVector[i] = float32(i * 5)
		}
		err = v.Replace(idx, newVector)
		if err != nil {
			t.Fatalf("failed to replace vector: %v", err)
		}

		// Verify the replacement
		retrieved, err := v.Get(idx)
		if err != nil {
			t.Fatalf("failed to get vector: %v", err)
		}
		if len(retrieved) != customSize {
			t.Errorf("expected length %d, got %d", customSize, len(retrieved))
		}
		for i := range newVector {
			if retrieved[i] != newVector[i] {
				t.Errorf("at index %d: expected %f, got %f", i, newVector[i], retrieved[i])
			}
		}
	})

	t.Run("replace same vector multiple times", func(t *testing.T) {
		path := filepath.Join(tmpDir, "vectors_replace_multiple_times.bin")
		v, err := New(path, nil)
		if err != nil {
			t.Fatalf("failed to create vectors: %v", err)
		}

		// Append and flush a vector
		originalVector := make([]float32, DefaultVectorSize)
		for i := range originalVector {
			originalVector[i] = float32(i)
		}
		idx, err := v.Append(originalVector)
		if err != nil {
			t.Fatalf("failed to append vector: %v", err)
		}
		err = v.Flush()
		if err != nil {
			t.Fatalf("failed to flush: %v", err)
		}

		// Replace multiple times
		for iteration := 1; iteration <= 5; iteration++ {
			newVector := make([]float32, DefaultVectorSize)
			for i := range newVector {
				newVector[i] = float32(iteration * 1000)
			}
			err = v.Replace(idx, newVector)
			if err != nil {
				t.Fatalf("failed to replace vector (iteration %d): %v", iteration, err)
			}

			// Verify the replacement
			retrieved, err := v.Get(idx)
			if err != nil {
				t.Fatalf("failed to get vector (iteration %d): %v", iteration, err)
			}
			for i := range newVector {
				if retrieved[i] != newVector[i] {
					t.Errorf("iteration %d, index %d: expected %f, got %f", iteration, i, newVector[i], retrieved[i])
				}
			}
		}
	})
}
