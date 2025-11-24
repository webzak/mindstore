package vectors

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/webzak/mindstore/internal/testutil"
)

func TestAppend(t *testing.T) {
	tmpDir := testutil.MakeTempDir(t)
	defer os.RemoveAll(tmpDir)

	t.Run("append single vector", func(t *testing.T) {
		path := filepath.Join(tmpDir, "vectors_append_single.bin")
		v, err := New(path, nil)
		if err != nil {
			t.Fatalf("failed to create vectors: %v", err)
		}

		// Create a vector
		vector := make([]float32, DefaultVectorSize)
		for i := range vector {
			vector[i] = float32(i)
		}

		// Append the vector
		idx, err := v.Append(vector)
		if err != nil {
			t.Fatalf("failed to append vector: %v", err)
		}

		// Verify index is 0 (first vector)
		if idx != 0 {
			t.Errorf("expected index 0, got %d", idx)
		}

		// Verify count
		if v.Count() != 1 {
			t.Errorf("expected count 1, got %d", v.Count())
		}

		// Verify vector is in append buffer
		if len(v.appendBuffer) != 1 {
			t.Errorf("expected appendBuffer length 1, got %d", len(v.appendBuffer))
		}

		// Verify persisted size is still 0
		if v.persistedSize != 0 {
			t.Errorf("expected persistedSize 0, got %d", v.persistedSize)
		}
	})

	t.Run("append invalid vector length", func(t *testing.T) {
		path := filepath.Join(tmpDir, "vectors_append_invalid.bin")
		v, err := New(path, nil)
		if err != nil {
			t.Fatalf("failed to create vectors: %v", err)
		}

		// Create a vector with wrong size
		vector := make([]float32, DefaultVectorSize+10)

		// Append should fail
		_, err = v.Append(vector)
		if err == nil {
			t.Error("expected error for invalid vector length, got nil")
		}
	})

	t.Run("automatic flushing when buffer is full", func(t *testing.T) {
		path := filepath.Join(tmpDir, "vectors_append_flush.bin")
		// Use small buffer size to test flushing
		opts := &VectorsOptions{
			VectorSize:          DefaultVectorSize,
			MaxBufferSize:       10,
			MaxAppendBufferSize: 5, // Flush after 5 vectors
		}
		v, err := New(path, opts)
		if err != nil {
			t.Fatalf("failed to create vectors: %v", err)
		}

		// Verify IsPersisted returns true initially
		if !v.IsPersisted() {
			t.Error("expected IsPersisted() to be true initially")
		}

		// Append vectors one by one
		vectors := make([][]float32, 10)
		for i := 0; i < 10; i++ {
			vectors[i] = make([]float32, DefaultVectorSize)
			for j := range vectors[i] {
				vectors[i][j] = float32(i*100 + j)
			}

			idx, err := v.Append(vectors[i])
			if err != nil {
				t.Fatalf("failed to append vector %d: %v", i, err)
			}

			// Verify index
			if idx != i {
				t.Errorf("vector %d: expected index %d, got %d", i, i, idx)
			}

			// Check buffer state based on position
			if i < 4 {
				// First 4 vectors should be in append buffer
				expectedBufferLen := i + 1
				if len(v.appendBuffer) != expectedBufferLen {
					t.Errorf("after appending vector %d: expected appendBuffer length %d, got %d", i, expectedBufferLen, len(v.appendBuffer))
				}
				if v.persistedSize != 0 {
					t.Errorf("after appending vector %d: expected persistedSize 0, got %d", i, v.persistedSize)
				}
				// IsPersisted should be false when there are vectors in append buffer
				if v.IsPersisted() {
					t.Errorf("after appending vector %d: expected IsPersisted() to be false", i)
				}
			} else if i == 4 {
				// After 5th vector (index 4), automatic flush should occur
				// Append buffer should be empty after flush
				if len(v.appendBuffer) != 0 {
					t.Errorf("after appending vector %d (should trigger flush): expected appendBuffer length 0, got %d", i, len(v.appendBuffer))
				}
				// All 5 vectors should be persisted
				if v.persistedSize != 5 {
					t.Errorf("after appending vector %d (should trigger flush): expected persistedSize 5, got %d", i, v.persistedSize)
				}
				// IsPersisted should be true after automatic flush
				if !v.IsPersisted() {
					t.Errorf("after appending vector %d (should trigger flush): expected IsPersisted() to be true", i)
				}
			} else if i >= 5 && i < 9 {
				// Vectors 5-8 should be in append buffer again
				expectedBufferLen := i - 4
				if len(v.appendBuffer) != expectedBufferLen {
					t.Errorf("after appending vector %d: expected appendBuffer length %d, got %d", i, expectedBufferLen, len(v.appendBuffer))
				}
				if v.persistedSize != 5 {
					t.Errorf("after appending vector %d: expected persistedSize 5, got %d", i, v.persistedSize)
				}
				// IsPersisted should be false when there are vectors in append buffer
				if v.IsPersisted() {
					t.Errorf("after appending vector %d: expected IsPersisted() to be false", i)
				}
			} else if i == 9 {
				// After 10th vector (index 9), another automatic flush should occur
				if len(v.appendBuffer) != 0 {
					t.Errorf("after appending vector %d (should trigger flush): expected appendBuffer length 0, got %d", i, len(v.appendBuffer))
				}
				// All 10 vectors should be persisted
				if v.persistedSize != 10 {
					t.Errorf("after appending vector %d (should trigger flush): expected persistedSize 10, got %d", i, v.persistedSize)
				}
				// IsPersisted should be true after automatic flush
				if !v.IsPersisted() {
					t.Errorf("after appending vector %d (should trigger flush): expected IsPersisted() to be true", i)
				}
			}
		}

		// Verify total count
		if v.Count() != 10 {
			t.Errorf("expected total count 10, got %d", v.Count())
		}

		// Verify all vectors can be retrieved correctly
		for i := 0; i < 10; i++ {
			retrieved, err := v.Get(i)
			if err != nil {
				t.Fatalf("failed to get vector %d: %v", i, err)
			}
			for j := range vectors[i] {
				if retrieved[j] != vectors[i][j] {
					t.Errorf("vector %d, index %d: expected %f, got %f", i, j, vectors[i][j], retrieved[j])
				}
			}
		}
	})

	t.Run("IsPersisted method verification", func(t *testing.T) {
		path := filepath.Join(tmpDir, "vectors_ispersisted.bin")
		opts := &VectorsOptions{
			VectorSize:          DefaultVectorSize,
			MaxBufferSize:       10,
			MaxAppendBufferSize: 3,
		}
		v, err := New(path, opts)
		if err != nil {
			t.Fatalf("failed to create vectors: %v", err)
		}

		// Initially should be persisted (empty append buffer)
		if !v.IsPersisted() {
			t.Error("expected IsPersisted() to be true for new vectors instance")
		}

		// Append one vector
		vector1 := make([]float32, DefaultVectorSize)
		for i := range vector1 {
			vector1[i] = float32(i)
		}
		_, err = v.Append(vector1)
		if err != nil {
			t.Fatalf("failed to append vector: %v", err)
		}

		// Should not be persisted (has data in append buffer)
		if v.IsPersisted() {
			t.Error("expected IsPersisted() to be false after appending one vector")
		}

		// Append second vector
		vector2 := make([]float32, DefaultVectorSize)
		for i := range vector2 {
			vector2[i] = float32(i * 2)
		}
		_, err = v.Append(vector2)
		if err != nil {
			t.Fatalf("failed to append second vector: %v", err)
		}

		// Still should not be persisted
		if v.IsPersisted() {
			t.Error("expected IsPersisted() to be false after appending two vectors")
		}

		// Manually flush
		err = v.Flush()
		if err != nil {
			t.Fatalf("failed to flush: %v", err)
		}

		// Should be persisted after manual flush
		if !v.IsPersisted() {
			t.Error("expected IsPersisted() to be true after manual flush")
		}

		// Append third vector (will not trigger auto-flush since maxAppendSize is 3)
		vector3 := make([]float32, DefaultVectorSize)
		for i := range vector3 {
			vector3[i] = float32(i * 3)
		}
		_, err = v.Append(vector3)
		if err != nil {
			t.Fatalf("failed to append third vector: %v", err)
		}

		// Should not be persisted again
		if v.IsPersisted() {
			t.Error("expected IsPersisted() to be false after appending third vector")
		}

		// Append fourth and fifth vectors to trigger auto-flush
		for k := 4; k <= 5; k++ {
			vector := make([]float32, DefaultVectorSize)
			for i := range vector {
				vector[i] = float32(i * k)
			}
			_, err = v.Append(vector)
			if err != nil {
				t.Fatalf("failed to append vector %d: %v", k, err)
			}
		}

		// Should be persisted after auto-flush (3 vectors trigger flush)
		if !v.IsPersisted() {
			t.Error("expected IsPersisted() to be true after auto-flush")
		}
	})

	t.Run("append and retrieve from buffer before flush", func(t *testing.T) {
		path := filepath.Join(tmpDir, "vectors_append_retrieve.bin")
		v, err := New(path, nil)
		if err != nil {
			t.Fatalf("failed to create vectors: %v", err)
		}

		// Append multiple vectors without flushing
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

		// Verify IsPersisted is false
		if v.IsPersisted() {
			t.Error("expected IsPersisted() to be false with vectors in append buffer")
		}

		// Retrieve vectors from append buffer
		for i := 0; i < 3; i++ {
			retrieved, err := v.Get(i)
			if err != nil {
				t.Fatalf("failed to get vector %d from append buffer: %v", i, err)
			}
			for j := range vectors[i] {
				if retrieved[j] != vectors[i][j] {
					t.Errorf("vector %d, index %d: expected %f, got %f", i, j, vectors[i][j], retrieved[j])
				}
			}
		}
	})

	t.Run("append after reopening storage", func(t *testing.T) {
		path := filepath.Join(tmpDir, "vectors_append_reopen.bin")

		// Create and append some vectors
		v1, err := New(path, nil)
		if err != nil {
			t.Fatalf("failed to create vectors: %v", err)
		}

		vector1 := make([]float32, DefaultVectorSize)
		for i := range vector1 {
			vector1[i] = float32(i)
		}
		idx1, err := v1.Append(vector1)
		if err != nil {
			t.Fatalf("failed to append vector: %v", err)
		}
		if idx1 != 0 {
			t.Errorf("expected index 0, got %d", idx1)
		}

		err = v1.Flush()
		if err != nil {
			t.Fatalf("failed to flush: %v", err)
		}

		// After flush, persistedSize should be 1 (number of vectors)
		if v1.persistedSize != 1 {
			t.Errorf("expected persistedSize 1 after flush, got %d", v1.persistedSize)
		}

		// Reopen storage
		v2, err := New(path, nil)
		if err != nil {
			t.Fatalf("failed to reopen vectors: %v", err)
		}

		// persistedSize should be the number of vectors (not the number of floats)
		// For 1 vector of 768 floats: (768 * 4 bytes) / (768 * 4) = 1
		expectedPersistedSize := 1
		if v2.persistedSize != expectedPersistedSize {
			t.Errorf("expected persistedSize %d, got %d", expectedPersistedSize, v2.persistedSize)
		}

		// Append another vector
		vector2 := make([]float32, DefaultVectorSize)
		for i := range vector2 {
			vector2[i] = float32(i * 2)
		}
		idx2, err := v2.Append(vector2)
		if err != nil {
			t.Fatalf("failed to append vector after reopen: %v", err)
		}

		// The index should be 1 (second vector)
		// index = persistedSize + len(appendBuffer) - 1 = 1 + 1 - 1 = 1
		expectedIdx := v2.persistedSize + len(v2.appendBuffer) - 1
		if idx2 != expectedIdx {
			t.Errorf("expected index %d, got %d", expectedIdx, idx2)
		}

		// Verify first vector can be retrieved
		retrieved1, err := v2.Get(0)
		if err != nil {
			t.Fatalf("failed to get first vector: %v", err)
		}
		for i := range vector1 {
			if retrieved1[i] != vector1[i] {
				t.Errorf("first vector, index %d: expected %f, got %f", i, vector1[i], retrieved1[i])
			}
		}

		// The second vector should be retrievable at its returned index
		retrieved2, err := v2.Get(idx2)
		if err != nil {
			t.Fatalf("failed to get second vector at index %d: %v", idx2, err)
		}
		for i := range vector2 {
			if retrieved2[i] != vector2[i] {
				t.Errorf("second vector, index %d: expected %f, got %f", i, vector2[i], retrieved2[i])
			}
		}
	})
}
