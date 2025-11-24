package vectors

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/webzak/mindstore/internal/testutil"
)

func TestGet(t *testing.T) {
	tmpDir := testutil.MakeTempDir(t)
	defer os.RemoveAll(tmpDir)

	t.Run("index out of bounds - negative", func(t *testing.T) {
		path := filepath.Join(tmpDir, "vectors_get_negative.bin")
		v, err := New(path, nil)
		if err != nil {
			t.Fatalf("failed to create vectors: %v", err)
		}

		_, err = v.Get(-1)
		if err == nil {
			t.Error("expected error for negative index, got nil")
		}
	})

	t.Run("index out of bounds - empty storage", func(t *testing.T) {
		path := filepath.Join(tmpDir, "vectors_get_empty.bin")
		v, err := New(path, nil)
		if err != nil {
			t.Fatalf("failed to create vectors: %v", err)
		}

		_, err = v.Get(0)
		if err == nil {
			t.Error("expected error for index 0 on empty storage, got nil")
		}
	})

	t.Run("index out of bounds - beyond count", func(t *testing.T) {
		path := filepath.Join(tmpDir, "vectors_get_beyond.bin")
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

		// Try to get index 1 (only index 0 exists)
		_, err = v.Get(1)
		if err == nil {
			t.Error("expected error for index beyond count, got nil")
		}
	})

	t.Run("get from append buffer", func(t *testing.T) {
		path := filepath.Join(tmpDir, "vectors_get_append.bin")
		v, err := New(path, nil)
		if err != nil {
			t.Fatalf("failed to create vectors: %v", err)
		}

		// Create and append a vector
		vector := make([]float32, DefaultVectorSize)
		for i := range vector {
			vector[i] = float32(i)
		}
		idx, err := v.Append(vector)
		if err != nil {
			t.Fatalf("failed to append vector: %v", err)
		}

		// Get the vector from append buffer (not yet flushed)
		retrieved, err := v.Get(idx)
		if err != nil {
			t.Fatalf("failed to get vector: %v", err)
		}

		// Verify the vector matches
		if len(retrieved) != len(vector) {
			t.Errorf("expected length %d, got %d", len(vector), len(retrieved))
		}
		for i := range vector {
			if retrieved[i] != vector[i] {
				t.Errorf("at index %d: expected %f, got %f", i, vector[i], retrieved[i])
			}
		}
	})

	t.Run("get from persisted storage", func(t *testing.T) {
		path := filepath.Join(tmpDir, "vectors_get_persisted.bin")
		v, err := New(path, nil)
		if err != nil {
			t.Fatalf("failed to create vectors: %v", err)
		}

		// Create and append a vector
		vector := make([]float32, DefaultVectorSize)
		for i := range vector {
			vector[i] = float32(i * 2)
		}
		idx, err := v.Append(vector)
		if err != nil {
			t.Fatalf("failed to append vector: %v", err)
		}

		// Flush to persist
		err = v.Flush()
		if err != nil {
			t.Fatalf("failed to flush: %v", err)
		}

		// Get the vector from persisted storage
		retrieved, err := v.Get(idx)
		if err != nil {
			t.Fatalf("failed to get vector: %v", err)
		}

		// Verify the vector matches
		if len(retrieved) != len(vector) {
			t.Errorf("expected length %d, got %d", len(vector), len(retrieved))
		}
		for i := range vector {
			if retrieved[i] != vector[i] {
				t.Errorf("at index %d: expected %f, got %f", i, vector[i], retrieved[i])
			}
		}
	})

	t.Run("get from memory buffer", func(t *testing.T) {
		path := filepath.Join(tmpDir, "vectors_get_buffer.bin")
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
		vectors := make([][]float32, 20)
		for i := 0; i < 20; i++ {
			vectors[i] = make([]float32, DefaultVectorSize)
			for j := range vectors[i] {
				vectors[i][j] = float32(i*1000 + j)
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

		// Load a buffer by accessing a vector (this will trigger loadBuffer)
		// Access vector at index 10, which should load buffer starting at offset 10
		retrieved, err := v.Get(10)
		if err != nil {
			t.Fatalf("failed to get vector 10: %v", err)
		}

		// Verify the vector matches
		if len(retrieved) != len(vectors[10]) {
			t.Errorf("expected length %d, got %d", len(vectors[10]), len(retrieved))
		}
		for i := range vectors[10] {
			if retrieved[i] != vectors[10][i] {
				t.Errorf("at index %d: expected %f, got %f", i, vectors[10][i], retrieved[i])
			}
		}

		// Now get another vector from the same buffer (should hit buffer)
		retrieved2, err := v.Get(11)
		if err != nil {
			t.Fatalf("failed to get vector 11: %v", err)
		}

		// Verify the vector matches
		for i := range vectors[11] {
			if retrieved2[i] != vectors[11][i] {
				t.Errorf("at index %d: expected %f, got %f", i, vectors[11][i], retrieved2[i])
			}
		}
	})

	t.Run("returned vector is a copy", func(t *testing.T) {
		path := filepath.Join(tmpDir, "vectors_get_copy.bin")
		v, err := New(path, nil)
		if err != nil {
			t.Fatalf("failed to create vectors: %v", err)
		}

		// Create and append a vector
		vector := make([]float32, DefaultVectorSize)
		for i := range vector {
			vector[i] = float32(i)
		}
		idx, err := v.Append(vector)
		if err != nil {
			t.Fatalf("failed to append vector: %v", err)
		}

		// Get the vector
		retrieved, err := v.Get(idx)
		if err != nil {
			t.Fatalf("failed to get vector: %v", err)
		}

		// Modify the retrieved vector
		retrieved[0] = 999.0

		// Get the vector again
		retrieved2, err := v.Get(idx)
		if err != nil {
			t.Fatalf("failed to get vector again: %v", err)
		}

		// Verify the modification didn't affect the stored vector
		if retrieved2[0] == 999.0 {
			t.Error("modification to retrieved vector affected stored vector - not a proper copy")
		}
		if retrieved2[0] != 0.0 {
			t.Errorf("expected first element to be 0.0, got %f", retrieved2[0])
		}
	})

	t.Run("get multiple vectors from different locations", func(t *testing.T) {
		path := filepath.Join(tmpDir, "vectors_get_mixed.bin")
		opts := &VectorsOptions{
			VectorSize:          DefaultVectorSize,
			MaxBufferSize:       5,
			MaxAppendBufferSize: 3,
		}
		v, err := New(path, opts)
		if err != nil {
			t.Fatalf("failed to create vectors: %v", err)
		}

		// Append 10 vectors and flush (these will be in persisted storage)
		persistedVectors := make([][]float32, 10)
		for i := 0; i < 10; i++ {
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

		// Append 2 more vectors (these will be in append buffer)
		appendVectors := make([][]float32, 2)
		for i := 0; i < 2; i++ {
			appendVectors[i] = make([]float32, DefaultVectorSize)
			for j := range appendVectors[i] {
				appendVectors[i][j] = float32((i+10)*100 + j)
			}
			_, err = v.Append(appendVectors[i])
			if err != nil {
				t.Fatalf("failed to append vector %d: %v", i+10, err)
			}
		}

		// Get vector from persisted storage (index 0)
		retrieved0, err := v.Get(0)
		if err != nil {
			t.Fatalf("failed to get vector 0: %v", err)
		}
		for i := range persistedVectors[0] {
			if retrieved0[i] != persistedVectors[0][i] {
				t.Errorf("vector 0, index %d: expected %f, got %f", i, persistedVectors[0][i], retrieved0[i])
			}
		}

		// Get vector from persisted storage (index 5, will load into buffer)
		retrieved5, err := v.Get(5)
		if err != nil {
			t.Fatalf("failed to get vector 5: %v", err)
		}
		for i := range persistedVectors[5] {
			if retrieved5[i] != persistedVectors[5][i] {
				t.Errorf("vector 5, index %d: expected %f, got %f", i, persistedVectors[5][i], retrieved5[i])
			}
		}

		// Get vector from memory buffer (index 6, should be in buffer from previous Get(5))
		retrieved6, err := v.Get(6)
		if err != nil {
			t.Fatalf("failed to get vector 6: %v", err)
		}
		for i := range persistedVectors[6] {
			if retrieved6[i] != persistedVectors[6][i] {
				t.Errorf("vector 6, index %d: expected %f, got %f", i, persistedVectors[6][i], retrieved6[i])
			}
		}

		// Get vector from append buffer (index 10)
		retrieved10, err := v.Get(10)
		if err != nil {
			t.Fatalf("failed to get vector 10: %v", err)
		}
		for i := range appendVectors[0] {
			if retrieved10[i] != appendVectors[0][i] {
				t.Errorf("vector 10, index %d: expected %f, got %f", i, appendVectors[0][i], retrieved10[i])
			}
		}

		// Get vector from append buffer (index 11)
		retrieved11, err := v.Get(11)
		if err != nil {
			t.Fatalf("failed to get vector 11: %v", err)
		}
		for i := range appendVectors[1] {
			if retrieved11[i] != appendVectors[1][i] {
				t.Errorf("vector 11, index %d: expected %f, got %f", i, appendVectors[1][i], retrieved11[i])
			}
		}
	})

	t.Run("get at buffer boundaries", func(t *testing.T) {
		path := filepath.Join(tmpDir, "vectors_get_boundaries.bin")
		opts := &VectorsOptions{
			VectorSize:          DefaultVectorSize,
			MaxBufferSize:       5,
			MaxAppendBufferSize: 5,
		}
		v, err := New(path, opts)
		if err != nil {
			t.Fatalf("failed to create vectors: %v", err)
		}

		// Append exactly MaxBufferSize vectors
		vectors := make([][]float32, 5)
		for i := 0; i < 5; i++ {
			vectors[i] = make([]float32, DefaultVectorSize)
			for j := range vectors[i] {
				vectors[i][j] = float32(i*10 + j)
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

		// Get first vector (index 0)
		retrieved0, err := v.Get(0)
		if err != nil {
			t.Fatalf("failed to get vector 0: %v", err)
		}
		for i := range vectors[0] {
			if retrieved0[i] != vectors[0][i] {
				t.Errorf("vector 0, index %d: expected %f, got %f", i, vectors[0][i], retrieved0[i])
			}
		}

		// Get last vector in buffer (index 4)
		retrieved4, err := v.Get(4)
		if err != nil {
			t.Fatalf("failed to get vector 4: %v", err)
		}
		for i := range vectors[4] {
			if retrieved4[i] != vectors[4][i] {
				t.Errorf("vector 4, index %d: expected %f, got %f", i, vectors[4][i], retrieved4[i])
			}
		}
	})

	t.Run("get after reopening storage", func(t *testing.T) {
		path := filepath.Join(tmpDir, "vectors_get_reopen.bin")

		// Create vectors and append some data
		v1, err := New(path, nil)
		if err != nil {
			t.Fatalf("failed to create vectors: %v", err)
		}

		vectors := make([][]float32, 3)
		for i := 0; i < 3; i++ {
			vectors[i] = make([]float32, DefaultVectorSize)
			for j := range vectors[i] {
				vectors[i][j] = float32(i*50 + j)
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

		// Reopen the storage
		v2, err := New(path, nil)
		if err != nil {
			t.Fatalf("failed to reopen vectors: %v", err)
		}

		// Get vectors from reopened storage
		for i := 0; i < 3; i++ {
			retrieved, err := v2.Get(i)
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

	t.Run("get with custom vector size", func(t *testing.T) {
		path := filepath.Join(tmpDir, "vectors_get_custom_size.bin")
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

		// Create and append a vector with custom size
		vector := make([]float32, customSize)
		for i := range vector {
			vector[i] = float32(i * 3)
		}
		idx, err := v.Append(vector)
		if err != nil {
			t.Fatalf("failed to append vector: %v", err)
		}
		err = v.Flush()
		if err != nil {
			t.Fatalf("failed to flush: %v", err)
		}

		// Get the vector
		retrieved, err := v.Get(idx)
		if err != nil {
			t.Fatalf("failed to get vector: %v", err)
		}

		// Verify size and content
		if len(retrieved) != customSize {
			t.Errorf("expected length %d, got %d", customSize, len(retrieved))
		}
		for i := range vector {
			if retrieved[i] != vector[i] {
				t.Errorf("at index %d: expected %f, got %f", i, vector[i], retrieved[i])
			}
		}
	})

	t.Run("get consecutive vectors from loaded buffer", func(t *testing.T) {
		path := filepath.Join(tmpDir, "vectors_get_buffer_hit.bin")
		opts := &VectorsOptions{
			VectorSize:          DefaultVectorSize,
			MaxBufferSize:       10,
			MaxAppendBufferSize: 5,
		}
		v, err := New(path, opts)
		if err != nil {
			t.Fatalf("failed to create vectors: %v", err)
		}

		// Append 20 vectors and flush
		vectors := make([][]float32, 20)
		for i := 0; i < 20; i++ {
			vectors[i] = make([]float32, DefaultVectorSize)
			for j := range vectors[i] {
				vectors[i][j] = float32(i*1000 + j)
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

		// Verify buffer is initially empty
		if len(v.buffer.rows) != 0 {
			t.Errorf("expected empty buffer initially, got length %d", len(v.buffer.rows))
		}

		// Directly load buffer with vectors 5-14
		v.buffer, err = v.loadBuffer(5, 10)
		if err != nil {
			t.Fatalf("failed to load buffer: %v", err)
		}

		// Verify buffer is loaded correctly
		if v.buffer.start != 5 {
			t.Errorf("expected buffer.start=5, got %d", v.buffer.start)
		}
		if len(v.buffer.rows) != 10 {
			t.Errorf("expected buffer.rows length=10, got %d", len(v.buffer.rows))
		}

		// Now Get() vectors within the buffer range (5-14)
		// These should hit the buffer path (lines 119-124) without reloading from storage
		for idx := 5; idx <= 14; idx++ {
			retrieved, err := v.Get(idx)
			if err != nil {
				t.Fatalf("failed to get vector %d: %v", idx, err)
			}

			// Verify the vector matches
			for i := range vectors[idx] {
				if retrieved[i] != vectors[idx][i] {
					t.Errorf("vector %d, index %d: expected %f, got %f", idx, i, vectors[idx][i], retrieved[i])
				}
			}

			// Verify buffer hasn't changed (proving it's a buffer hit, not a reload)
			if v.buffer.start != 5 {
				t.Errorf("buffer.start changed to %d, expected to stay at 5", v.buffer.start)
			}
			if len(v.buffer.rows) != 10 {
				t.Errorf("buffer.rows length changed to %d, expected to stay at 10", len(v.buffer.rows))
			}
		}

		// Get a vector outside the buffer range (index 2)
		// This should NOT use the buffer and will read directly from storage
		// The buffer should remain unchanged because Get() doesn't populate the buffer
		retrieved2, err := v.Get(2)
		if err != nil {
			t.Fatalf("failed to get vector 2: %v", err)
		}

		// Verify vector 2
		for i := range vectors[2] {
			if retrieved2[i] != vectors[2][i] {
				t.Errorf("vector 2, index %d: expected %f, got %f", i, vectors[2][i], retrieved2[i])
			}
		}

		// Verify buffer was NOT changed (Get() doesn't populate buffer)
		if v.buffer.start != 5 {
			t.Errorf("expected buffer.start to stay at 5, got %d", v.buffer.start)
		}
		if len(v.buffer.rows) != 10 {
			t.Errorf("expected buffer.rows length to stay at 10, got %d", len(v.buffer.rows))
		}
	})
}
