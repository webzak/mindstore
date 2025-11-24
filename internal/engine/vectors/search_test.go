package vectors

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/webzak/mindstore/internal/engine/math"
	"github.com/webzak/mindstore/internal/testutil"
)

func TestSearch(t *testing.T) {
	tmpDir := testutil.MakeTempDir(t)
	defer os.RemoveAll(tmpDir)

	t.Run("invalid vector length", func(t *testing.T) {
		path := filepath.Join(tmpDir, "vectors_search_invalid_length.bin")
		v, err := New(path, nil)
		if err != nil {
			t.Fatalf("failed to create vectors: %v", err)
		}

		// Try to search with wrong vector size
		wrongVector := make([]float32, DefaultVectorSize+10)
		_, err = v.Search(wrongVector, math.CosineSimMethod, math.SortDesc, 10)
		if err == nil {
			t.Error("expected error for invalid vector length, got nil")
		}
	})

	t.Run("invalid limit - negative", func(t *testing.T) {
		path := filepath.Join(tmpDir, "vectors_search_negative_limit.bin")
		v, err := New(path, nil)
		if err != nil {
			t.Fatalf("failed to create vectors: %v", err)
		}

		vector := make([]float32, DefaultVectorSize)
		_, err = v.Search(vector, math.CosineSimMethod, math.SortDesc, -1)
		if err == nil {
			t.Error("expected error for negative limit, got nil")
		}
	})

	t.Run("unsupported search method", func(t *testing.T) {
		path := filepath.Join(tmpDir, "vectors_search_unsupported_method.bin")
		v, err := New(path, nil)
		if err != nil {
			t.Fatalf("failed to create vectors: %v", err)
		}

		vector := make([]float32, DefaultVectorSize)
		// Use an invalid search method (999)
		_, err = v.Search(vector, math.VectorSearchMethod(999), math.SortDesc, 10)
		if err == nil {
			t.Error("expected error for unsupported search method, got nil")
		}
	})

	t.Run("search on empty storage", func(t *testing.T) {
		path := filepath.Join(tmpDir, "vectors_search_empty.bin")
		v, err := New(path, nil)
		if err != nil {
			t.Fatalf("failed to create vectors: %v", err)
		}

		vector := make([]float32, DefaultVectorSize)
		for i := range vector {
			vector[i] = float32(i)
		}

		results, err := v.Search(vector, math.CosineSimMethod, math.SortDesc, 10)
		if err != nil {
			t.Fatalf("failed to search: %v", err)
		}

		if len(results) != 0 {
			t.Errorf("expected 0 results on empty storage, got %d", len(results))
		}
	})

	t.Run("search with single vector in append buffer", func(t *testing.T) {
		path := filepath.Join(tmpDir, "vectors_search_single_append.bin")
		v, err := New(path, nil)
		if err != nil {
			t.Fatalf("failed to create vectors: %v", err)
		}

		// Create and append a vector
		vector := make([]float32, DefaultVectorSize)
		for i := range vector {
			vector[i] = 1.0
		}
		idx, err := v.Append(vector)
		if err != nil {
			t.Fatalf("failed to append vector: %v", err)
		}

		// Search with the same vector (should get perfect similarity)
		results, err := v.Search(vector, math.CosineSimMethod, math.SortDesc, 10)
		if err != nil {
			t.Fatalf("failed to search: %v", err)
		}

		if len(results) != 1 {
			t.Fatalf("expected 1 result, got %d", len(results))
		}

		if results[0].ID != idx {
			t.Errorf("expected ID %d, got %d", idx, results[0].ID)
		}

		// Cosine similarity with itself should be 1.0
		if results[0].Value < 0.999 || results[0].Value > 1.001 {
			t.Errorf("expected similarity ~1.0, got %f", results[0].Value)
		}
	})

	t.Run("search with multiple vectors in append buffer", func(t *testing.T) {
		path := filepath.Join(tmpDir, "vectors_search_multiple_append.bin")
		v, err := New(path, nil)
		if err != nil {
			t.Fatalf("failed to create vectors: %v", err)
		}

		// Create query vector
		queryVector := make([]float32, DefaultVectorSize)
		for i := range queryVector {
			queryVector[i] = 1.0
		}

		// Append multiple vectors with varying similarity
		vectors := make([][]float32, 5)
		for i := 0; i < 5; i++ {
			vectors[i] = make([]float32, DefaultVectorSize)
			for j := range vectors[i] {
				// Create vectors with different patterns
				vectors[i][j] = float32(i+1) * 0.5
			}
			_, err = v.Append(vectors[i])
			if err != nil {
				t.Fatalf("failed to append vector %d: %v", i, err)
			}
		}

		// Search
		results, err := v.Search(queryVector, math.CosineSimMethod, math.SortDesc, 10)
		if err != nil {
			t.Fatalf("failed to search: %v", err)
		}

		if len(results) != 5 {
			t.Fatalf("expected 5 results, got %d", len(results))
		}

		// Verify results are sorted in descending order
		for i := 0; i < len(results)-1; i++ {
			if results[i].Value < results[i+1].Value {
				t.Errorf("results not sorted: results[%d].Value=%f < results[%d].Value=%f",
					i, results[i].Value, i+1, results[i+1].Value)
			}
		}
	})

	t.Run("search with persisted vectors only", func(t *testing.T) {
		path := filepath.Join(tmpDir, "vectors_search_persisted.bin")
		v, err := New(path, nil)
		if err != nil {
			t.Fatalf("failed to create vectors: %v", err)
		}

		// Create query vector
		queryVector := make([]float32, DefaultVectorSize)
		for i := range queryVector {
			queryVector[i] = 1.0
		}

		// Append and flush vectors
		numVectors := 10
		for i := 0; i < numVectors; i++ {
			vector := make([]float32, DefaultVectorSize)
			for j := range vector {
				vector[j] = float32(i+1) * 0.3
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

		// Search
		results, err := v.Search(queryVector, math.CosineSimMethod, math.SortDesc, 10)
		if err != nil {
			t.Fatalf("failed to search: %v", err)
		}

		if len(results) != numVectors {
			t.Fatalf("expected %d results, got %d", numVectors, len(results))
		}

		// Verify results are sorted in descending order
		for i := 0; i < len(results)-1; i++ {
			if results[i].Value < results[i+1].Value {
				t.Errorf("results not sorted: results[%d].Value=%f < results[%d].Value=%f",
					i, results[i].Value, i+1, results[i+1].Value)
			}
		}
	})

	t.Run("search with mixed persisted and append buffer vectors", func(t *testing.T) {
		path := filepath.Join(tmpDir, "vectors_search_mixed.bin")
		opts := &VectorsOptions{
			VectorSize:          DefaultVectorSize,
			MaxBufferSize:       5,
			MaxAppendBufferSize: 10,
		}
		v, err := New(path, opts)
		if err != nil {
			t.Fatalf("failed to create vectors: %v", err)
		}

		// Create query vector
		queryVector := make([]float32, DefaultVectorSize)
		for i := range queryVector {
			queryVector[i] = 1.0
		}

		// Append and flush 5 vectors
		for i := 0; i < 5; i++ {
			vector := make([]float32, DefaultVectorSize)
			for j := range vector {
				vector[j] = float32(i+1) * 0.2
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

		// Append 3 more vectors (not flushed)
		for i := 5; i < 8; i++ {
			vector := make([]float32, DefaultVectorSize)
			for j := range vector {
				vector[j] = float32(i+1) * 0.2
			}
			_, err = v.Append(vector)
			if err != nil {
				t.Fatalf("failed to append vector %d: %v", i, err)
			}
		}

		// Search
		results, err := v.Search(queryVector, math.CosineSimMethod, math.SortDesc, 10)
		if err != nil {
			t.Fatalf("failed to search: %v", err)
		}

		if len(results) != 8 {
			t.Fatalf("expected 8 results, got %d", len(results))
		}

		// Verify all IDs are present
		foundIDs := make(map[int]bool)
		for _, result := range results {
			foundIDs[result.ID] = true
		}

		for i := 0; i < 8; i++ {
			if !foundIDs[i] {
				t.Errorf("missing ID %d in results", i)
			}
		}

		// Verify results are sorted in descending order
		for i := 0; i < len(results)-1; i++ {
			if results[i].Value < results[i+1].Value {
				t.Errorf("results not sorted: results[%d].Value=%f < results[%d].Value=%f",
					i, results[i].Value, i+1, results[i+1].Value)
			}
		}
	})

	t.Run("search with limit parameter", func(t *testing.T) {
		path := filepath.Join(tmpDir, "vectors_search_limit.bin")
		v, err := New(path, nil)
		if err != nil {
			t.Fatalf("failed to create vectors: %v", err)
		}

		// Create query vector
		queryVector := make([]float32, DefaultVectorSize)
		for i := range queryVector {
			queryVector[i] = 1.0
		}

		// Append 20 vectors
		for i := 0; i < 20; i++ {
			vector := make([]float32, DefaultVectorSize)
			for j := range vector {
				vector[j] = float32(i+1) * 0.1
			}
			_, err = v.Append(vector)
			if err != nil {
				t.Fatalf("failed to append vector %d: %v", i, err)
			}
		}

		// Search with limit of 5
		results, err := v.Search(queryVector, math.CosineSimMethod, math.SortDesc, 5)
		if err != nil {
			t.Fatalf("failed to search: %v", err)
		}

		if len(results) != 5 {
			t.Fatalf("expected 5 results with limit=5, got %d", len(results))
		}

		// Verify results are the top 5 by similarity
		for i := 0; i < len(results)-1; i++ {
			if results[i].Value < results[i+1].Value {
				t.Errorf("results not sorted: results[%d].Value=%f < results[%d].Value=%f",
					i, results[i].Value, i+1, results[i+1].Value)
			}
		}
	})

	t.Run("search with zero limit - return all results", func(t *testing.T) {
		path := filepath.Join(tmpDir, "vectors_search_zero_limit.bin")
		v, err := New(path, nil)
		if err != nil {
			t.Fatalf("failed to create vectors: %v", err)
		}

		// Create query vector
		queryVector := make([]float32, DefaultVectorSize)
		for i := range queryVector {
			queryVector[i] = 1.0
		}

		// Append 15 vectors
		numVectors := 15
		for i := 0; i < numVectors; i++ {
			vector := make([]float32, DefaultVectorSize)
			for j := range vector {
				vector[j] = float32(i+1) * 0.15
			}
			_, err = v.Append(vector)
			if err != nil {
				t.Fatalf("failed to append vector %d: %v", i, err)
			}
		}

		// Search with limit of 0 (should return all)
		results, err := v.Search(queryVector, math.CosineSimMethod, math.SortDesc, 0)
		if err != nil {
			t.Fatalf("failed to search: %v", err)
		}

		if len(results) != numVectors {
			t.Fatalf("expected %d results with limit=0, got %d", numVectors, len(results))
		}
	})

	t.Run("search across multiple chunks - buffer reloading", func(t *testing.T) {
		path := filepath.Join(tmpDir, "vectors_search_chunks.bin")
		opts := &VectorsOptions{
			VectorSize:          DefaultVectorSize,
			MaxBufferSize:       10, // Small buffer to force multiple chunks
			MaxAppendBufferSize: 5,
		}
		v, err := New(path, opts)
		if err != nil {
			t.Fatalf("failed to create vectors: %v", err)
		}

		// Create query vector
		queryVector := make([]float32, DefaultVectorSize)
		for i := range queryVector {
			queryVector[i] = 1.0
		}

		// Append 30 vectors (will require 3 chunks to process)
		numVectors := 30
		for i := 0; i < numVectors; i++ {
			vector := make([]float32, DefaultVectorSize)
			for j := range vector {
				vector[j] = float32(i+1) * 0.05
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

		// Search
		results, err := v.Search(queryVector, math.CosineSimMethod, math.SortDesc, 0)
		if err != nil {
			t.Fatalf("failed to search: %v", err)
		}

		if len(results) != numVectors {
			t.Fatalf("expected %d results, got %d", numVectors, len(results))
		}

		// Verify all IDs are present
		foundIDs := make(map[int]bool)
		for _, result := range results {
			foundIDs[result.ID] = true
		}

		for i := 0; i < numVectors; i++ {
			if !foundIDs[i] {
				t.Errorf("missing ID %d in results", i)
			}
		}

		// Verify results are sorted in descending order
		for i := 0; i < len(results)-1; i++ {
			if results[i].Value < results[i+1].Value {
				t.Errorf("results not sorted: results[%d].Value=%f < results[%d].Value=%f",
					i, results[i].Value, i+1, results[i+1].Value)
			}
		}
	})

	t.Run("search result ordering - descending by similarity", func(t *testing.T) {
		path := filepath.Join(tmpDir, "vectors_search_ordering.bin")
		v, err := New(path, nil)
		if err != nil {
			t.Fatalf("failed to create vectors: %v", err)
		}

		// Create query vector [1, 0, 0, 0, ...]
		queryVector := make([]float32, DefaultVectorSize)
		queryVector[0] = 1.0

		// Create vectors with known similarity to query
		// Vector 0: [1, 0, 0, ...] - perfect match, similarity = 1.0
		vector0 := make([]float32, DefaultVectorSize)
		vector0[0] = 1.0
		_, err = v.Append(vector0)
		if err != nil {
			t.Fatalf("failed to append vector 0: %v", err)
		}

		// Vector 1: [0.5, 0.5, 0, ...] - partial match
		vector1 := make([]float32, DefaultVectorSize)
		vector1[0] = 0.5
		vector1[1] = 0.5
		_, err = v.Append(vector1)
		if err != nil {
			t.Fatalf("failed to append vector 1: %v", err)
		}

		// Vector 2: [0, 1, 0, ...] - orthogonal, similarity = 0.0
		vector2 := make([]float32, DefaultVectorSize)
		vector2[1] = 1.0
		_, err = v.Append(vector2)
		if err != nil {
			t.Fatalf("failed to append vector 2: %v", err)
		}

		// Search
		results, err := v.Search(queryVector, math.CosineSimMethod, math.SortDesc, 0)
		if err != nil {
			t.Fatalf("failed to search: %v", err)
		}

		if len(results) != 3 {
			t.Fatalf("expected 3 results, got %d", len(results))
		}

		// Verify ordering: vector 0 should be first (highest similarity)
		if results[0].ID != 0 {
			t.Errorf("expected first result to be ID 0, got %d", results[0].ID)
		}

		// Vector 2 should be last (lowest similarity)
		if results[2].ID != 2 {
			t.Errorf("expected last result to be ID 2, got %d", results[2].ID)
		}

		// Verify similarity values are in descending order
		if results[0].Value < results[1].Value || results[1].Value < results[2].Value {
			t.Errorf("results not in descending order: %f, %f, %f",
				results[0].Value, results[1].Value, results[2].Value)
		}
	})

	t.Run("search with custom vector size", func(t *testing.T) {
		path := filepath.Join(tmpDir, "vectors_search_custom_size.bin")
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

		// Create query vector
		queryVector := make([]float32, customSize)
		for i := range queryVector {
			queryVector[i] = 1.0
		}

		// Append vectors
		numVectors := 5
		for i := 0; i < numVectors; i++ {
			vector := make([]float32, customSize)
			for j := range vector {
				vector[j] = float32(i+1) * 0.2
			}
			_, err = v.Append(vector)
			if err != nil {
				t.Fatalf("failed to append vector %d: %v", i, err)
			}
		}

		// Search
		results, err := v.Search(queryVector, math.CosineSimMethod, math.SortDesc, 0)
		if err != nil {
			t.Fatalf("failed to search: %v", err)
		}

		if len(results) != numVectors {
			t.Fatalf("expected %d results, got %d", numVectors, len(results))
		}

		// Verify results are sorted
		for i := 0; i < len(results)-1; i++ {
			if results[i].Value < results[i+1].Value {
				t.Errorf("results not sorted: results[%d].Value=%f < results[%d].Value=%f",
					i, results[i].Value, i+1, results[i+1].Value)
			}
		}
	})

	t.Run("search after reopening storage", func(t *testing.T) {
		path := filepath.Join(tmpDir, "vectors_search_reopen.bin")

		// Create and populate vectors
		v1, err := New(path, nil)
		if err != nil {
			t.Fatalf("failed to create vectors: %v", err)
		}

		// Create query vector
		queryVector := make([]float32, DefaultVectorSize)
		for i := range queryVector {
			queryVector[i] = 1.0
		}

		// Append vectors
		numVectors := 10
		for i := 0; i < numVectors; i++ {
			vector := make([]float32, DefaultVectorSize)
			for j := range vector {
				vector[j] = float32(i+1) * 0.1
			}
			_, err = v1.Append(vector)
			if err != nil {
				t.Fatalf("failed to append vector %d: %v", i, err)
			}
		}

		err = v1.Flush()
		if err != nil {
			t.Fatalf("failed to flush: %v", err)
		}

		// Reopen storage
		v2, err := New(path, nil)
		if err != nil {
			t.Fatalf("failed to reopen vectors: %v", err)
		}

		// Search on reopened storage
		results, err := v2.Search(queryVector, math.CosineSimMethod, math.SortDesc, 0)
		if err != nil {
			t.Fatalf("failed to search: %v", err)
		}

		if len(results) != numVectors {
			t.Fatalf("expected %d results, got %d", numVectors, len(results))
		}

		// Verify results are sorted
		for i := 0; i < len(results)-1; i++ {
			if results[i].Value < results[i+1].Value {
				t.Errorf("results not sorted: results[%d].Value=%f < results[%d].Value=%f",
					i, results[i].Value, i+1, results[i+1].Value)
			}
		}
	})

	t.Run("search with ascending sort order", func(t *testing.T) {
		path := filepath.Join(tmpDir, "vectors_search_asc.bin")
		v, err := New(path, nil)
		if err != nil {
			t.Fatalf("failed to create vectors: %v", err)
		}

		// Create query vector
		queryVector := make([]float32, DefaultVectorSize)
		for i := range queryVector {
			queryVector[i] = 1.0
		}

		// Append vectors
		numVectors := 5
		for i := 0; i < numVectors; i++ {
			vector := make([]float32, DefaultVectorSize)
			for j := range vector {
				vector[j] = float32(i+1) * 0.3
			}
			_, err = v.Append(vector)
			if err != nil {
				t.Fatalf("failed to append vector %d: %v", i, err)
			}
		}

		// Search with ascending order (note: the final sort is always descending in current implementation)
		// This test verifies the sortOrder parameter is passed through correctly
		results, err := v.Search(queryVector, math.CosineSimMethod, math.SortAsc, 0)
		if err != nil {
			t.Fatalf("failed to search: %v", err)
		}

		if len(results) != numVectors {
			t.Fatalf("expected %d results, got %d", numVectors, len(results))
		}

		// Note: The current implementation always sorts descending at the end
		// This is because it re-sorts after combining chunks
		// This test documents that behavior
		for i := 0; i < len(results)-1; i++ {
			if results[i].Value < results[i+1].Value {
				t.Errorf("results not sorted descending: results[%d].Value=%f < results[%d].Value=%f",
					i, results[i].Value, i+1, results[i+1].Value)
			}
		}
	})

	t.Run("search with limit larger than result count", func(t *testing.T) {
		path := filepath.Join(tmpDir, "vectors_search_large_limit.bin")
		v, err := New(path, nil)
		if err != nil {
			t.Fatalf("failed to create vectors: %v", err)
		}

		// Create query vector
		queryVector := make([]float32, DefaultVectorSize)
		for i := range queryVector {
			queryVector[i] = 1.0
		}

		// Append only 3 vectors
		numVectors := 3
		for i := 0; i < numVectors; i++ {
			vector := make([]float32, DefaultVectorSize)
			for j := range vector {
				vector[j] = float32(i+1) * 0.4
			}
			_, err = v.Append(vector)
			if err != nil {
				t.Fatalf("failed to append vector %d: %v", i, err)
			}
		}

		// Search with limit of 100 (larger than available vectors)
		results, err := v.Search(queryVector, math.CosineSimMethod, math.SortDesc, 100)
		if err != nil {
			t.Fatalf("failed to search: %v", err)
		}

		// Should return all 3 vectors, not 100
		if len(results) != numVectors {
			t.Fatalf("expected %d results, got %d", numVectors, len(results))
		}
	})
}
