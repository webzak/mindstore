package vectors

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/webzak/mindstore/internal/testutil"
	"github.com/webzak/mindstore/internal/types"
)

func TestVectors(t *testing.T) {
	dir := testutil.MakeTempDir(t)
	defer os.RemoveAll(dir)

	path := filepath.Join(dir, "vectors.bin")
	vectorLength := 3
	v, err := New(path, vectorLength)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	// Test Append (In-Memory)
	vec1 := []float32{1.0, 2.0, 3.0}
	idx1, err := v.Append(vec1)
	if err != nil {
		t.Fatalf("Append() error = %v", err)
	}
	if idx1 != 0 {
		t.Errorf("Append() index = %d, want 0", idx1)
	}

	vec2 := []float32{4.0, 5.0, 6.0}
	idx2, err := v.Append(vec2)
	if err != nil {
		t.Fatalf("Append() error = %v", err)
	}
	if idx2 != 1 {
		t.Errorf("Append() index = %d, want 1", idx2)
	}

	// Test Count (In-Memory)
	count := v.Count()
	if count != 2 {
		t.Errorf("Count() = %d, want 2", count)
	}

	// Test Get (In-Memory)
	got1, err := v.Get(0)
	if err != nil {
		t.Fatalf("Get(0) error = %v", err)
	}
	if !slicesEqual(got1, vec1) {
		t.Errorf("Get(0) = %v, want %v", got1, vec1)
	}

	got2, err := v.Get(1)
	if err != nil {
		t.Fatalf("Get(1) error = %v", err)
	}
	if !slicesEqual(got2, vec2) {
		t.Errorf("Get(1) = %v, want %v", got2, vec2)
	}

	// Test Replace (In-Memory)
	vec3 := []float32{7.0, 8.0, 9.0}
	err = v.Replace(0, vec3)
	if err != nil {
		t.Fatalf("Replace(0) error = %v", err)
	}

	got3, err := v.Get(0)
	if err != nil {
		t.Fatalf("Get(0) error = %v", err)
	}
	if !slicesEqual(got3, vec3) {
		t.Errorf("Get(0) = %v, want %v", got3, vec3)
	}

	// Test Flush (Persistence)
	err = v.Flush()
	if err != nil {
		t.Fatalf("Flush() error = %v", err)
	}

	// Re-open and Load
	v2, err := New(path, vectorLength)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	loaded, err := v2.Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if loaded != 2 {
		t.Errorf("Load() loaded = %d, want 2", loaded)
	}

	// Verify loaded data
	got3Loaded, err := v2.Get(0)
	if err != nil {
		t.Fatalf("Get(0) error = %v", err)
	}
	if !slicesEqual(got3Loaded, vec3) {
		t.Errorf("Get(0) = %v, want %v", got3Loaded, vec3)
	}

	got2Loaded, err := v2.Get(1)
	if err != nil {
		t.Fatalf("Get(1) error = %v", err)
	}
	if !slicesEqual(got2Loaded, vec2) {
		t.Errorf("Get(1) = %v, want %v", got2Loaded, vec2)
	}

	// Test Invalid Length
	err = v.Replace(0, []float32{1.0})
	if err == nil {
		t.Error("Replace() expected error for invalid length")
	}

	_, err = v.Append([]float32{1.0})
	if err == nil {
		t.Error("Append() expected error for invalid length")
	}
}

func TestSearch(t *testing.T) {
	dir := testutil.MakeTempDir(t)
	defer os.RemoveAll(dir)

	path := filepath.Join(dir, "vectors.bin")
	vectorLength := 3
	v, err := New(path, vectorLength)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	// Add test vectors
	vec1 := []float32{1.0, 0.0, 0.0} // along x-axis
	vec2 := []float32{0.0, 1.0, 0.0} // along y-axis
	vec3 := []float32{1.0, 1.0, 0.0} // 45 degrees from x-axis
	vec4 := []float32{2.0, 0.0, 0.0} // along x-axis (same direction as vec1)

	_, err = v.Append(vec1)
	if err != nil {
		t.Fatalf("Append(vec1) error = %v", err)
	}
	_, err = v.Append(vec2)
	if err != nil {
		t.Fatalf("Append(vec2) error = %v", err)
	}
	_, err = v.Append(vec3)
	if err != nil {
		t.Fatalf("Append(vec3) error = %v", err)
	}
	_, err = v.Append(vec4)
	if err != nil {
		t.Fatalf("Append(vec4) error = %v", err)
	}

	// Test 1: Search with vector similar to vec1
	query1 := []float32{1.0, 0.0, 0.0}
	results, err := v.Search(query1, types.CosineSimMethod, 0)
	if err != nil {
		t.Fatalf("Search() error = %v", err)
	}

	// Should return all 4 vectors, sorted by similarity (descending)
	if len(results) != 4 {
		t.Errorf("Search() returned %d results, want 4", len(results))
	}

	// vec1 and vec4 should be most similar (both along x-axis)
	// Their IDs should be in the top 2 positions
	if results[0].ID != 0 && results[0].ID != 3 {
		t.Errorf("Search() top result ID = %d, want 0 or 3", results[0].ID)
	}
	if results[1].ID != 0 && results[1].ID != 3 {
		t.Errorf("Search() second result ID = %d, want 0 or 3", results[1].ID)
	}

	// Verify similarity values are in descending order
	for i := 0; i < len(results)-1; i++ {
		if results[i].Value < results[i+1].Value {
			t.Errorf("Search() results not sorted: results[%d].Value = %f < results[%d].Value = %f",
				i, results[i].Value, i+1, results[i+1].Value)
		}
	}

	// Test 2: Search with invalid vector length
	invalidQuery := []float32{1.0, 2.0}
	_, err = v.Search(invalidQuery, types.CosineSimMethod, 0)
	if err == nil {
		t.Error("Search() expected error for invalid vector length")
	}

	// Test 3: Search with unsupported method
	validQuery := []float32{1.0, 0.0, 0.0}
	_, err = v.Search(validQuery, types.VectorSearchMethod(999), 0)
	if err == nil {
		t.Error("Search() expected error for unsupported search method")
	}

	// Test 4: Verify Position field matches ID
	for i, result := range results {
		if result.Position != result.ID {
			t.Errorf("Search() results[%d].Position = %d, want %d (should match ID)",
				i, result.Position, result.ID)
		}
	}
}

func slicesEqual(a, b []float32) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
