package math

import (
	"math"
	"testing"

	"github.com/webzak/mindstore/internal/types"
)

// TestCosineSim tests the CosineSim function with various cases
func TestCosineSim(t *testing.T) {
	tests := []struct {
		name     string
		a        []float32
		b        []float32
		expected float32
		epsilon  float32 // tolerance for floating point comparison
	}{
		{
			name:     "identical vectors",
			a:        []float32{1.0, 2.0, 3.0},
			b:        []float32{1.0, 2.0, 3.0},
			expected: 1.0,
			epsilon:  0.0001,
		},
		{
			name:     "opposite vectors",
			a:        []float32{1.0, 2.0, 3.0},
			b:        []float32{-1.0, -2.0, -3.0},
			expected: -1.0,
			epsilon:  0.0001,
		},
		{
			name:     "orthogonal vectors",
			a:        []float32{1.0, 0.0},
			b:        []float32{0.0, 1.0},
			expected: 0.0,
			epsilon:  0.0001,
		},
		{
			name:     "zero vector a",
			a:        []float32{0.0, 0.0, 0.0},
			b:        []float32{1.0, 2.0, 3.0},
			expected: 0.0,
			epsilon:  0.0001,
		},
		{
			name:     "zero vector b",
			a:        []float32{1.0, 2.0, 3.0},
			b:        []float32{0.0, 0.0, 0.0},
			expected: 0.0,
			epsilon:  0.0001,
		},
		{
			name:     "both zero vectors",
			a:        []float32{0.0, 0.0, 0.0},
			b:        []float32{0.0, 0.0, 0.0},
			expected: 0.0,
			epsilon:  0.0001,
		},
		{
			name:     "normalized vectors at 45 degrees",
			a:        []float32{1.0, 0.0},
			b:        []float32{1.0, 1.0},
			expected: float32(1.0 / math.Sqrt(2.0)), // cos(45°) ≈ 0.707
			epsilon:  0.0001,
		},
		{
			name:     "general case",
			a:        []float32{1.0, 2.0, 3.0, 4.0},
			b:        []float32{5.0, 6.0, 7.0, 8.0},
			expected: 0.9688, // calculated value
			epsilon:  0.0001,
		},
		{
			name:     "negative and positive values",
			a:        []float32{-1.0, 2.0, -3.0},
			b:        []float32{4.0, -5.0, 6.0},
			expected: -0.9746, // calculated value
			epsilon:  0.0001,
		},
		{
			name:     "small values",
			a:        []float32{0.001, 0.002, 0.003},
			b:        []float32{0.004, 0.005, 0.006},
			expected: 0.9746, // calculated value
			epsilon:  0.0001,
		},
		{
			name:     "large values",
			a:        []float32{1000.0, 2000.0, 3000.0},
			b:        []float32{4000.0, 5000.0, 6000.0},
			expected: 0.9746, // calculated value
			epsilon:  0.0001,
		},
		{
			name:     "single element vectors",
			a:        []float32{5.0},
			b:        []float32{10.0},
			expected: 1.0,
			epsilon:  0.0001,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := CosineSim(tt.a, tt.b)
			if math.Abs(float64(result-tt.expected)) > float64(tt.epsilon) {
				t.Errorf("CosineSim(%v, %v) = %v, expected %v (±%v)", tt.a, tt.b, result, tt.expected, tt.epsilon)
			}
		})
	}
}

// TestCosineSimRanking tests the CosineSimRanking function
func TestCosineSimRanking(t *testing.T) {
	t.Run("basic ranking descending", func(t *testing.T) {
		rows := [][]float32{
			{1.0, 0.0, 0.0},
			{0.0, 1.0, 0.0},
			{1.0, 1.0, 0.0},
			{1.0, 0.5, 0.0},
		}
		vector := []float32{1.0, 0.0, 0.0}

		result, err := CosineSimRanking(rows, vector, types.SortDesc, 0)
		if err != nil {
			t.Fatalf("CosineSimRanking returned error: %v", err)
		}

		if len(result) != 4 {
			t.Fatalf("Expected 4 results, got %d", len(result))
		}

		// ID 0 and 3 should be at the top (highest similarity)
		if result[0].ID != 0 && result[0].ID != 3 {
			t.Errorf("Expected ID 0 or 3 at position 0, got %d", result[0].ID)
		}

		// Result should be in descending order
		for i := 0; i < len(result)-1; i++ {
			if result[i].Value < result[i+1].Value {
				t.Errorf("Results not in descending order: %v >= %v failed at index %d", result[i].Value, result[i+1].Value, i)
			}
		}
	})

	t.Run("basic ranking ascending", func(t *testing.T) {
		rows := [][]float32{
			{1.0, 0.0, 0.0},
			{0.0, 1.0, 0.0},
			{1.0, 1.0, 0.0},
		}
		vector := []float32{1.0, 0.0, 0.0}

		result, err := CosineSimRanking(rows, vector, types.SortAsc, 0)
		if err != nil {
			t.Fatalf("CosineSimRanking returned error: %v", err)
		}

		if len(result) != 3 {
			t.Fatalf("Expected 3 results, got %d", len(result))
		}

		// Result should be in ascending order
		for i := 0; i < len(result)-1; i++ {
			if result[i].Value > result[i+1].Value {
				t.Errorf("Results not in ascending order: %v <= %v failed at index %d", result[i].Value, result[i+1].Value, i)
			}
		}
	})

	t.Run("with limit", func(t *testing.T) {
		rows := [][]float32{
			{1.0, 0.0},
			{0.0, 1.0},
			{1.0, 1.0},
			{2.0, 2.0},
			{3.0, 3.0},
		}
		vector := []float32{1.0, 0.0}

		result, err := CosineSimRanking(rows, vector, types.SortDesc, 2)
		if err != nil {
			t.Fatalf("CosineSimRanking returned error: %v", err)
		}

		if len(result) != 2 {
			t.Fatalf("Expected 2 results (limited), got %d", len(result))
		}
	})

	t.Run("limit larger than results", func(t *testing.T) {
		rows := [][]float32{
			{1.0, 0.0},
			{0.0, 1.0},
		}
		vector := []float32{1.0, 0.0}

		result, err := CosineSimRanking(rows, vector, types.SortDesc, 10)
		if err != nil {
			t.Fatalf("CosineSimRanking returned error: %v", err)
		}

		if len(result) != 2 {
			t.Fatalf("Expected 2 results, got %d", len(result))
		}
	})

	t.Run("zero limit returns all", func(t *testing.T) {
		rows := [][]float32{
			{1.0, 0.0},
			{0.0, 1.0},
			{1.0, 1.0},
		}
		vector := []float32{1.0, 0.0}

		result, err := CosineSimRanking(rows, vector, types.SortDesc, 0)
		if err != nil {
			t.Fatalf("CosineSimRanking returned error: %v", err)
		}

		if len(result) != 3 {
			t.Fatalf("Expected 3 results, got %d", len(result))
		}
	})

	t.Run("vector size mismatch error", func(t *testing.T) {
		rows := [][]float32{
			{1.0, 0.0, 0.0},
			{0.0, 1.0},
		}
		vector := []float32{1.0, 0.0, 0.0}

		result, err := CosineSimRanking(rows, vector, types.SortDesc, 0)
		if err == nil {
			t.Fatalf("Expected error for size mismatch, got nil")
		}
		if result != nil {
			t.Errorf("Expected nil result on error, got %v", result)
		}
	})

	t.Run("empty rows", func(t *testing.T) {
		rows := [][]float32{}
		vector := []float32{1.0, 0.0}

		result, err := CosineSimRanking(rows, vector, types.SortDesc, 0)
		if err != nil {
			t.Fatalf("CosineSimRanking returned error: %v", err)
		}

		if len(result) != 0 {
			t.Fatalf("Expected 0 results for empty rows, got %d", len(result))
		}
	})

	t.Run("distance struct fields", func(t *testing.T) {
		rows := [][]float32{
			{1.0, 0.0},
			{0.0, 1.0},
		}
		vector := []float32{1.0, 0.0}

		result, err := CosineSimRanking(rows, vector, types.SortDesc, 0)
		if err != nil {
			t.Fatalf("CosineSimRanking returned error: %v", err)
		}

		// Check that ID and Position are set correctly
		for i, dist := range result {
			if dist.ID < 0 || dist.ID >= len(rows) {
				t.Errorf("Invalid ID %d at result index %d", dist.ID, i)
			}
			if dist.Position < 0 || dist.Position >= len(rows) {
				t.Errorf("Invalid Position %d at result index %d", dist.Position, i)
			}
		}
	})

	t.Run("ensure values are correct", func(t *testing.T) {
		rows := [][]float32{
			{1.0, 0.0},
		}
		vector := []float32{1.0, 0.0}

		result, err := CosineSimRanking(rows, vector, types.SortDesc, 0)
		if err != nil {
			t.Fatalf("CosineSimRanking returned error: %v", err)
		}

		if len(result) != 1 {
			t.Fatalf("Expected 1 result, got %d", len(result))
		}

		// Cosine similarity should be 1.0 for identical vectors
		epsilon := float32(0.0001)
		if math.Abs(float64(result[0].Value-1.0)) > float64(epsilon) {
			t.Errorf("Expected Value 1.0, got %v", result[0].Value)
		}
	})
}

// BenchmarkCosineSim benchmarks the CosineSim function
func BenchmarkCosineSim(b *testing.B) {
	a := make([]float32, 128)
	vec := make([]float32, 128)
	for i := range a {
		a[i] = float32(i)
		vec[i] = float32(i * 2)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		CosineSim(a, vec)
	}
}

// BenchmarkCosineSimRanking benchmarks the CosineSimRanking function
func BenchmarkCosineSimRanking(b *testing.B) {
	rows := make([][]float32, 1000)
	for i := range rows {
		rows[i] = make([]float32, 128)
		for j := range rows[i] {
			rows[i][j] = float32(i*j) / 100.0
		}
	}
	vector := make([]float32, 128)
	for i := range vector {
		vector[i] = float32(i)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		CosineSimRanking(rows, vector, types.SortDesc, 10)
	}
}
