package vectors

import (
	"fmt"
	"io"

	"github.com/webzak/mindstore/internal/engine/conv"
	"github.com/webzak/mindstore/internal/engine/math"
	"github.com/webzak/mindstore/internal/engine/storage"
	"github.com/webzak/mindstore/internal/types"
)

// Vectors represents the fixed-size vector storage
type Vectors struct {
	storage *storage.File
	// length is the vector length
	length int
	// rows holds the vectors in memory
	rows [][]float32
}

// New creates a new vectors storage
func New(path string, vectorLength int) (*Vectors, error) {
	storage := storage.NewFile(path)
	if err := storage.Init(); err != nil {
		return nil, err
	}
	return &Vectors{
		storage: storage,
		length:  vectorLength,
		rows:    make([][]float32, 0),
	}, nil
}

// Load reads all vectors from storage into memory
func (v *Vectors) Load() (int, error) {
	size, err := v.storage.Size()
	if err != nil {
		return 0, err
	}

	if size == 0 {
		v.rows = make([][]float32, 0)
		return 0, nil
	}

	reader, err := v.storage.Reader(0)
	if err != nil {
		return 0, err
	}
	defer reader.Close()

	data, err := io.ReadAll(reader)
	if err != nil {
		return 0, err
	}

	flat := conv.BytesToFloat32Slice(data)
	count := len(flat) / v.length
	v.rows = make([][]float32, count)
	for i := 0; i < count; i++ {
		start := i * v.length
		end := start + v.length
		// We need to copy here because flat might be reused or GC'd if we were doing something else,
		// but actually conv.BytesToFloat32Slice returns a slice pointing to the same array as data (unsafe) or a copy?
		// conv.BytesToFloat32Slice usually uses unsafe to cast []byte to []float32.
		// If we want to be safe and have independent rows, we should probably copy.
		// However, for now, let's just slice it.
		// Wait, if we slice `flat`, all rows point to the huge `flat` array.
		// If we later append, we might reallocate.
		// If we want to support efficient Append/Replace, having independent slices is better?
		// Actually, having them point to one big array is fine for reading, but if we start modifying (Replace),
		// we might want to copy.
		// Let's copy to be safe and clean.
		row := make([]float32, v.length)
		copy(row, flat[start:end])
		v.rows[i] = row
	}

	return count, nil
}

// Get returns a vector at the given index from memory
func (v *Vectors) Get(index int) ([]float32, error) {
	if index < 0 || index >= len(v.rows) {
		return nil, fmt.Errorf("index out of bounds: %d", index)
	}
	// Return a copy to prevent external modification of internal state
	result := make([]float32, v.length)
	copy(result, v.rows[index])
	return result, nil
}

// Append appends a vector to memory and returns its index
func (v *Vectors) Append(vector []float32) (int, error) {
	if len(vector) != v.length {
		return 0, fmt.Errorf("invalid vector length: expected %d, got %d", v.length, len(vector))
	}

	// Copy the vector to ensure we own the memory
	newRow := make([]float32, v.length)
	copy(newRow, vector)
	v.rows = append(v.rows, newRow)
	return len(v.rows) - 1, nil
}

// Replace replaces a vector at the given index in memory
func (v *Vectors) Replace(index int, vector []float32) error {
	if len(vector) != v.length {
		return fmt.Errorf("invalid vector length: expected %d, got %d", v.length, len(vector))
	}
	if index < 0 || index >= len(v.rows) {
		return fmt.Errorf("index out of bounds: %d", index)
	}

	// Copy the vector
	copy(v.rows[index], vector)
	return nil
}

// Count returns the number of vectors in memory
func (v *Vectors) Count() int {
	return len(v.rows)
}

// Search performs similarity search on the vectors using the specified method
// Returns a slice of Distance structs where ID is the vector index
func (v *Vectors) Search(vector []float32, method types.VectorSearchMethod, limit int) ([]math.Distance, error) {
	if len(vector) != v.length {
		return nil, fmt.Errorf("invalid vector length: expected %d, got %d", v.length, len(vector))
	}

	if limit < 0 {
		return nil, fmt.Errorf("invalid limit: expected positive, got %d", limit)
	}

	switch method {
	case types.CosineSimMethod:
		// Use descending order (highest similarity first) with no limit (0)
		return math.CosineSimRanking(v.rows, vector, types.SortDesc, limit)
	default:
		return nil, fmt.Errorf("unsupported search method: %d", method)
	}
}

// Flush saves the in-memory vectors to storage
func (v *Vectors) Flush() error {
	writer, err := v.storage.Writer(0)
	if err != nil {
		return err
	}
	defer writer.Close()

	// Flatten rows
	totalSize := len(v.rows) * v.length
	flat := make([]float32, 0, totalSize)
	for _, row := range v.rows {
		flat = append(flat, row...)
	}

	data := conv.Float32SliceToByte(flat)
	_, err = writer.Write(data)
	return err
}
