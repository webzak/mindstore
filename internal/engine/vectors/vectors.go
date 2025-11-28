package vectors

import (
	"fmt"
	"io"
	"iter"

	//"slices"

	"github.com/webzak/mindstore/internal/engine/conv"
	"github.com/webzak/mindstore/internal/engine/storage"
)

const (
	DefaultVectorSize          = 768
	DefaultMaxBufferSize       = 64
	DefaultMaxAppendBufferSize = 64
)

type memBuffer struct {
	start int
	data  []byte // flat data to prevent GC of cleaning up rows
	rows  [][]float32
}

// VectorsOptions
type VectorsOptions struct {
	// VectorSize is the size of the float32 vector
	VectorSize int
	// MaxBufferSize is the maximum amount of vectors in memory buffer
	MaxBufferSize int
	// MaxAppendBufferSize is the maximum amount of appended vectors which triggers flush
	MaxAppendBufferSize int
}

// DefaultVectorOptions return vector defaults
func DefaultVectorsOptions() VectorsOptions {
	return VectorsOptions{
		VectorSize:          DefaultVectorSize,
		MaxBufferSize:       DefaultMaxBufferSize,
		MaxAppendBufferSize: DefaultMaxAppendBufferSize,
	}
}

// Vectors represents the fixed-size vector storage
type Vectors struct {
	storage *storage.File
	// the actual amount persited
	persistedSize int
	// vectorSize is the size of the float32 vector
	vectorSize int
	// maxBufferSize is the maximum size of memory buffer
	maxBufferSize int
	// maxAppendSize is the maximum sile of append buffer
	maxAppendSize int
	// buffer holds the vectors in memory
	buffer memBuffer
	// appendBuffer holds the vectors to be appended
	appendBuffer [][]float32
}

// New creates a new vectors storage
func New(path string, opt *VectorsOptions) (*Vectors, error) {
	storage := storage.NewFile(path)
	if err := storage.Init(); err != nil {
		return nil, err
	}
	size, err := storage.Size()
	if err != nil {
		return nil, err
	}
	v := Vectors{
		storage:       storage,
		appendBuffer:  make([][]float32, 0),
		vectorSize:    DefaultVectorSize,
		maxBufferSize: DefaultMaxBufferSize,
		maxAppendSize: DefaultMaxAppendBufferSize,
	}
	if opt != nil {
		v.vectorSize = opt.VectorSize
		v.maxBufferSize = opt.MaxBufferSize
		v.maxAppendSize = opt.MaxAppendBufferSize
	}
	// Calculate persistedSize after vectorSize is set
	v.persistedSize = int(size) / (v.vectorSize * conv.Float32Size)
	return &v, nil
}

// Count returns the number of vectors
func (v *Vectors) Count() int {
	return v.persistedSize + len(v.appendBuffer)
}

// Flush saves not saved vectors from append buffer to storage
func (v *Vectors) Flush() error {
	if len(v.appendBuffer) == 0 {
		return nil
	}
	appender, err := v.storage.Appender()
	if err != nil {
		return err
	}
	defer appender.Close()

	size := len(v.appendBuffer)
	for _, row := range v.appendBuffer {
		data := conv.Float32SliceToByte(row)
		_, err = appender.Write(data)
		if err != nil {
			return err
		}
	}
	v.appendBuffer = make([][]float32, 0)
	v.persistedSize += size
	return nil
}

// IsPersisted returns true if there are no pending writes
func (v *Vectors) IsPersisted() bool {
	return len(v.appendBuffer) == 0
}

// Get returns a vector at the given index
func (v *Vectors) Get(index int) ([]float32, error) {
	if index < 0 || index >= v.persistedSize+len(v.appendBuffer) {
		return nil, fmt.Errorf("index out of bounds: %d", index)
	}

	// if index hit the memory buffer return the value
	if len(v.buffer.rows) > 0 && index >= v.buffer.start && index < v.buffer.start+len(v.buffer.rows) {
		bufferIndex := index - v.buffer.start
		result := make([]float32, v.vectorSize)
		copy(result, v.buffer.rows[bufferIndex])
		return result, nil
	}

	// if index hit the persisted size return the value from storage
	if index < v.persistedSize {
		// Read single vector directly from storage
		vectorByteSize := v.vectorSize * conv.Float32Size
		byteOffset := int64(index * vectorByteSize)

		reader, err := v.storage.Reader(byteOffset)
		if err != nil {
			return nil, err
		}
		defer reader.Close()

		data := make([]byte, vectorByteSize)
		n, err := io.ReadFull(reader, data)
		if err != nil {
			return nil, err
		}
		if n != vectorByteSize {
			return nil, fmt.Errorf("expected to read %d bytes, got %d", vectorByteSize, n)
		}

		// Convert bytes to float32 slice
		flat := conv.BytesToFloat32Slice(data)
		result := make([]float32, v.vectorSize)
		copy(result, flat)
		return result, nil
	}

	// if index hit the append buffer return the value from append buffer
	appendIndex := index - v.persistedSize
	result := make([]float32, v.vectorSize)
	copy(result, v.appendBuffer[appendIndex])
	return result, nil
}

// Append appends a vector
func (v *Vectors) Append(index int, vector []float32) error {
	if len(vector) != v.vectorSize {
		return fmt.Errorf("invalid vector length: expected %d, got %d", v.vectorSize, len(vector))
	}

	// calculate expected index before appending
	expectedIndex := v.persistedSize + len(v.appendBuffer)

	// verify index integrity
	if index != expectedIndex {
		return fmt.Errorf("index integrity error: expected %d, got %d", expectedIndex, index)
	}
	// add to append buffer
	v.appendBuffer = append(v.appendBuffer, vector)

	// if append buffer is full flush to storage
	if len(v.appendBuffer) >= v.maxAppendSize {
		if err := v.Flush(); err != nil {
			return err
		}
	}

	// return the index of appended vector and error if any
	return nil
}

// Replace replaces a vector at the given index
func (v *Vectors) Replace(index int, vector []float32) error {
	if len(vector) != v.vectorSize {
		return fmt.Errorf("invalid vector length: expected %d, got %d", v.vectorSize, len(vector))
	}
	if index < 0 || index >= v.persistedSize+len(v.appendBuffer) {
		return fmt.Errorf("index out of bounds: %d", index)
	}
	// if index is less than persisted size, replace the vector in storage
	if index < v.persistedSize {
		vectorByteSize := v.vectorSize * conv.Float32Size
		byteOffset := int64(index * vectorByteSize)

		writer, err := v.storage.Writer(byteOffset)
		if err != nil {
			return err
		}
		defer writer.Close()

		data := conv.Float32SliceToByte(vector)
		_, err = writer.Write(data)
		if err != nil {
			return err
		}

		// if index is also inside memory buffer, replace the vector there too
		if len(v.buffer.rows) > 0 && index >= v.buffer.start && index < v.buffer.start+len(v.buffer.rows) {
			bufferIndex := index - v.buffer.start
			copy(v.buffer.rows[bufferIndex], vector)
		}

		return nil
	}

	// if the index is inside append buffer, replace there and do immediate flush
	appendIndex := index - v.persistedSize
	copy(v.appendBuffer[appendIndex], vector)
	return v.Flush()
}

// Delete vectors by indexes
func (v *Vectors) Delete(indexes []int) error {
	if len(indexes) == 0 {
		return nil
	}

	if !v.IsPersisted() {
		if err := v.Flush(); err != nil {
			return err
		}
	}

	// Validate indexes and create a set for fast lookup
	deleteSet := make(map[int]bool)
	for _, idx := range indexes {
		if idx < 0 || idx >= v.persistedSize {
			return fmt.Errorf("index out of bounds: %d", idx)
		}
		deleteSet[idx] = true
	}

	// Read all storage data as bytes
	vectorByteSize := v.vectorSize * conv.Float32Size
	totalBytes := v.persistedSize * vectorByteSize

	reader, err := v.storage.Reader(0)
	if err != nil {
		return fmt.Errorf("failed to open reader: %w", err)
	}
	defer reader.Close()

	allData := make([]byte, totalBytes)
	n, err := io.ReadFull(reader, allData)
	if err != nil {
		return fmt.Errorf("failed to read storage: %w", err)
	}
	if n != totalBytes {
		return fmt.Errorf("expected to read %d bytes, got %d", totalBytes, n)
	}

	// Truncate the storage to zero
	if err := v.storage.Truncate(0); err != nil {
		return fmt.Errorf("failed to truncate storage: %w", err)
	}

	// Write back only non-deleted vectors
	newSize := v.persistedSize - len(deleteSet)
	if newSize > 0 {
		appender, err := v.storage.Appender()
		if err != nil {
			return fmt.Errorf("failed to open appender: %w", err)
		}
		defer appender.Close()

		// Iterate over vectors and write non-deleted ones
		for i := 0; i < v.persistedSize; i++ {
			if !deleteSet[i] {
				// Calculate byte offset for this vector
				offset := i * vectorByteSize
				vectorData := allData[offset : offset+vectorByteSize]
				_, err = appender.Write(vectorData)
				if err != nil {
					return fmt.Errorf("failed to write vector: %w", err)
				}
			}
		}
	}

	// Update persisted size
	v.persistedSize = newSize

	// Clean the vectors buffer
	v.buffer = memBuffer{}

	return nil
}

// Search performs similarity search on the vectors using the specified method
// Returns a slice of Distance structs where ID is the vector index
// func (v *Vectors) Search(vector []float32, method math.VectorSearchMethod, sortOrder math.SortOrder, limit int) ([]math.Distance, error) {
// 	if len(vector) != v.vectorSize {
// 		return nil, fmt.Errorf("invalid vector length: expected %d, got %d", v.vectorSize, len(vector))
// 	}

// 	if limit < 0 {
// 		return nil, fmt.Errorf("invalid limit: expected positive, got %d", limit)
// 	}

// 	// Select the appropriate ranking function based on the search method
// 	var rankFunc math.RankingFunc
// 	switch method {
// 	case math.CosineSimMethod:
// 		rankFunc = math.CosineSimRanking
// 	default:
// 		return nil, fmt.Errorf("unsupported search method: %d", method)
// 	}

// 	return v.search(vector, rankFunc, sortOrder, limit)
// }

// search performs similarity search across all vectors using the provided ranking function
// It processes persisted vectors in chunks and includes appendBuffer vectors
// func (v *Vectors) search(vector []float32, rankFunc math.RankingFunc, sortOrder math.SortOrder, limit int) ([]math.Distance, error) {
// 	allResults := make([]math.Distance, 0)

// 	// Process persisted vectors in chunks
// 	offset := 0
// 	for offset < v.persistedSize {
// 		// Check if we need to load a new chunk into v.buffer
// 		// Skip loading if buffer is already at the correct offset with data
// 		if !(v.buffer.start == offset && len(v.buffer.rows) > 0) {
// 			// Determine chunk size
// 			chunkSize := v.maxBufferSize
// 			if offset+chunkSize > v.persistedSize {
// 				chunkSize = v.persistedSize - offset
// 			}

// 			// Load chunk from storage into v.buffer
// 			var err error
// 			v.buffer, err = v.loadBuffer(offset, chunkSize)
// 			if err != nil {
// 				return nil, fmt.Errorf("failed to load buffer at offset %d: %w", offset, err)
// 			}
// 		}

// 		// Calculate similarity/distance for this chunk using the provided ranking function
// 		chunkResults, err := rankFunc(v.buffer.rows, vector, sortOrder, 0)
// 		if err != nil {
// 			return nil, err
// 		}

// 		// Adjust IDs to account for offset
// 		for i := range chunkResults {
// 			chunkResults[i].ID += offset
// 		}

// 		// Accumulate results
// 		allResults = append(allResults, chunkResults...)

// 		offset += len(v.buffer.rows)
// 	}

// 	// Process appendBuffer if not empty
// 	if len(v.appendBuffer) > 0 {
// 		appendResults, err := rankFunc(v.appendBuffer, vector, sortOrder, 0)
// 		if err != nil {
// 			return nil, err
// 		}

// 		// Adjust IDs to account for persisted size
// 		for i := range appendResults {
// 			appendResults[i].ID += v.persistedSize
// 		}

// 		// Accumulate append buffer results
// 		allResults = append(allResults, appendResults...)
// 	}

// 	// Sort all results by similarity (descending)
// 	// We need to re-sort since we've combined multiple chunks
// 	slices.SortFunc(allResults, func(a, b math.Distance) int {
// 		if a.Value > b.Value {
// 			return -1 // a comes before b (descending order)
// 		} else if a.Value < b.Value {
// 			return 1 // b comes before a
// 		}
// 		return 0 // equal
// 	})

// 	// Apply limit if specified
// 	if limit > 0 && len(allResults) > limit {
// 		return allResults[:limit], nil
// 	}

// 	return allResults, nil
// }

// loadBuffer loads a specific range of vectors from storage into a buffer
// start: the index of the first vector to load
// amount: the number of vectors to load
// Returns a buffer containing the requested vectors with zero-copy conversion
func (v *Vectors) loadBuffer(start, amount int) (memBuffer, error) {
	if start < 0 {
		return memBuffer{}, fmt.Errorf("start index cannot be negative: %d", start)
	}
	if amount <= 0 {
		return memBuffer{}, fmt.Errorf("amount must be positive: %d", amount)
	}

	// Calculate byte positions
	vectorByteSize := v.vectorSize * conv.Float32Size
	byteStart := start * vectorByteSize
	byteAmount := amount * vectorByteSize

	// Read the specific range of bytes from storage
	reader, err := v.storage.Reader(int64(byteStart))
	if err != nil {
		return memBuffer{}, err
	}
	defer reader.Close()

	data := make([]byte, byteAmount)
	n, err := io.ReadFull(reader, data)
	if err != nil {
		return memBuffer{}, err
	}
	if n != byteAmount {
		return memBuffer{}, fmt.Errorf("expected to read %d bytes, got %d", byteAmount, n)
	}

	// Convert bytes to float32 slice using zero-copy unsafe conversion
	flat := conv.BytesToFloat32Slice(data)

	// Create rows by slicing the flat array (no copying)
	rows := make([][]float32, amount)
	for i := 0; i < amount; i++ {
		rowStart := i * v.vectorSize
		rowEnd := rowStart + v.vectorSize
		rows[i] = flat[rowStart:rowEnd]
	}

	return memBuffer{
		start: start,
		data:  data, // Keep reference to prevent GC
		rows:  rows,
	}, nil
}

// Iterator returns an iterator over all vectors (persisted + append buffer) that yields (index, vector) pairs.
// This uses Go 1.23's iter.Seq2 for idiomatic range-over-function iteration.
//
// Example usage:
//
//	for index, vector := range vectors.Iterator() {
//	    // use index and vector
//	}
//
// Note: The returned vectors are copies to prevent mutations from affecting stored data.
func (v *Vectors) Iterator() iter.Seq2[int, []float32] {
	return func(yield func(int, []float32) bool) {
		// Iterate over persisted vectors
		for i := 0; i < v.persistedSize; i++ {
			vector, err := v.Get(i)
			if err != nil {
				// Skip vectors that can't be read
				continue
			}
			if !yield(i, vector) {
				return
			}
		}

		// Iterate over append buffer
		for i, vec := range v.appendBuffer {
			// Create a copy to prevent mutations
			vector := make([]float32, len(vec))
			copy(vector, vec)
			if !yield(v.persistedSize+i, vector) {
				return
			}
		}
	}
}
