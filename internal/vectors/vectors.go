package vectors

import (
	"fmt"
	"io"
	"iter"
	"os"

	"github.com/webzak/mindstore/internal/conv"
	"github.com/webzak/mindstore/internal/storage"
)

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
	// appendBuffer holds the vectors to be appended
	appendBuffer [][]float32
	// readerFD is the file descriptor for reading, kept open for reuse
	readerFD *os.File
}

// Options
type Options struct {
	// VectorSize is the size of the float32 vector
	VectorSize int
	// MaxBufferSize is the maximum amount of vectors in memory buffer
	MaxBufferSize int
	// MaxAppendBufferSize is the maximum amount of appended vectors which triggers flush
	MaxAppendBufferSize int
}

// New creates a new vectors storage
func New(path string, opt Options) (*Vectors, error) {
	storage := storage.NewFile(path)
	if err := storage.Init(false); err != nil {
		return nil, err
	}
	// If file doesn't exist yet (lazy creation), Size() will return error - just use 0
	size, _ := storage.Size()
	v := Vectors{
		storage:       storage,
		appendBuffer:  make([][]float32, 0),
		vectorSize:    opt.VectorSize,
		maxBufferSize: opt.MaxBufferSize,
		maxAppendSize: opt.MaxAppendBufferSize,
	}

	// Calculate persistedSize after vectorSize is set
	if v.vectorSize > 0 {
		v.persistedSize = int(size) / (v.vectorSize * conv.Float32Size)
	} else {
		v.persistedSize = 0
	}
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

// Close flushes any unflushed data and closes the reader file descriptor
func (v *Vectors) Close() error {
	// Flush any unflushed data
	if err := v.Flush(); err != nil {
		return err
	}

	// Close the reader file descriptor if it's open
	if v.readerFD != nil {
		if err := v.readerFD.Close(); err != nil {
			return err
		}
		v.readerFD = nil
	}

	return nil
}

// Truncate removes all vectors from storage
func (v *Vectors) Truncate() error {
	// Truncate the storage file to zero size
	if err := v.storage.Truncate(); err != nil {
		return err
	}

	// Clear in-memory buffers
	v.appendBuffer = v.appendBuffer[:0]
	v.persistedSize = 0

	return nil
}

// Get returns a vector at the given position
func (v *Vectors) Get(position int32) ([]float32, error) {
	if position < 0 || int(position) >= v.persistedSize+len(v.appendBuffer) {
		return nil, fmt.Errorf("position out of bounds: %d", position)
	}

	// if position hit the append buffer return the value from append buffer
	if int(position) >= v.persistedSize {
		appendIndex := int(position) - v.persistedSize
		return v.appendBuffer[appendIndex], nil
	}

	if v.readerFD == nil {
		reader, err := v.storage.Reader(0)
		if err != nil {
			return nil, err
		}
		v.readerFD = reader
	}

	// Calculate vector size and offset
	vectorByteSize := v.vectorSize * conv.Float32Size
	byteOffset := int64(position) * int64(vectorByteSize)

	// Seek to the proper offset
	_, err := v.readerFD.Seek(byteOffset, io.SeekStart)
	if err != nil {
		return nil, err
	}

	// Read the vector data
	data := make([]byte, vectorByteSize)
	n, err := io.ReadFull(v.readerFD, data)
	if err != nil {
		return nil, err
	}
	if n != vectorByteSize {
		return nil, fmt.Errorf("expected to read %d bytes, got %d", vectorByteSize, n)
	}

	// Convert bytes to float32 slice (with safe copy)
	result := conv.BytesToFloat32SliceSafe(data)
	return result, nil

}

// Append appends a vector and returns its position
func (v *Vectors) Append(vector []float32) (int32, error) {
	if len(vector) != v.vectorSize {
		return -1, fmt.Errorf("invalid vector length: expected %d, got %d", v.vectorSize, len(vector))
	}

	// calculate position before appending
	position := int32(v.persistedSize + len(v.appendBuffer))

	// add to append buffer
	v.appendBuffer = append(v.appendBuffer, vector)

	// if append buffer is full flush to storage
	if len(v.appendBuffer) >= v.maxAppendSize {
		if err := v.Flush(); err != nil {
			return position, err
		}
	}

	// return the position of appended vector
	return position, nil
}

// Replace replaces a vector at the given position
func (v *Vectors) Replace(position int32, vector []float32) error {
	if len(vector) != v.vectorSize {
		return fmt.Errorf("invalid vector length: expected %d, got %d", v.vectorSize, len(vector))
	}
	if position < 0 || int(position) >= v.persistedSize+len(v.appendBuffer) {
		return fmt.Errorf("position out of bounds: %d", position)
	}
	// if position is less than persisted size, replace the vector in storage
	if int(position) < v.persistedSize {
		vectorByteSize := v.vectorSize * conv.Float32Size
		byteOffset := int64(position) * int64(vectorByteSize)

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
		return nil
	}

	// if the position is inside append buffer, replace there and do immediate flush
	appendIndex := int(position) - v.persistedSize
	copy(v.appendBuffer[appendIndex], vector)
	return v.Flush()
}

// Delete vectors by positions
func (v *Vectors) Delete(positions []int32) error {
	if len(positions) == 0 {
		return nil
	}

	if !v.IsPersisted() {
		if err := v.Flush(); err != nil {
			return err
		}
	}

	// Validate positions and create a set for fast lookup
	deleteSet := make(map[int]bool)
	for _, pos := range positions {
		idx := int(pos)
		if pos < 0 || idx >= v.persistedSize {
			return fmt.Errorf("position out of bounds: %d", pos)
		}
		deleteSet[idx] = true
	}

	// Read all storage data as bytes
	vectorByteSize := v.vectorSize * conv.Float32Size
	totalBytes := v.persistedSize * vectorByteSize

	// Open file descriptor if not already open
	if v.readerFD == nil {
		reader, err := v.storage.Reader(0)
		if err != nil {
			return fmt.Errorf("failed to open reader: %w", err)
		}
		v.readerFD = reader
	}

	// Seek to the beginning
	_, err := v.readerFD.Seek(0, io.SeekStart)
	if err != nil {
		return fmt.Errorf("failed to seek: %w", err)
	}

	allData := make([]byte, totalBytes)
	n, err := io.ReadFull(v.readerFD, allData)
	if err != nil {
		return fmt.Errorf("failed to read storage: %w", err)
	}
	if n != totalBytes {
		return fmt.Errorf("expected to read %d bytes, got %d", totalBytes, n)
	}

	// Truncate the storage to zero
	if err := v.storage.Truncate(); err != nil {
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
	return nil
}

// Iterator returns an iterator over all vectors
func (v *Vectors) Iterator() iter.Seq2[int, []float32] {
	return func(yield func(int, []float32) bool) {
		// Iterate over persisted vectors
		if v.persistedSize > 0 {
			// Open reader if not already open
			if v.readerFD == nil {
				reader, err := v.storage.Reader(0)
				if err != nil {
					return
				}
				v.readerFD = reader
			}

			// Seek to the beginning
			_, err := v.readerFD.Seek(0, io.SeekStart)
			if err != nil {
				return
			}

			vectorByteSize := v.vectorSize * conv.Float32Size
			bufferByteSize := v.maxBufferSize * vectorByteSize
			buffer := make([]byte, bufferByteSize)

			// Iterate over persisted vectors in chunks
			for startIdx := 0; startIdx < v.persistedSize; startIdx += v.maxBufferSize {
				// Calculate how many vectors to read in this chunk
				remaining := v.persistedSize - startIdx
				chunkSize := v.maxBufferSize
				if remaining < chunkSize {
					chunkSize = remaining
				}

				// Read chunk from file
				chunkByteSize := chunkSize * vectorByteSize
				n, err := io.ReadFull(v.readerFD, buffer[:chunkByteSize])
				if err != nil {
					return
				}
				if n != chunkByteSize {
					return
				}

				// Process each vector in the chunk
				for i := 0; i < chunkSize; i++ {
					offset := i * vectorByteSize
					vectorBytes := buffer[offset : offset+vectorByteSize]
					vector := conv.BytesToFloat32SliceSafe(vectorBytes)
					index := startIdx + i
					if !yield(index, vector) {
						return
					}
				}
			}
		}

		// Iterate over append buffer (returning original slices)
		for i, vector := range v.appendBuffer {
			index := v.persistedSize + i
			if !yield(index, vector) {
				return
			}
		}
	}
}
