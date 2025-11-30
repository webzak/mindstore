package data

import (
	"fmt"
	"io"
	"os"

	"github.com/webzak/mindstore/internal/storage"
)

// Options are options for data storage
type Options struct {
	MaxAppendBufferSize int64
}

// Data represents the raw data storage
type Data struct {
	storage *storage.File
	// readerFD is the file descriptor for reading, kept open for reuse
	readerFD *os.File
	// persistedSize is the total bytes written to disk
	persistedSize int64
	// maxAppendBufferSize is the buffer threshold in bytes
	maxAppendBufferSize int64
	// appendBuffer is the in-memory buffer for staged data
	appendBuffer []byte
	// bufferOffsets tracks start offset of each buffered record
	bufferOffsets []int64
}

// New creates a new data storage
func New(path string, opt Options) (*Data, error) {
	storage := storage.NewFile(path)
	if err := storage.Init(); err != nil {
		return nil, err
	}

	// Calculate persistedSize from file
	persistedSize, err := storage.Size()
	if err != nil {
		return nil, err
	}

	return &Data{
		storage:             storage,
		persistedSize:       persistedSize,
		maxAppendBufferSize: opt.MaxAppendBufferSize,
		appendBuffer:        make([]byte, 0, opt.MaxAppendBufferSize),
		bufferOffsets:       make([]int64, 0, 64), // Preallocate for ~64 records
	}, nil
}

// Flush flushes the append buffer to storage
func (d *Data) Flush() error {
	// Nothing to flush if buffer is empty
	if len(d.appendBuffer) == 0 {
		return nil
	}

	// Get appender handle
	appender, err := d.storage.Appender()
	if err != nil {
		return err
	}
	defer appender.Close()

	// Write entire buffer to disk
	n, err := appender.Write(d.appendBuffer)
	if err != nil {
		return err
	}

	// Update persisted size
	d.persistedSize += int64(n)

	// Clear buffer and offsets (preserve capacity to avoid reallocations)
	d.appendBuffer = d.appendBuffer[:0]
	d.bufferOffsets = d.bufferOffsets[:0]

	return nil
}

// IsPersisted returns true if there are no pending writes
func (d *Data) IsPersisted() bool {
	return len(d.appendBuffer) == 0
}

// Close closes the reader file descriptor
func (d *Data) Close() error {
	// Flush any unflushed data (similar to vectors.Close())
	if err := d.Flush(); err != nil {
		return err
	}

	// Close the reader file descriptor if it's open
	if d.readerFD != nil {
		if err := d.readerFD.Close(); err != nil {
			return err
		}
		d.readerFD = nil
	}

	return nil
}

// Truncate removes all data from storage
func (d *Data) Truncate() error {
	// Truncate the storage file to zero size
	if err := d.storage.Truncate(0); err != nil {
		return err
	}

	// Clear in-memory buffers
	d.appendBuffer = d.appendBuffer[:0]
	d.bufferOffsets = d.bufferOffsets[:0]
	d.persistedSize = 0

	return nil
}

// Read reads data from storage
func (d *Data) Read(offset int64, length int64) ([]byte, error) {
	// Handle sentinel offset for empty data
	if offset < 0 {
		return []byte{}, nil
	}

	// Check if data is in the append buffer
	if offset >= d.persistedSize {
		bufferOffset := offset - d.persistedSize
		bufferEnd := bufferOffset + length

		// Verify the data is within buffer bounds
		if bufferEnd > int64(len(d.appendBuffer)) {
			return nil, fmt.Errorf("read beyond buffer: offset=%d, length=%d, bufferSize=%d",
				offset, length, len(d.appendBuffer))
		}

		// Return slice from buffer (make a copy to avoid external mutation)
		result := make([]byte, length)
		copy(result, d.appendBuffer[bufferOffset:bufferEnd])
		return result, nil
	}

	// Data is persisted, read from storage
	// Lazily initialize reader file descriptor on first use
	if d.readerFD == nil {
		reader, err := d.storage.Reader(0)
		if err != nil {
			return nil, err
		}
		d.readerFD = reader
	}

	// Seek to the proper offset
	_, err := d.readerFD.Seek(offset, io.SeekStart)
	if err != nil {
		return nil, err
	}

	// Read the data
	buf := make([]byte, length)
	n, err := io.ReadFull(d.readerFD, buf)
	if err != nil {
		return nil, err
	}
	if int64(n) != length {
		return nil, fmt.Errorf("expected %d bytes, got %d", length, n)
	}
	return buf, nil
}

// Append appends data to storage and returns offset and length
func (d *Data) Append(data []byte) (int64, int64, error) {
	// Handle empty or nil data with sentinel offset
	if len(data) == 0 {
		return -1, 0, nil
	}

	dataSize := int64(len(data))

	// Calculate current offset (where this data will live)
	offset := d.persistedSize + int64(len(d.appendBuffer))

	// Special case: maxAppendBufferSize = 0 means immediate write
	if d.maxAppendBufferSize == 0 {
		// Write directly to disk
		writer, err := d.storage.Appender()
		if err != nil {
			return 0, 0, err
		}
		defer writer.Close()

		n, err := writer.Write(data)
		if err != nil {
			return 0, 0, err
		}

		d.persistedSize += int64(n)
		return offset, int64(n), nil
	}

	// Check if adding this data would exceed buffer size
	newBufferSize := int64(len(d.appendBuffer)) + dataSize

	if newBufferSize > d.maxAppendBufferSize {
		// OVERLAP CASE: First flush existing buffer
		if err := d.Flush(); err != nil {
			return 0, 0, err
		}

		// Recalculate offset after flush
		offset = d.persistedSize

		// If the single record is larger than buffer size, write directly
		if dataSize > d.maxAppendBufferSize {
			writer, err := d.storage.Appender()
			if err != nil {
				return 0, 0, err
			}
			defer writer.Close()

			n, err := writer.Write(data)
			if err != nil {
				return 0, 0, err
			}

			d.persistedSize += int64(n)
			return offset, int64(n), nil
		}
	}

	// Add to buffer
	d.bufferOffsets = append(d.bufferOffsets, offset)
	d.appendBuffer = append(d.appendBuffer, data...)

	return offset, dataSize, nil
}

// Replace replaces data at the specified offset
func (d *Data) Replace(data []byte, offset int64) error {
	// Calculate total data size (persisted + buffered)
	totalSize := d.persistedSize + int64(len(d.appendBuffer))

	// Step 1: Check that offset is valid for replacement
	if offset < 0 || offset >= totalSize {
		return fmt.Errorf("invalid offset for replacement: offset=%d, totalSize=%d", offset, totalSize)
	}

	dataLen := int64(len(data))

	// Step 2: If offset is in appendBuffer and replaced data fits into the appendBuffer
	if offset >= d.persistedSize {
		bufferOffset := offset - d.persistedSize
		bufferEnd := bufferOffset + dataLen

		// Check if the replaced data fits into the appendBuffer
		if bufferEnd <= int64(len(d.appendBuffer)) {
			// Replace data in appendBuffer
			copy(d.appendBuffer[bufferOffset:bufferEnd], data)
			return nil
		}
	}

	// Step 3: For all other cases, flush and then replace in storage
	if err := d.Flush(); err != nil {
		return err
	}

	// Replace data in storage using Writer at the specified offset
	writer, err := d.storage.Writer(offset)
	if err != nil {
		return err
	}
	defer writer.Close()

	n, err := writer.Write(data)
	if err != nil {
		return err
	}

	if int64(n) != dataLen {
		return fmt.Errorf("expected to write %d bytes, wrote %d", dataLen, n)
	}

	return nil
}
