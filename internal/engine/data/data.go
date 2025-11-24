package data

import (
	"fmt"
	"io"

	"github.com/webzak/mindstore/internal/engine/storage"
)

// Data represents the raw data storage
type Data struct {
	storage *storage.File
}

// New creates a new data storage
func New(path string) (*Data, error) {
	storage := storage.NewFile(path)
	if err := storage.Init(); err != nil {
		return nil, err
	}
	return &Data{
		storage: storage,
	}, nil
}

// Read reads data from storage
func (d *Data) Read(offset int64, length int32) ([]byte, error) {
	reader, err := d.storage.Reader(offset)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	buf := make([]byte, length)
	n, err := io.ReadFull(reader, buf)
	if err != nil {
		return nil, err
	}
	if int32(n) != length {
		return nil, fmt.Errorf("expected %d bytes, got %d", length, n)
	}
	return buf, nil
}

// Append appends data to storage and returns offset and length
func (d *Data) Append(data []byte) (int64, int32, error) {
	offset, err := d.storage.Size()
	if err != nil {
		return 0, 0, err
	}

	writer, err := d.storage.Appender()
	if err != nil {
		return 0, 0, err
	}
	defer writer.Close()

	n, err := writer.Write(data)
	if err != nil {
		return 0, 0, err
	}
	return offset, int32(n), nil
}
