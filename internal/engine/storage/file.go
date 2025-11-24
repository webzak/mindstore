package storage

import (
	"errors"
	"fmt"
	"io"
	"os"
)

var (
	ErrFileStat          = errors.New("error file stat")
	ErrFileAppend        = errors.New("error opening file for append")
	ErrFileWrite         = errors.New("error opening file for write")
	ErrFileRead          = errors.New("error opening file for reading")
	ErrFilePathIsDir     = errors.New("error file path is directory")
	ErrFileCreate        = errors.New("error creating file")
	ErrFileInvalidOffset = errors.New("error invalid file offset")
	ErrFileSeek          = errors.New("error file seek")
	ErrMemOffset         = errors.New("error invalid memory offset")
)

// File represents the file storage
type File struct {
	path string
}

// NewFile creates file storage
// note it does not do any filesystem operations, check Init() method
func NewFile(path string) *File {
	return &File{path}
}

// Init looks up for the file path, if file is not created it creates new one
func (f *File) Init() error {
	stat, err := os.Stat(f.path)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return err
		}
	} else {
		if stat.IsDir() {
			return fmt.Errorf("%w: %s", ErrFilePathIsDir, f.path)
		}
		return nil
	}
	fd, err := os.Create(f.path)
	if err != nil {
		return fmt.Errorf("%w: %s %s", ErrFileCreate, err.Error(), f.path)
	}
	err = fd.Close()
	if err != nil {
		return err
	}
	return nil
}

// Size returns file size in bytes
func (f *File) Size() (int64, error) {
	s, err := os.Stat(f.path)
	if err != nil {
		return 0, fmt.Errorf("%w: %s", ErrFileStat, err.Error())
	}
	return s.Size(), nil
}

// Creates file writer on specific offset
func (f *File) Writer(offset int64) (*os.File, error) {
	if offset < 0 {
		return nil, ErrFileInvalidOffset
	}
	fd, err := os.OpenFile(f.path, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, fmt.Errorf("%w: %s %s", ErrFileWrite, err.Error(), f.path)
	}

	_, err = fd.Seek(offset, io.SeekStart)
	if err != nil {
		fd.Close()
		return nil, fmt.Errorf("%w: %s %s", ErrFileSeek, err.Error(), f.path)
	}
	return fd, nil
}

// Appender creates file description opened in append mode
func (f *File) Appender() (*os.File, error) {

	fd, err := os.OpenFile(f.path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return nil, fmt.Errorf("%w: %s %s", ErrFileAppend, err.Error(), f.path)
	}
	return fd, nil
}

// Reader creates file descriptor set on specific offset
func (f *File) Reader(offset int64) (*os.File, error) {
	if offset < 0 {
		return nil, ErrFileInvalidOffset
	}

	fd, err := os.Open(f.path)
	if err != nil {
		return nil, fmt.Errorf("%w: %s %s", ErrFileRead, err.Error(), f.path)
	}
	_, err = fd.Seek(offset, 0)
	if err != nil {
		return nil, fmt.Errorf("%w: %s %s", ErrFileSeek, err.Error(), f.path)
	}
	return fd, nil
}

// Truncate truncates the file to the specified size
func (f *File) Truncate(size int64) error {
	if size < 0 {
		return ErrFileInvalidOffset
	}
	return os.Truncate(f.path, size)
}
