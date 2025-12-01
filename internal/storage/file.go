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

// Init validates the file path and optionally creates an empty file
// If create is true, creates an empty file if it doesn't exist
// If create is false, only validates the path (file will be created on first write)
func (f *File) Init(create bool) error {
	stat, err := os.Stat(f.path)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return err
		}
		// File doesn't exist
		if !create {
			return nil
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
// If the file doesn't exist (lazy creation), returns 0
func (f *File) Size() (int64, error) {
	s, err := os.Stat(f.path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			// File doesn't exist yet (lazy creation) - size is 0
			return 0, nil
		}
		return 0, fmt.Errorf("%w: %s", ErrFileStat, err.Error())
	}
	return s.Size(), nil
}

// Creates file writer on specific offset
// Use offset = -1 to seek to end of file (append mode)
func (f *File) Writer(offset int64) (*os.File, error) {
	if offset < -1 {
		return nil, ErrFileInvalidOffset
	}
	fd, err := os.OpenFile(f.path, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, fmt.Errorf("%w: %s %s", ErrFileWrite, err.Error(), f.path)
	}

	var seekWhence int
	var seekOffset int64
	if offset == -1 {
		seekWhence = io.SeekEnd
		seekOffset = 0
	} else {
		seekWhence = io.SeekStart
		seekOffset = offset
	}

	_, err = fd.Seek(seekOffset, seekWhence)
	if err != nil {
		fd.Close()
		return nil, fmt.Errorf("%w: %s %s", ErrFileSeek, err.Error(), f.path)
	}
	return fd, nil
}

// Appender creates file description opened in append mode
// This is a convenience wrapper for Writer(-1)
func (f *File) Appender() (*os.File, error) {
	return f.Writer(-1)
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

// Truncate truncates the file to zero bytes
// If the file doesn't exist, this is a no-op (file will be created empty on first write)
func (f *File) Truncate() error {
	err := os.Truncate(f.path, 0)
	if err != nil && errors.Is(err, os.ErrNotExist) {
		// File doesn't exist - no need to truncate, it will be created empty on first write
		return nil
	}
	return err
}

// Path returns file path
func (f *File) Path() string {
	return f.path
}
