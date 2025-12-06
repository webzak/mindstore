package dataset

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/webzak/mindstore/internal/storage"
	"github.com/webzak/mindstore/internal/testutil/assert"
)

func TestOpen(t *testing.T) {
	tmpDir := t.TempDir()

	tests := []struct {
		name    string
		path    string
		dsName  string
		opts    Options
		wantErr bool
	}{
		{
			name:    "create new dataset with default options",
			path:    tmpDir,
			dsName:  "test_dataset",
			opts:    DefaultOptions(),
			wantErr: false,
		},
		{
			name:   "create dataset with custom options",
			path:   tmpDir,
			dsName: "test_custom",
			opts: Options{
				MaxDataAppendBufferSize:     1024,
				MaxMetaDataAppendBufferSize: 512,
				MaxIndexAppendBufferSize:    32,
				VectorSize:                  384,
				MaxVectorBufferSize:         32,
				MaxVectorAppendBufferSize:   32,
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ds, err := Open(tt.path, tt.dsName, tt.opts)
			if (err != nil) != tt.wantErr {
				t.Errorf("Open() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr {
				assert.NotNil(t, ds, "expected non-nil dataset")
				defer ds.Close()

				// Verify dataset directory was created
				dsPath := filepath.Join(tt.path, tt.dsName)
				if _, err := os.Stat(dsPath); os.IsNotExist(err) {
					t.Errorf("dataset directory was not created at %s", dsPath)
				}

				// Verify lock file exists
				lockPath := filepath.Join(dsPath, ".lock")
				if _, err := os.Stat(lockPath); os.IsNotExist(err) {
					t.Errorf("lock file was not created at %s", lockPath)
				}
			}
		})
	}
}

func TestDatasetLocking(t *testing.T) {
	tmpDir := t.TempDir()
	dsName := "locked_dataset"

	// Open first instance
	ds1, err := Open(tmpDir, dsName, DefaultOptions())
	assert.NilError(t, err)
	defer ds1.Close()

	// Try to open second instance - should fail due to lock
	ds2, err := Open(tmpDir, dsName, DefaultOptions())
	assert.ErrorIs(t, ErrDatasetLocked, err)
	if ds2 != nil {
		ds2.Close()
	}
}

func TestClose(t *testing.T) {
	tmpDir := t.TempDir()
	ds, err := Open(tmpDir, "test_close", DefaultOptions())
	assert.NilError(t, err)

	// Close should succeed
	assert.NilError(t, ds.Close())

	// Second close should return error
	assert.ErrorIs(t, ErrDatasetClosed, ds.Close())
}

func TestOperationsOnClosedDataset(t *testing.T) {
	tmpDir := t.TempDir()
	ds, err := Open(tmpDir, "test_closed_ops", DefaultOptions())
	assert.NilError(t, err)

	ds.Close()

	// Test various operations on closed dataset
	item := Item{Data: []byte("test")}

	_, err = ds.Append(item)
	assert.ErrorIs(t, ErrDatasetClosed, err)

	_, err = ds.Read(0, ReadData)
	assert.ErrorIs(t, ErrDatasetClosed, err)

	err = ds.Flush()
	assert.ErrorIs(t, ErrDatasetClosed, err)

	err = ds.Truncate()
	assert.ErrorIs(t, ErrDatasetClosed, err)

	err = ds.AddTags(0, "test")
	assert.ErrorIs(t, ErrDatasetClosed, err)

	err = ds.RemoveTags(0, "test")
	assert.ErrorIs(t, ErrDatasetClosed, err)

	_, err = ds.GetIDsByTag("test")
	assert.ErrorIs(t, ErrDatasetClosed, err)

	_, err = ds.GetTagsByID(0)
	assert.ErrorIs(t, ErrDatasetClosed, err)
}

func TestFlush(t *testing.T) {
	tmpDir := t.TempDir()
	opts := DefaultOptions()
	opts.VectorSize = 3
	ds, err := Open(tmpDir, "test_flush", opts)
	assert.NilError(t, err)
	defer ds.Close()

	// Add some data
	item := Item{
		Data:           []byte("test data"),
		Meta:           []byte("test meta"),
		DataDescriptor: 1,
		MetaDescriptor: 2,
		Vector:         []float32{1.0, 2.0, 3.0},
	}
	_, err = ds.Append(item)
	assert.NilError(t, err)

	// Flush should succeed
	assert.NilError(t, ds.Flush())

	// After flush, data should be persisted
	if !ds.IsPersisted() {
		t.Error("expected dataset to be persisted after Flush()")
	}
}

func TestTruncate(t *testing.T) {
	tmpDir := t.TempDir()
	ds, err := Open(tmpDir, "test_truncate", DefaultOptions())
	assert.NilError(t, err)
	defer ds.Close()

	// Add some items
	for i := 0; i < 10; i++ {
		item := Item{
			Data:           []byte("test data"),
			DataDescriptor: 1,
		}
		_, err = ds.Append(item)
		assert.NilError(t, err)
	}

	assert.Equal(t, 10, ds.Count())

	// Truncate
	assert.NilError(t, ds.Truncate())

	// Count should be 0
	assert.Equal(t, 0, ds.Count())
}

func TestCount(t *testing.T) {
	tmpDir := t.TempDir()
	ds, err := Open(tmpDir, "test_count", DefaultOptions())
	assert.NilError(t, err)
	defer ds.Close()

	// Initially should be 0
	assert.Equal(t, 0, ds.Count())

	// Add items and verify count
	for i := 1; i <= 5; i++ {
		item := Item{Data: []byte("test")}
		_, err = ds.Append(item)
		assert.NilError(t, err)
		assert.Equal(t, i, ds.Count())
	}
}

func TestIsPersisted(t *testing.T) {
	tmpDir := t.TempDir()
	ds, err := Open(tmpDir, "test_persisted", DefaultOptions())
	assert.NilError(t, err)
	defer ds.Close()

	// Initially should be persisted (empty)
	if !ds.IsPersisted() {
		t.Error("expected empty dataset to be persisted")
	}

	// Add data - may or may not be persisted depending on buffer size
	item := Item{Data: []byte("test")}
	_, err = ds.Append(item)
	assert.NilError(t, err)

	// After flush, should definitely be persisted
	ds.Flush()
	if !ds.IsPersisted() {
		t.Error("expected dataset to be persisted after Flush()")
	}
}

func TestEnsureDir(t *testing.T) {
	tmpDir := t.TempDir()

	tests := []struct {
		name    string
		path    string
		setup   func(string) error
		wantErr bool
	}{
		{
			name:    "create new directory",
			path:    filepath.Join(tmpDir, "new_dir"),
			setup:   nil,
			wantErr: false,
		},
		{
			name:    "directory already exists",
			path:    filepath.Join(tmpDir, "existing_dir"),
			setup:   func(p string) error { return os.MkdirAll(p, 0755) },
			wantErr: false,
		},
		{
			name: "path is a file, not directory",
			path: filepath.Join(tmpDir, "file_not_dir"),
			setup: func(p string) error {
				return os.WriteFile(p, []byte("test"), 0644)
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.setup != nil {
				assert.NilError(t, tt.setup(tt.path))
			}

			err := storage.EnsureDir(tt.path)
			if (err != nil) != tt.wantErr {
				t.Errorf("storage.EnsureDir() error = %v, wantErr %v", err, tt.wantErr)
			}

			if !tt.wantErr {
				// Verify directory exists
				info, err := os.Stat(tt.path)
				assert.NilError(t, err)
				if !info.IsDir() {
					t.Error("path is not a directory")
				}
			}
		})
	}
}
