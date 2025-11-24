package vectors

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/webzak/mindstore/internal/testutil"
)

func TestNew(t *testing.T) {
	tmpDir := testutil.MakeTempDir(t)
	defer os.RemoveAll(tmpDir)

	t.Run("default options", func(t *testing.T) {
		path := filepath.Join(tmpDir, "vectors_default.bin")
		v, err := New(path, nil)
		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}
		if v == nil {
			t.Fatal("expected non-nil Vectors instance")
		}
		if v.vectorSize != DefaultVectorSize {
			t.Errorf("expected vectorSize=%d, got %d", DefaultVectorSize, v.vectorSize)
		}
		if v.maxBufferSize != DefaultMaxBufferSize {
			t.Errorf("expected maxBufferSize=%d, got %d", DefaultMaxBufferSize, v.maxBufferSize)
		}
		if v.maxAppendSize != DefaultMaxAppendBufferSize {
			t.Errorf("expected maxAppendSize=%d, got %d", DefaultMaxAppendBufferSize, v.maxAppendSize)
		}
		if v.persistedSize != 0 {
			t.Errorf("expected persistedSize=0, got %d", v.persistedSize)
		}
		if v.storage == nil {
			t.Error("expected non-nil storage")
		}
		if v.appendBuffer == nil {
			t.Error("expected non-nil appendBuffer")
		}
		if len(v.appendBuffer) != 0 {
			t.Errorf("expected empty appendBuffer, got length %d", len(v.appendBuffer))
		}
	})

	t.Run("custom options", func(t *testing.T) {
		path := filepath.Join(tmpDir, "vectors_custom.bin")
		opts := &VectorsOptions{
			VectorSize:          384,
			MaxBufferSize:       128,
			MaxAppendBufferSize: 32,
		}
		v, err := New(path, opts)
		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}
		if v.vectorSize != 384 {
			t.Errorf("expected vectorSize=384, got %d", v.vectorSize)
		}
		if v.maxBufferSize != 128 {
			t.Errorf("expected maxBufferSize=128, got %d", v.maxBufferSize)
		}
		if v.maxAppendSize != 32 {
			t.Errorf("expected maxAppendSize=32, got %d", v.maxAppendSize)
		}
	})

	t.Run("creates file if not exists", func(t *testing.T) {
		path := filepath.Join(tmpDir, "vectors_new.bin")
		// Ensure file doesn't exist
		os.Remove(path)

		v, err := New(path, nil)
		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}
		if v == nil {
			t.Fatal("expected non-nil Vectors instance")
		}

		// Verify file was created
		if _, err := os.Stat(path); os.IsNotExist(err) {
			t.Error("expected file to be created")
		}
	})

	t.Run("opens existing file", func(t *testing.T) {
		path := filepath.Join(tmpDir, "vectors_existing.bin")

		// Create and write some data
		v1, err := New(path, nil)
		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}

		// Append a vector to create some data
		vector := make([]float32, DefaultVectorSize)
		for i := range vector {
			vector[i] = float32(i)
		}
		_, err = v1.Append(vector)
		if err != nil {
			t.Fatalf("failed to append vector: %v", err)
		}
		err = v1.Flush()
		if err != nil {
			t.Fatalf("failed to flush: %v", err)
		}

		// Open the same file again
		v2, err := New(path, nil)
		if err != nil {
			t.Fatalf("expected no error, got: %v", err)
		}

		// Verify persistedSize is calculated correctly
		// Note: persistedSize is calculated as total_bytes / (vectorSize * Float32Size)
		// For 1 vector of 768 floats: (768 * 4 bytes) / (768 * 4) = 1
		expectedSize := 1
		if v2.persistedSize != expectedSize {
			t.Errorf("expected persistedSize=%d, got %d", expectedSize, v2.persistedSize)
		}
	})

	t.Run("invalid directory path", func(t *testing.T) {
		// Use a path that cannot be created (e.g., inside a non-existent parent)
		path := filepath.Join(tmpDir, "nonexistent", "subdir", "vectors.bin")

		_, err := New(path, nil)
		if err == nil {
			t.Error("expected error for invalid directory path, got nil")
		}
	})
}
