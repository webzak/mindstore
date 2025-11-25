package vectors

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/webzak/mindstore/internal/testutil"
)

func TestAppendIntegrityCheck(t *testing.T) {
	tmpDir := testutil.MakeTempDir(t)
	defer os.RemoveAll(tmpDir)

	t.Run("append with correct index succeeds", func(t *testing.T) {
		path := filepath.Join(tmpDir, "vectors_integrity_correct.bin")
		v, err := New(path, nil)
		if err != nil {
			t.Fatalf("failed to create vectors: %v", err)
		}

		// Create vectors
		vector1 := make([]float32, DefaultVectorSize)
		for i := range vector1 {
			vector1[i] = float32(i)
		}

		vector2 := make([]float32, DefaultVectorSize)
		for i := range vector2 {
			vector2[i] = float32(i * 2)
		}

		// Append with correct index 0
		err = v.Append(0, vector1)
		if err != nil {
			t.Fatalf("failed to append with correct index 0: %v", err)
		}

		// Append with correct index 1
		err = v.Append(1, vector2)
		if err != nil {
			t.Fatalf("failed to append with correct index 1: %v", err)
		}

		// Verify count
		if v.Count() != 2 {
			t.Errorf("expected count 2, got %d", v.Count())
		}
	})

	t.Run("append with incorrect index fails", func(t *testing.T) {
		path := filepath.Join(tmpDir, "vectors_integrity_incorrect.bin")
		v, err := New(path, nil)
		if err != nil {
			t.Fatalf("failed to create vectors: %v", err)
		}

		vector := make([]float32, DefaultVectorSize)
		for i := range vector {
			vector[i] = float32(i)
		}

		// Try to append with incorrect index (should be 0)
		err = v.Append(5, vector)
		if err == nil {
			t.Fatal("expected error for incorrect index, got nil")
		}

		// Verify error message contains "integrity error"
		expectedMsg := "index integrity error: expected 0, got 5"
		if err.Error() != expectedMsg {
			t.Errorf("expected error message '%s', got '%s'", expectedMsg, err.Error())
		}

		// Verify count is still 0
		if v.Count() != 0 {
			t.Errorf("expected count 0 after failed append, got %d", v.Count())
		}
	})

	t.Run("append with wrong index after flush", func(t *testing.T) {
		path := filepath.Join(tmpDir, "vectors_integrity_after_flush.bin")
		v, err := New(path, nil)
		if err != nil {
			t.Fatalf("failed to create vectors: %v", err)
		}

		// Append first vector
		vector1 := make([]float32, DefaultVectorSize)
		for i := range vector1 {
			vector1[i] = float32(i)
		}
		err = v.Append(0, vector1)
		if err != nil {
			t.Fatalf("failed to append first vector: %v", err)
		}

		// Flush
		err = v.Flush()
		if err != nil {
			t.Fatalf("failed to flush: %v", err)
		}

		// Try to append with wrong index (should be 1, not 0)
		vector2 := make([]float32, DefaultVectorSize)
		for i := range vector2 {
			vector2[i] = float32(i * 2)
		}
		err = v.Append(0, vector2)
		if err == nil {
			t.Fatal("expected error for incorrect index after flush, got nil")
		}

		expectedMsg := "index integrity error: expected 1, got 0"
		if err.Error() != expectedMsg {
			t.Errorf("expected error message '%s', got '%s'", expectedMsg, err.Error())
		}

		// Verify correct index works
		err = v.Append(1, vector2)
		if err != nil {
			t.Fatalf("failed to append with correct index 1: %v", err)
		}

		if v.Count() != 2 {
			t.Errorf("expected count 2, got %d", v.Count())
		}
	})

	t.Run("append with negative index fails", func(t *testing.T) {
		path := filepath.Join(tmpDir, "vectors_integrity_negative.bin")
		v, err := New(path, nil)
		if err != nil {
			t.Fatalf("failed to create vectors: %v", err)
		}

		vector := make([]float32, DefaultVectorSize)
		for i := range vector {
			vector[i] = float32(i)
		}

		// Try to append with negative index
		err = v.Append(-1, vector)
		if err == nil {
			t.Fatal("expected error for negative index, got nil")
		}

		expectedMsg := "index integrity error: expected 0, got -1"
		if err.Error() != expectedMsg {
			t.Errorf("expected error message '%s', got '%s'", expectedMsg, err.Error())
		}
	})
}
