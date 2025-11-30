package testutil

import (
	"path/filepath"
	"testing"
)

// Helper function to create a temporary test file
func CreateTempFile(t *testing.T, name string) string {
	t.Helper()
	tmpDir := t.TempDir()
	return filepath.Join(tmpDir, name)
}
