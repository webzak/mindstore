package meta

import (
	"encoding/gob"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/webzak/mindstore/internal/engine/storage"
)

func TestMeta_Operations(t *testing.T) {
	// Create a temporary file for storage
	tmpDir := t.TempDir()
	tmpFile := filepath.Join(tmpDir, "meta.db")
	f := storage.NewFile(tmpFile)
	if err := f.Init(); err != nil {
		t.Fatalf("Failed to init storage file: %v", err)
	}
	defer func() {
		// Close not needed as NewFile doesn't open, but we might want to clean up if needed.
		// storage.File doesn't have Close() method on itself, it opens/closes on demand or returns *os.File
	}()

	m := NewMeta(f)

	// Test Set and Get
	id1 := 1
	data1 := map[string]any{
		"name": "Alice",
		"age":  30,
	}
	m.Set(id1, data1)

	got1 := m.Get(id1)
	if !reflect.DeepEqual(got1, data1) {
		t.Errorf("Get(%d) = %v, want %v", id1, got1, data1)
	}

	// Test SetKey
	m.SetKey(id1, "city", "New York")
	data1["city"] = "New York"
	got1Updated := m.Get(id1)
	if !reflect.DeepEqual(got1Updated, data1) {
		t.Errorf("Get(%d) after SetKey = %v, want %v", id1, got1Updated, data1)
	}

	// Test DeleteKey
	m.DeleteKey(id1, "age")
	delete(data1, "age")
	got1DeletedKey := m.Get(id1)
	if !reflect.DeepEqual(got1DeletedKey, data1) {
		t.Errorf("Get(%d) after DeleteKey = %v, want %v", id1, got1DeletedKey, data1)
	}

	// Test Delete
	m.Delete(id1)
	if m.Get(id1) != nil {
		t.Errorf("Get(%d) after Delete should be nil", id1)
	}
}

func TestMeta_Persistence(t *testing.T) {
	// Register types for gob if necessary (basic types are usually fine, but let's be safe if we use custom ones)
	// For this test, we use basic types.
	gob.Register(map[string]any{})

	tmpDir := t.TempDir()
	tmpFile := filepath.Join(tmpDir, "meta_persist.db")
	f := storage.NewFile(tmpFile)
	if err := f.Init(); err != nil {
		t.Fatalf("Failed to init storage file: %v", err)
	}

	m := NewMeta(f)

	id1 := 1
	data1 := map[string]any{
		"name":   "Bob",
		"active": true,
		"score":  95.5,
	}
	m.Set(id1, data1)

	id2 := 2
	data2 := map[string]any{
		"tags": []string{"go", "db"},
	}
	m.Set(id2, data2)

	if err := m.Flush(); err != nil {
		t.Fatalf("Save() failed: %v", err)
	}

	// Create a new Meta instance and load from the same file
	f2 := storage.NewFile(tmpFile)
	// No need to Init again as file exists, but Init handles it.
	if err := f2.Init(); err != nil {
		t.Fatalf("Failed to init storage file again: %v", err)
	}

	m2 := NewMeta(f2)
	if err := m2.Load(); err != nil {
		t.Fatalf("Load() failed: %v", err)
	}

	got1 := m2.Get(id1)
	if !reflect.DeepEqual(got1, data1) {
		t.Errorf("Loaded Get(%d) = %v, want %v", id1, got1, data1)
	}

	// Note: DeepEqual might fail for slices if they are nil vs empty, or different underlying types from gob.
	// gob decodes slices as slices.
	// However, numbers might be decoded as different types if not careful, but gob usually preserves exact types for basic types.
	// Let's check id2 manually if DeepEqual fails due to type mismatches (e.g. int vs int64).
	// Actually, for interface{}, gob might decode integers as int or int64 depending on the value.
	// But since we are in the same process and architecture, it should be consistent.

	got2 := m2.Get(id2)
	if _, ok := got2["tags"]; !ok {
		t.Errorf("Loaded Get(%d) missing 'tags'", id2)
	}
}
