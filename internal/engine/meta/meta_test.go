package meta

import (
	"encoding/gob"
	"path/filepath"
	"testing"
)

func init() {
	// Register map types for gob encoding
	gob.Register(map[string]any{})
}

// Helper function to create a temporary test file
func createTempFile(t *testing.T) string {
	t.Helper()
	tmpDir := t.TempDir()
	return filepath.Join(tmpDir, "meta.dat")
}

func TestNew(t *testing.T) {
	path := createTempFile(t)

	meta, err := New(path)
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	if meta == nil {
		t.Fatal("New() returned nil meta")
	}

	if meta.data == nil {
		t.Error("data map not initialized")
	}

	if !meta.isPersisted {
		t.Error("isPersisted should be true for new meta")
	}

	if !meta.isLoaded {
		t.Error("isLoaded should be true for empty storage")
	}
}

func TestGet(t *testing.T) {
	path := createTempFile(t)
	meta, err := New(path)
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	// Test getting metadata for non-existent ID
	data, err := meta.Get(999)
	if err != nil {
		t.Errorf("Get() failed: %v", err)
	}
	if data != nil {
		t.Errorf("Expected nil for non-existent ID, got %v", data)
	}

	// Add metadata
	meta.Set(1, map[string]any{"key1": "value1", "key2": 42})

	// Test getting metadata for existing ID
	data, err = meta.Get(1)
	if err != nil {
		t.Errorf("Get() failed: %v", err)
	}
	if len(data) != 2 {
		t.Errorf("Expected 2 keys, got %d", len(data))
	}
	if data["key1"] != "value1" {
		t.Errorf("Expected 'value1', got %v", data["key1"])
	}
	if data["key2"] != 42 {
		t.Errorf("Expected 42, got %v", data["key2"])
	}

	// Test that returned map is a copy
	data["key1"] = "modified"
	originalData, _ := meta.Get(1)
	if originalData["key1"] == "modified" {
		t.Error("Modifying returned map should not affect internal state")
	}

	// Test negative ID
	_, err = meta.Get(-1)
	if err == nil {
		t.Error("Expected error for negative ID")
	}
}

func TestSet(t *testing.T) {
	path := createTempFile(t)
	meta, err := New(path)
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	// Test setting metadata
	err = meta.Set(1, map[string]any{"key1": "value1"})
	if err != nil {
		t.Errorf("Set() failed: %v", err)
	}

	// Verify metadata was set
	data, _ := meta.Get(1)
	if len(data) != 1 || data["key1"] != "value1" {
		t.Error("Metadata not set correctly")
	}

	// Test merging behavior
	err = meta.Set(1, map[string]any{"key2": "value2"})
	if err != nil {
		t.Errorf("Set() failed: %v", err)
	}

	// Verify both keys exist
	data, _ = meta.Get(1)
	if len(data) != 2 {
		t.Errorf("Expected 2 keys after merge, got %d", len(data))
	}
	if data["key1"] != "value1" || data["key2"] != "value2" {
		t.Error("Merge did not preserve existing keys")
	}

	// Test updating existing key
	err = meta.Set(1, map[string]any{"key1": "updated"})
	if err != nil {
		t.Errorf("Set() failed: %v", err)
	}

	data, _ = meta.Get(1)
	if data["key1"] != "updated" {
		t.Error("Existing key not updated")
	}

	// Test nil data
	err = meta.Set(2, nil)
	if err != nil {
		t.Errorf("Set() with nil data failed: %v", err)
	}

	// Test negative ID
	err = meta.Set(-1, map[string]any{"key": "value"})
	if err == nil {
		t.Error("Expected error for negative ID")
	}

	// Verify isPersisted is false after setting
	if meta.isPersisted {
		t.Error("isPersisted should be false after setting metadata")
	}
}

func TestReplace(t *testing.T) {
	path := createTempFile(t)
	meta, err := New(path)
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	// Set initial metadata
	meta.Set(1, map[string]any{"key1": "value1", "key2": "value2"})

	// Test replacing metadata
	err = meta.Replace(1, map[string]any{"key3": "value3"})
	if err != nil {
		t.Errorf("Replace() failed: %v", err)
	}

	// Verify old keys are gone
	data, _ := meta.Get(1)
	if len(data) != 1 {
		t.Errorf("Expected 1 key after replace, got %d", len(data))
	}
	if data["key3"] != "value3" {
		t.Error("New key not set correctly")
	}
	if _, exists := data["key1"]; exists {
		t.Error("Old keys should be removed after replace")
	}

	// Test replacing with nil
	err = meta.Replace(1, nil)
	if err != nil {
		t.Errorf("Replace() with nil failed: %v", err)
	}

	data, _ = meta.Get(1)
	if len(data) != 0 {
		t.Errorf("Expected empty map after replacing with nil, got %d keys", len(data))
	}

	// Test negative ID
	err = meta.Replace(-1, map[string]any{"key": "value"})
	if err == nil {
		t.Error("Expected error for negative ID")
	}

	// Verify isPersisted is false after replacing
	if meta.isPersisted {
		t.Error("isPersisted should be false after replacing metadata")
	}
}

func TestSetKey(t *testing.T) {
	path := createTempFile(t)
	meta, err := New(path)
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	// Test setting a key
	err = meta.SetKey(1, "key1", "value1")
	if err != nil {
		t.Errorf("SetKey() failed: %v", err)
	}

	// Verify key was set
	data, _ := meta.Get(1)
	if data["key1"] != "value1" {
		t.Error("Key not set correctly")
	}

	// Test setting another key for same ID
	err = meta.SetKey(1, "key2", 42)
	if err != nil {
		t.Errorf("SetKey() failed: %v", err)
	}

	data, _ = meta.Get(1)
	if len(data) != 2 {
		t.Errorf("Expected 2 keys, got %d", len(data))
	}

	// Test updating existing key
	err = meta.SetKey(1, "key1", "updated")
	if err != nil {
		t.Errorf("SetKey() failed: %v", err)
	}

	data, _ = meta.Get(1)
	if data["key1"] != "updated" {
		t.Error("Key not updated correctly")
	}

	// Test negative ID
	err = meta.SetKey(-1, "key", "value")
	if err == nil {
		t.Error("Expected error for negative ID")
	}

	// Test empty key
	err = meta.SetKey(1, "", "value")
	if err == nil {
		t.Error("Expected error for empty key")
	}

	// Verify isPersisted is false after setting key
	if meta.isPersisted {
		t.Error("isPersisted should be false after setting key")
	}
}

func TestDelete(t *testing.T) {
	path := createTempFile(t)
	meta, err := New(path)
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	// Add metadata
	meta.Set(1, map[string]any{"key1": "value1"})
	meta.Set(2, map[string]any{"key2": "value2"})

	// Test deleting metadata
	err = meta.Delete(1)
	if err != nil {
		t.Errorf("Delete() failed: %v", err)
	}

	// Verify metadata was deleted
	data, _ := meta.Get(1)
	if data != nil {
		t.Error("Metadata should be deleted")
	}

	// Verify other metadata still exists
	data, _ = meta.Get(2)
	if data == nil || data["key2"] != "value2" {
		t.Error("Other metadata should not be affected")
	}

	// Test deleting non-existent ID (should not error)
	err = meta.Delete(999)
	if err != nil {
		t.Errorf("Delete() on non-existent ID failed: %v", err)
	}

	// Test negative ID
	err = meta.Delete(-1)
	if err == nil {
		t.Error("Expected error for negative ID")
	}

	// Verify isPersisted is false after deleting
	if meta.isPersisted {
		t.Error("isPersisted should be false after deleting metadata")
	}
}

func TestDeleteKey(t *testing.T) {
	path := createTempFile(t)
	meta, err := New(path)
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	// Add metadata
	meta.Set(1, map[string]any{"key1": "value1", "key2": "value2"})

	// Test deleting a key
	err = meta.DeleteKey(1, "key1")
	if err != nil {
		t.Errorf("DeleteKey() failed: %v", err)
	}

	// Verify key was deleted
	data, _ := meta.Get(1)
	if _, exists := data["key1"]; exists {
		t.Error("Key should be deleted")
	}
	if data["key2"] != "value2" {
		t.Error("Other keys should not be affected")
	}

	// Test deleting last key (should remove ID entry)
	err = meta.DeleteKey(1, "key2")
	if err != nil {
		t.Errorf("DeleteKey() failed: %v", err)
	}

	data, _ = meta.Get(1)
	if data != nil {
		t.Error("ID entry should be removed when last key is deleted")
	}

	// Test deleting non-existent key (should not error)
	meta.Set(2, map[string]any{"key": "value"})
	err = meta.DeleteKey(2, "nonexistent")
	if err != nil {
		t.Errorf("DeleteKey() on non-existent key failed: %v", err)
	}

	// Test negative ID
	err = meta.DeleteKey(-1, "key")
	if err == nil {
		t.Error("Expected error for negative ID")
	}

	// Test empty key
	err = meta.DeleteKey(1, "")
	if err == nil {
		t.Error("Expected error for empty key")
	}

	// Verify isPersisted is false after deleting key
	if meta.isPersisted {
		t.Error("isPersisted should be false after deleting key")
	}
}

func TestCount(t *testing.T) {
	path := createTempFile(t)
	meta, err := New(path)
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	// Test empty meta
	count, err := meta.Count()
	if err != nil {
		t.Errorf("Count() failed: %v", err)
	}
	if count != 0 {
		t.Errorf("Expected count 0, got %d", count)
	}

	// Add metadata to different IDs
	meta.Set(1, map[string]any{"key1": "value1"})
	meta.Set(2, map[string]any{"key2": "value2"})

	count, err = meta.Count()
	if err != nil {
		t.Errorf("Count() failed: %v", err)
	}
	if count != 2 {
		t.Errorf("Expected count 2, got %d", count)
	}

	// Add more metadata to existing ID (should not increase count)
	meta.SetKey(1, "key3", "value3")

	count, err = meta.Count()
	if err != nil {
		t.Errorf("Count() failed: %v", err)
	}
	if count != 2 {
		t.Errorf("Expected count 2, got %d", count)
	}

	// Add metadata to new ID
	meta.Set(3, map[string]any{"key4": "value4"})

	count, err = meta.Count()
	if err != nil {
		t.Errorf("Count() failed: %v", err)
	}
	if count != 3 {
		t.Errorf("Expected count 3, got %d", count)
	}

	// Delete metadata
	meta.Delete(1)

	count, err = meta.Count()
	if err != nil {
		t.Errorf("Count() failed: %v", err)
	}
	if count != 2 {
		t.Errorf("Expected count 2 after delete, got %d", count)
	}
}

func TestFlush(t *testing.T) {
	path := createTempFile(t)
	meta, err := New(path)
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	// Add metadata
	meta.Set(1, map[string]any{"key1": "value1"})

	// Verify isPersisted is false
	if meta.isPersisted {
		t.Error("isPersisted should be false after adding metadata")
	}

	// Flush
	err = meta.Flush()
	if err != nil {
		t.Errorf("Flush() failed: %v", err)
	}

	// Verify isPersisted is true
	if !meta.isPersisted {
		t.Error("isPersisted should be true after Flush()")
	}

	// Verify storage is not empty
	size, err := meta.storage.Size()
	if err != nil {
		t.Errorf("Size() failed: %v", err)
	}
	if size == 0 {
		t.Error("Expected storage size to be greater than 0 after Flush()")
	}

	// Test flushing when already persisted (should be no-op)
	err = meta.Flush()
	if err != nil {
		t.Errorf("Flush() on already persisted meta failed: %v", err)
	}
}

func TestIsPersisted(t *testing.T) {
	path := createTempFile(t)
	meta, err := New(path)
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	// Verify initial state
	if !meta.IsPersisted() {
		t.Error("IsPersisted() should be true for new meta")
	}

	// Modify data
	meta.Set(1, map[string]any{"key": "value"})

	// Verify isPersisted is false
	if meta.IsPersisted() {
		t.Error("IsPersisted() should be false after modification")
	}

	// Flush
	meta.Flush()

	// Verify isPersisted is true
	if !meta.IsPersisted() {
		t.Error("IsPersisted() should be true after Flush()")
	}
}

func TestDestroy(t *testing.T) {
	path := createTempFile(t)
	meta, err := New(path)
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	// Add metadata
	meta.Set(1, map[string]any{"key1": "value1"})
	meta.Set(2, map[string]any{"key2": "value2"})

	// Flush to persist
	meta.Flush()

	// Destroy
	err = meta.Destroy()
	if err != nil {
		t.Errorf("Destroy() failed: %v", err)
	}

	// Verify data map is empty
	if len(meta.data) != 0 {
		t.Error("data map should be empty after Destroy()")
	}

	// Verify isPersisted is true
	if !meta.isPersisted {
		t.Error("isPersisted should be true after Destroy()")
	}

	// Verify storage is empty
	size, err := meta.storage.Size()
	if err != nil {
		t.Errorf("Size() failed: %v", err)
	}
	if size != 0 {
		t.Errorf("Expected storage size to be 0, got %d", size)
	}

	// Verify count is 0
	count, _ := meta.Count()
	if count != 0 {
		t.Errorf("Expected count 0 after Destroy(), got %d", count)
	}
}

func TestPersistenceAndLoad(t *testing.T) {
	path := createTempFile(t)

	// Create and populate meta
	meta1, err := New(path)
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	meta1.Set(1, map[string]any{"key1": "value1", "key2": 42})
	meta1.Set(2, map[string]any{"key3": true, "key4": 3.14})
	meta1.Set(3, map[string]any{"nested": map[string]any{"inner": "value"}})

	err = meta1.Flush()
	if err != nil {
		t.Errorf("Flush() failed: %v", err)
	}

	// Create new meta instance from same path
	meta2, err := New(path)
	if err != nil {
		t.Fatalf("New() failed on reload: %v", err)
	}

	// Force load
	meta2.isLoaded = false
	err = meta2.load()
	if err != nil {
		t.Errorf("load() failed: %v", err)
	}

	// Verify data for ID 1
	data, _ := meta2.Get(1)
	if len(data) != 2 {
		t.Errorf("Expected 2 keys for ID 1, got %d", len(data))
	}
	if data["key1"] != "value1" || data["key2"] != 42 {
		t.Error("Data for ID 1 not loaded correctly")
	}

	// Verify data for ID 2
	data, _ = meta2.Get(2)
	if len(data) != 2 {
		t.Errorf("Expected 2 keys for ID 2, got %d", len(data))
	}
	if data["key3"] != true || data["key4"] != 3.14 {
		t.Error("Data for ID 2 not loaded correctly")
	}

	// Verify data for ID 3 (nested map)
	data, _ = meta2.Get(3)
	if nested, ok := data["nested"].(map[string]any); !ok {
		t.Error("Nested map not loaded correctly")
	} else if nested["inner"] != "value" {
		t.Error("Nested map value not loaded correctly")
	}

	// Verify isLoaded flag
	if !meta2.isLoaded {
		t.Error("isLoaded should be true after load()")
	}

	// Verify count
	count, _ := meta2.Count()
	if count != 3 {
		t.Errorf("Expected count 3 after reload, got %d", count)
	}
}

func TestLoadEmptyFile(t *testing.T) {
	path := createTempFile(t)

	// Create meta with empty file
	meta, err := New(path)
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	// Force load on empty file
	meta.isLoaded = false
	err = meta.load()
	if err != nil {
		t.Errorf("load() on empty file failed: %v", err)
	}

	if !meta.isLoaded {
		t.Error("isLoaded should be true after loading empty file")
	}

	if len(meta.data) != 0 {
		t.Error("data map should be empty after loading empty file")
	}
}

func TestLazyLoad(t *testing.T) {
	path := createTempFile(t)

	// Create and populate meta
	meta1, err := New(path)
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	meta1.Set(1, map[string]any{"key1": "value1"})
	meta1.Flush()

	// Create new instance
	meta2, err := New(path)
	if err != nil {
		t.Fatalf("New() failed on reload: %v", err)
	}

	// Mark as not loaded to simulate lazy loading
	meta2.isLoaded = false

	// Calling Set should trigger load
	err = meta2.Set(2, map[string]any{"key2": "value2"})
	if err != nil {
		t.Errorf("Set() failed: %v", err)
	}

	if !meta2.isLoaded {
		t.Error("isLoaded should be true after Set() triggers load")
	}

	// Verify old data was loaded
	data, _ := meta2.Get(1)
	if data == nil || data["key1"] != "value1" {
		t.Error("Previously persisted data should be loaded")
	}

	// Verify new data was added
	data, _ = meta2.Get(2)
	if data == nil || data["key2"] != "value2" {
		t.Error("New data should be added after load")
	}

	// Test lazy load with Get
	meta3, _ := New(path)
	meta3.isLoaded = false

	data, _ = meta3.Get(1)
	if !meta3.isLoaded {
		t.Error("Get() should trigger lazy load")
	}
	if data == nil {
		t.Error("Get() should return data after lazy load")
	}

	// Test lazy load with Count
	meta4, _ := New(path)
	meta4.isLoaded = false

	_, _ = meta4.Count()
	if !meta4.isLoaded {
		t.Error("Count() should trigger lazy load")
	}

	// Test lazy load with Delete
	meta5, _ := New(path)
	meta5.isLoaded = false

	_ = meta5.Delete(1)
	if !meta5.isLoaded {
		t.Error("Delete() should trigger lazy load")
	}
}

func TestFlushWhenNotLoaded(t *testing.T) {
	path := createTempFile(t)
	meta, err := New(path)
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	// Mark as not loaded
	meta.isLoaded = false

	// Try to flush (should be no-op)
	err = meta.Flush()
	if err != nil {
		t.Errorf("Flush() when not loaded should not error: %v", err)
	}

	// Verify still not loaded
	if meta.isLoaded {
		t.Error("Flush() should not load data")
	}
}

func TestSetVsReplaceBehavior(t *testing.T) {
	path := createTempFile(t)
	meta, err := New(path)
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	// Set initial data
	meta.Set(1, map[string]any{"key1": "value1", "key2": "value2"})

	// Use Set to add a key (should merge)
	meta.Set(1, map[string]any{"key3": "value3"})

	data, _ := meta.Get(1)
	if len(data) != 3 {
		t.Error("Set() should merge, not replace")
	}

	// Use Replace to set new data (should replace)
	meta.Replace(1, map[string]any{"key4": "value4"})

	data, _ = meta.Get(1)
	if len(data) != 1 || data["key4"] != "value4" {
		t.Error("Replace() should completely replace data")
	}
	if _, exists := data["key1"]; exists {
		t.Error("Replace() should remove old keys")
	}
}

func TestComplexNestedData(t *testing.T) {
	path := createTempFile(t)
	meta, err := New(path)
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	// Test with various data types (avoiding complex slices that require gob registration)
	complexData := map[string]any{
		"string": "value",
		"int":    42,
		"float":  3.14,
		"bool":   true,
		"map":    map[string]any{"inner": "value"},
		"nilVal": nil,
	}

	err = meta.Set(1, complexData)
	if err != nil {
		t.Errorf("Set() with complex data failed: %v", err)
	}

	// Flush and reload
	meta.Flush()

	meta2, _ := New(path)
	meta2.isLoaded = false
	meta2.load()

	// Verify complex data
	data, _ := meta2.Get(1)
	if len(data) == 0 {
		t.Fatal("No data loaded after reload")
	}
	if data["string"] != "value" {
		t.Errorf("String value not persisted correctly, got %v", data["string"])
	}
	if data["int"] != 42 {
		t.Errorf("Int value not persisted correctly, got %v", data["int"])
	}
	if data["float"] != 3.14 {
		t.Errorf("Float value not persisted correctly, got %v", data["float"])
	}
	if data["bool"] != true {
		t.Errorf("Bool value not persisted correctly, got %v", data["bool"])
	}
	if data["nilVal"] != nil {
		t.Errorf("Nil value not persisted correctly, got %v", data["nilVal"])
	}
	// Verify nested map
	if nestedMap, ok := data["map"].(map[string]any); !ok {
		t.Error("Nested map not loaded with correct type")
	} else if nestedMap["inner"] != "value" {
		t.Errorf("Nested map value not persisted correctly, got %v", nestedMap["inner"])
	}
}

func TestLoad(t *testing.T) {
	path := createTempFile(t)

	// Create and populate meta
	meta1, err := New(path)
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	meta1.Set(1, map[string]any{"key": "value"})
	meta1.Flush()

	// Create new instance
	meta2, err := New(path)
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	// Mark as not loaded
	meta2.isLoaded = false

	// Call public Load method
	err = meta2.Load()
	if err != nil {
		t.Errorf("Load() failed: %v", err)
	}

	// Verify loaded
	if !meta2.isLoaded {
		t.Error("isLoaded should be true after Load()")
	}

	// Verify data
	data, _ := meta2.Get(1)
	if data == nil || data["key"] != "value" {
		t.Error("Data not loaded correctly")
	}
}
