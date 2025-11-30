package tags

import (
	"path/filepath"
	"testing"

	"github.com/webzak/mindstore/internal/testutil/assert"
)

// Helper function to create a temporary test file
func createTempFile(t *testing.T) string {
	t.Helper()
	tmpDir := t.TempDir()
	return filepath.Join(tmpDir, "tags.dat")
}

func TestNew(t *testing.T) {
	path := createTempFile(t)

	tags, err := New(path)
	assert.NilError(t, err)

	assert.NotNil(t, tags, "New() returned nil tags")
	assert.NotNil(t, tags.forward, "forward map not initialized")
	assert.NotNil(t, tags.reverse, "reverse map not initialized")
	assert.Equal(t, true, tags.isPersisted)
	assert.Equal(t, true, tags.isLoaded)
}

func TestAdd(t *testing.T) {
	path := createTempFile(t)
	tags, err := New(path)
	assert.NilError(t, err)

	// Test adding a tag
	err = tags.Add(1, "test")
	assert.NilError(t, err)

	// Verify forward map
	ids := tags.forward["test"]
	if len(ids) != 1 || ids[0] != 1 {
		t.Errorf("Expected forward map to have [1] for 'test', got %v", ids)
	}

	// Verify reverse map
	tagList := tags.reverse[1]
	if len(tagList) != 1 || tagList[0] != "test" {
		t.Errorf("Expected reverse map to have ['test'] for ID 1, got %v", tagList)
	}

	// Test duplicate tag
	err = tags.Add(1, "test")
	assert.ErrorIs(t, ErrDuplicatedTag, err)

	// Test case insensitivity
	err = tags.Add(2, "TEST")
	assert.NilError(t, err)

	ids = tags.forward["test"]
	assert.Equal(t, 2, len(ids))

	// Test whitespace trimming
	err = tags.Add(3, "  spaces  ")
	assert.NilError(t, err)

	ids = tags.forward["spaces"]
	if len(ids) != 1 || ids[0] != 3 {
		t.Errorf("Expected tag 'spaces' to be trimmed and normalized")
	}

	// Test negative ID
	err = tags.Add(-1, "negative")
	assert.NotNilError(t, err)

	// Test empty tag
	err = tags.Add(4, "")
	assert.NotNilError(t, err)

	// Test whitespace-only tag
	err = tags.Add(4, "   ")
	assert.NotNilError(t, err)

	// Verify isPersisted is false after adding
	assert.Equal(t, false, tags.isPersisted)
}

func TestGetIDs(t *testing.T) {
	path := createTempFile(t)
	tags, err := New(path)
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	// Test getting IDs for non-existent tag
	ids, err := tags.GetIDs("nonexistent")
	assert.NilError(t, err)
	if ids != nil {
		t.Errorf("Expected nil for non-existent tag, got %v", ids)
	}

	// Add tags
	tags.Add(1, "test")
	tags.Add(2, "test")
	tags.Add(3, "other")

	// Test getting IDs for existing tag
	ids, err = tags.GetIDs("test")
	assert.NilError(t, err)
	assert.Equal(t, 2, len(ids))

	// Test case insensitivity
	ids, err = tags.GetIDs("TEST")
	assert.NilError(t, err)
	assert.Equal(t, 2, len(ids))

	// Test that returned slice is a copy
	ids[0] = 999
	originalIDs := tags.forward["test"]
	if originalIDs[0] == 999 {
		t.Error("Modifying returned slice should not affect internal state")
	}
}

func TestGetTags(t *testing.T) {
	path := createTempFile(t)
	tags, err := New(path)
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	// Test getting tags for non-existent ID
	tagList, err := tags.GetTags(999)
	assert.NilError(t, err)
	if tagList != nil {
		t.Errorf("Expected nil for non-existent ID, got %v", tagList)
	}

	// Add tags
	tags.Add(1, "tag1")
	tags.Add(1, "tag2")
	tags.Add(2, "tag3")

	// Test getting tags for existing ID
	tagList, err = tags.GetTags(1)
	assert.NilError(t, err)
	assert.Equal(t, 2, len(tagList))

	// Test that returned slice is a copy
	tagList[0] = "modified"
	originalTags := tags.reverse[1]
	if originalTags[0] == "modified" {
		t.Error("Modifying returned slice should not affect internal state")
	}
}

func TestRemove(t *testing.T) {
	path := createTempFile(t)
	tags, err := New(path)
	assert.NilError(t, err)

	// Test removing non-existent tag
	err = tags.Remove(1, "nonexistent")
	assert.ErrorIs(t, ErrTagNotFound, err)

	// Add tags
	tags.Add(1, "test")
	tags.Add(2, "test")
	tags.Add(1, "other")

	// Test removing tag from ID
	err = tags.Remove(1, "test")
	assert.NilError(t, err)

	// Verify forward map
	ids := tags.forward["test"]
	if len(ids) != 1 || ids[0] != 2 {
		t.Errorf("Expected forward map to have [2] for 'test', got %v", ids)
	}

	// Verify reverse map
	tagList := tags.reverse[1]
	if len(tagList) != 1 || tagList[0] != "other" {
		t.Errorf("Expected reverse map to have ['other'] for ID 1, got %v", tagList)
	}

	// Test removing last occurrence of tag
	err = tags.Remove(2, "test")
	assert.NilError(t, err)

	// Verify tag is deleted from forward map
	_, exists := tags.forward["test"]
	if exists {
		t.Error("Expected 'test' tag to be deleted from forward map")
	}

	// Test case insensitivity
	tags.Add(3, "case")
	err = tags.Remove(3, "CASE")
	assert.NilError(t, err)

	// Test whitespace trimming
	tags.Add(4, "spaces")
	err = tags.Remove(4, "  spaces  ")
	assert.NilError(t, err)

	// Test negative ID
	err = tags.Remove(-1, "tag")
	assert.NotNilError(t, err)

	// Test empty tag
	err = tags.Remove(1, "")
	assert.NotNilError(t, err)

	// Test removing tag that doesn't exist for this ID
	tags.Add(5, "tag5")
	err = tags.Remove(6, "tag5")
	assert.ErrorIs(t, ErrTagNotFound, err)

	// Verify isPersisted is false after removing
	tags.isPersisted = true
	tags.Remove(1, "other")
	assert.Equal(t, false, tags.isPersisted)
}

func TestGetAllTags(t *testing.T) {
	path := createTempFile(t)
	tags, err := New(path)
	assert.NilError(t, err)

	// Test empty tags
	allTags, err := tags.GetAllTags()
	assert.NilError(t, err)
	assert.Equal(t, 0, len(allTags))

	// Add tags
	tags.Add(1, "tag1")
	tags.Add(2, "tag2")
	tags.Add(3, "tag1") // Duplicate tag, different ID

	allTags, err = tags.GetAllTags()
	assert.NilError(t, err)
	assert.Equal(t, 2, len(allTags))

	// Verify tags are returned
	tagMap := make(map[string]bool)
	for _, tag := range allTags {
		tagMap[tag] = true
	}
	if !tagMap["tag1"] || !tagMap["tag2"] {
		t.Errorf("Expected tags 'tag1' and 'tag2', got %v", allTags)
	}
}

func TestDestroy(t *testing.T) {
	path := createTempFile(t)
	tags, err := New(path)
	assert.NilError(t, err)

	// Add tags
	tags.Add(1, "test")
	tags.Add(2, "other")

	// Destroy
	err = tags.Destroy()
	assert.NilError(t, err)

	// Verify maps are empty
	assert.Equal(t, 0, len(tags.forward))
	assert.Equal(t, 0, len(tags.reverse))

	// Verify isPersisted is true
	assert.Equal(t, true, tags.isPersisted)

	// Verify storage is empty
	size, err := tags.storage.Size()
	assert.NilError(t, err)
	assert.Equal(t, int64(0), size)
}

func TestCount(t *testing.T) {
	path := createTempFile(t)
	tags, err := New(path)
	assert.NilError(t, err)

	// Test empty tags
	count, err := tags.Count()
	assert.NilError(t, err)
	assert.Equal(t, 0, count)

	// Add tags to different IDs
	tags.Add(1, "tag1")
	tags.Add(1, "tag2")
	tags.Add(2, "tag3")

	count, err = tags.Count()
	assert.NilError(t, err)
	assert.Equal(t, 2, count)

	// Add tag to new ID
	tags.Add(3, "tag4")

	count, err = tags.Count()
	assert.NilError(t, err)
	assert.Equal(t, 3, count)

	// Remove all tags from an ID
	tags.Remove(1, "tag1")
	tags.Remove(1, "tag2")

	count, err = tags.Count()
	assert.NilError(t, err)
	assert.Equal(t, 2, count)
}

func TestFlush(t *testing.T) {
	path := createTempFile(t)
	tags, err := New(path)
	assert.NilError(t, err)

	// Add tags
	tags.Add(1, "test")
	tags.Add(2, "other")

	// Verify isPersisted is false
	assert.Equal(t, false, tags.isPersisted)

	// Flush
	err = tags.Flush()
	assert.NilError(t, err)

	// Verify isPersisted is true
	assert.Equal(t, true, tags.isPersisted)

	// Verify storage is not empty
	size, err := tags.storage.Size()
	assert.NilError(t, err)
	if size == 0 {
		t.Error("Expected storage size to be greater than 0 after Flush()")
	}

	// Test flushing when already persisted (should be no-op)
	err = tags.Flush()
	assert.NilError(t, err)
}

func TestPersistenceAndLoad(t *testing.T) {
	path := createTempFile(t)

	// Create and populate tags
	tags1, err := New(path)
	assert.NilError(t, err)

	tags1.Add(1, "tag1")
	tags1.Add(1, "tag2")
	tags1.Add(2, "tag3")
	tags1.Add(3, "tag1") // Shared tag

	err = tags1.Flush()
	assert.NilError(t, err)

	// Create new tags instance from same path
	tags2, err := New(path)
	assert.NilError(t, err)

	// Force load
	tags2.isLoaded = false
	err = tags2.load()
	assert.NilError(t, err)

	// Verify forward map
	ids := tags2.forward["tag1"]
	if len(ids) != 2 {
		t.Errorf("Expected 2 IDs for 'tag1', got %d", len(ids))
	}

	ids = tags2.forward["tag2"]
	if len(ids) != 1 || ids[0] != 1 {
		t.Errorf("Expected [1] for 'tag2', got %v", ids)
	}

	ids = tags2.forward["tag3"]
	if len(ids) != 1 || ids[0] != 2 {
		t.Errorf("Expected [2] for 'tag3', got %v", ids)
	}

	// Verify reverse map
	tagList := tags2.reverse[1]
	if len(tagList) != 2 {
		t.Errorf("Expected 2 tags for ID 1, got %d", len(tagList))
	}

	tagList = tags2.reverse[2]
	if len(tagList) != 1 || tagList[0] != "tag3" {
		t.Errorf("Expected ['tag3'] for ID 2, got %v", tagList)
	}

	tagList = tags2.reverse[3]
	if len(tagList) != 1 || tagList[0] != "tag1" {
		t.Errorf("Expected ['tag1'] for ID 3, got %v", tagList)
	}

	// Verify isLoaded flag
	assert.Equal(t, true, tags2.isLoaded)
}

func TestLoadEmptyFile(t *testing.T) {
	path := createTempFile(t)

	// Create tags with empty file
	tags, err := New(path)
	assert.NilError(t, err)

	// Force load on empty file
	tags.isLoaded = false
	err = tags.load()
	assert.NilError(t, err)

	assert.Equal(t, true, tags.isLoaded)

	if len(tags.forward) != 0 || len(tags.reverse) != 0 {
		t.Error("Maps should be empty after loading empty file")
	}
}

func TestLazyLoad(t *testing.T) {
	path := createTempFile(t)

	// Create and populate tags
	tags1, err := New(path)
	assert.NilError(t, err)

	tags1.Add(1, "test")
	tags1.Flush()

	// Create new instance
	tags2, err := New(path)
	assert.NilError(t, err)

	// Mark as not loaded to simulate lazy loading
	tags2.isLoaded = false

	// Calling Add should trigger load
	err = tags2.Add(2, "new")
	assert.NilError(t, err)

	assert.Equal(t, true, tags2.isLoaded)

	// Verify old data was loaded
	ids, _ := tags2.GetIDs("test")
	if len(ids) != 1 || ids[0] != 1 {
		t.Error("Previously persisted data should be loaded")
	}

	// Verify new data was added
	ids, _ = tags2.GetIDs("new")
	if len(ids) != 1 || ids[0] != 2 {
		t.Error("New data should be added after load")
	}
}

func TestMultipleTagsPerID(t *testing.T) {
	path := createTempFile(t)
	tags, err := New(path)
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	// Add multiple tags to same ID
	tags.Add(1, "tag1")
	tags.Add(1, "tag2")
	tags.Add(1, "tag3")

	// Verify GetTags
	tagList, err := tags.GetTags(1)
	if err != nil {
		t.Errorf("GetTags() failed: %v", err)
	}
	if len(tagList) != 3 {
		t.Errorf("Expected 3 tags, got %d", len(tagList))
	}

	// Verify each tag points back to ID 1
	for _, tag := range tagList {
		ids, _ := tags.GetIDs(tag)
		if len(ids) != 1 || ids[0] != 1 {
			t.Errorf("Tag '%s' should point to ID 1, got %v", tag, ids)
		}
	}
}

func TestMultipleIDsPerTag(t *testing.T) {
	path := createTempFile(t)
	tags, err := New(path)
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	// Add same tag to multiple IDs
	tags.Add(1, "shared")
	tags.Add(2, "shared")
	tags.Add(3, "shared")

	// Verify GetIDs
	ids, err := tags.GetIDs("shared")
	if err != nil {
		t.Errorf("GetIDs() failed: %v", err)
	}
	if len(ids) != 3 {
		t.Errorf("Expected 3 IDs, got %d", len(ids))
	}

	// Verify each ID has the tag
	for _, id := range ids {
		tagList, _ := tags.GetTags(id)
		found := false
		for _, tag := range tagList {
			if tag == "shared" {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("ID %d should have tag 'shared'", id)
		}
	}
}

func TestComplexScenario(t *testing.T) {
	path := createTempFile(t)
	tags, err := New(path)
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	// Complex scenario: multiple IDs, multiple tags
	tags.Add(1, "go")
	tags.Add(1, "backend")
	tags.Add(2, "go")
	tags.Add(2, "frontend")
	tags.Add(3, "python")
	tags.Add(3, "backend")

	// Test Count
	count, _ := tags.Count()
	if count != 3 {
		t.Errorf("Expected count 3, got %d", count)
	}

	// Test GetAllTags
	allTags, _ := tags.GetAllTags()
	if len(allTags) != 4 {
		t.Errorf("Expected 4 unique tags, got %d", len(allTags))
	}

	// Test GetIDs for shared tag
	ids, _ := tags.GetIDs("go")
	if len(ids) != 2 {
		t.Errorf("Expected 2 IDs for 'go', got %d", len(ids))
	}

	ids, _ = tags.GetIDs("backend")
	if len(ids) != 2 {
		t.Errorf("Expected 2 IDs for 'backend', got %d", len(ids))
	}

	// Flush and reload
	tags.Flush()

	tags2, _ := New(path)
	tags2.isLoaded = false
	tags2.load()

	// Verify data after reload
	count, _ = tags2.Count()
	if count != 3 {
		t.Errorf("Expected count 3 after reload, got %d", count)
	}

	allTags, _ = tags2.GetAllTags()
	if len(allTags) != 4 {
		t.Errorf("Expected 4 unique tags after reload, got %d", len(allTags))
	}

	// Remove a tag and verify
	tags2.Remove(1, "backend")
	tagList, _ := tags2.GetTags(1)
	if len(tagList) != 1 || tagList[0] != "go" {
		t.Errorf("Expected ['go'] for ID 1 after removal, got %v", tagList)
	}

	ids, _ = tags2.GetIDs("backend")
	if len(ids) != 1 || ids[0] != 3 {
		t.Errorf("Expected [3] for 'backend' after removal, got %v", ids)
	}
}

func TestRemoveAll(t *testing.T) {
	path := createTempFile(t)
	tags, err := New(path)
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	// Add tags for multiple IDs
	tags.Add(1, "tag1")
	tags.Add(1, "tag2")
	tags.Add(1, "tag3")
	tags.Add(2, "tag2") // Shared tag
	tags.Add(3, "tag4")

	// Verify initial state for ID 1
	tagList, err := tags.GetTags(1)
	if err != nil {
		t.Fatalf("GetTags(1) failed: %v", err)
	}
	if len(tagList) != 3 {
		t.Errorf("Expected 3 tags for ID 1, got %d", len(tagList))
	}

	// Remove all tags for ID 1
	err = tags.RemoveAll(1)
	if err != nil {
		t.Errorf("RemoveAll(1) failed: %v", err)
	}

	// Verify ID 1 has no tags
	tagList, err = tags.GetTags(1)
	if err != nil {
		t.Fatalf("GetTags(1) failed: %v", err)
	}
	if len(tagList) != 0 {
		t.Errorf("Expected 0 tags for ID 1 after RemoveAll, got %d", len(tagList))
	}

	// Verify ID 1 is removed from reverse map
	if _, ok := tags.reverse[1]; ok {
		t.Error("ID 1 should be removed from reverse map")
	}

	// Verify ID 1 is removed from forward map for its tags
	ids, _ := tags.GetIDs("tag1")
	assert.Equal(t, 0, len(ids))
	if len(ids) != 0 {
		t.Errorf("Expected 0 IDs for 'tag1', got %v", ids)
	}

	// Verify shared tag 'tag2' still exists for ID 2
	ids, _ = tags.GetIDs("tag2")
	assert.Equal(t, 1, len(ids))
	assert.Equal(t, 2, ids[0])

	// Verify ID 2 is unaffected
	tagList, _ = tags.GetTags(2)
	assert.Equal(t, 1, len(tagList))
	assert.Equal(t, "tag2", tagList[0])

	// Verify ID 3 is unaffected
	tagList, _ = tags.GetTags(3)
	assert.Equal(t, 1, len(tagList))
	assert.Equal(t, "tag4", tagList[0])

	// Test RemoveAll for non-existent ID (should be no-op)
	err = tags.RemoveAll(999)
	assert.NilError(t, err)

	// Test RemoveAll for ID with no tags (should be no-op)
	tags.Add(4, "temp")
	tags.Remove(4, "temp") // ID 4 exists in reverse map? No, Remove deletes it if empty.
	// Let's ensure ID 4 is clean
	err = tags.RemoveAll(4)
	assert.NilError(t, err)

	// Verify isPersisted is false
	tags.isPersisted = true
	tags.Add(5, "tag5")
	tags.isPersisted = true // Reset to true to test RemoveAll
	tags.RemoveAll(5)
	assert.Equal(t, false, tags.isPersisted)
}
