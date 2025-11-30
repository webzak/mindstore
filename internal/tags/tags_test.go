package tags

import (
	"testing"

	"github.com/webzak/mindstore/internal/testutil"
	"github.com/webzak/mindstore/internal/testutil/assert"
)

func TestNew(t *testing.T) {
	path := testutil.CreateTempFile(t, "tags.dat")

	tags, err := New(path)
	assert.NilError(t, err)

	assert.NotNil(t, tags, "New() returned nil tags")
	assert.NotNil(t, tags.forward, "forward map not initialized")
	assert.NotNil(t, tags.reverse, "reverse map not initialized")
	assert.Equal(t, true, tags.isPersisted)
	assert.Equal(t, true, tags.isLoaded)
}

func TestAdd(t *testing.T) {
	path := testutil.CreateTempFile(t, "tags.dat")
	tags, err := New(path)
	assert.NilError(t, err)

	// Test adding a tag
	err = tags.Add(1, "test")
	assert.NilError(t, err)

	// Verify forward map
	ids := tags.forward["test"]
	assert.Equal(t, 1, len(ids))
	assert.Equal(t, 1, ids[0])

	// Verify reverse map
	tagList := tags.reverse[1]
	assert.Equal(t, 1, len(tagList))
	assert.Equal(t, "test", tagList[0])

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
	assert.Equal(t, 1, len(ids))
	assert.Equal(t, 3, ids[0])

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
	path := testutil.CreateTempFile(t, "tags.dat")
	tags, err := New(path)
	assert.NilError(t, err)

	// Test getting IDs for non-existent tag
	ids, err := tags.GetIDs("nonexistent")
	assert.NilError(t, err)
	assert.Equal(t, true, ids == nil)

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
	assert.Equal(t, false, originalIDs[0] == 999)
}

func TestGetTags(t *testing.T) {
	path := testutil.CreateTempFile(t, "tags.dat")
	tags, err := New(path)
	assert.NilError(t, err)

	// Test getting tags for non-existent ID
	tagList, err := tags.GetTags(999)
	assert.NilError(t, err)
	assert.Equal(t, true, tagList == nil)

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
	assert.Equal(t, false, originalTags[0] == "modified")
}

func TestRemove(t *testing.T) {
	path := testutil.CreateTempFile(t, "tags.dat")
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
	assert.Equal(t, 1, len(ids))
	assert.Equal(t, 2, ids[0])

	// Verify reverse map
	tagList := tags.reverse[1]
	assert.Equal(t, 1, len(tagList))
	assert.Equal(t, "other", tagList[0])

	// Test removing last occurrence of tag
	err = tags.Remove(2, "test")
	assert.NilError(t, err)

	// Verify tag is deleted from forward map
	_, exists := tags.forward["test"]
	assert.Equal(t, false, exists)

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
	path := testutil.CreateTempFile(t, "tags.dat")
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
	assert.Equal(t, true, tagMap["tag1"])
	assert.Equal(t, true, tagMap["tag2"])
}

func TestTruncate(t *testing.T) {
	path := testutil.CreateTempFile(t, "tags.dat")
	tags, err := New(path)
	assert.NilError(t, err)

	// Add tags
	tags.Add(1, "test")
	tags.Add(2, "other")

	err = tags.Truncate()
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
	path := testutil.CreateTempFile(t, "tags.dat")
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
	path := testutil.CreateTempFile(t, "tags.dat")
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
	assert.Equal(t, false, size == 0)

	// Test flushing when already persisted (should be no-op)
	err = tags.Flush()
	assert.NilError(t, err)
}

func TestPersistenceAndLoad(t *testing.T) {
	path := testutil.CreateTempFile(t, "tags.dat")

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
	assert.Equal(t, 2, len(ids))

	ids = tags2.forward["tag2"]
	assert.Equal(t, 1, len(ids))
	assert.Equal(t, 1, ids[0])

	ids = tags2.forward["tag3"]
	assert.Equal(t, 1, len(ids))
	assert.Equal(t, 2, ids[0])

	// Verify reverse map
	tagList := tags2.reverse[1]
	assert.Equal(t, 2, len(tagList))

	tagList = tags2.reverse[2]
	assert.Equal(t, 1, len(tagList))
	assert.Equal(t, "tag3", tagList[0])

	tagList = tags2.reverse[3]
	assert.Equal(t, 1, len(tagList))
	assert.Equal(t, "tag1", tagList[0])

	// Verify isLoaded flag
	assert.Equal(t, true, tags2.isLoaded)
}

func TestLoadEmptyFile(t *testing.T) {
	path := testutil.CreateTempFile(t, "tags.dat")

	// Create tags with empty file
	tags, err := New(path)
	assert.NilError(t, err)

	// Force load on empty file
	tags.isLoaded = false
	err = tags.load()
	assert.NilError(t, err)

	assert.Equal(t, true, tags.isLoaded)

	assert.Equal(t, 0, len(tags.forward))
	assert.Equal(t, 0, len(tags.reverse))
}

func TestLazyLoad(t *testing.T) {
	path := testutil.CreateTempFile(t, "tags.dat")

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
	assert.Equal(t, 1, len(ids))
	assert.Equal(t, 1, ids[0])

	// Verify new data was added
	ids, _ = tags2.GetIDs("new")
	assert.Equal(t, 1, len(ids))
	assert.Equal(t, 2, ids[0])
}

func TestMultipleTagsPerID(t *testing.T) {
	path := testutil.CreateTempFile(t, "tags.dat")
	tags, err := New(path)
	assert.NilError(t, err)

	// Add multiple tags to same ID
	tags.Add(1, "tag1")
	tags.Add(1, "tag2")
	tags.Add(1, "tag3")

	// Verify GetTags
	tagList, err := tags.GetTags(1)
	assert.NilError(t, err)
	assert.Equal(t, 3, len(tagList))

	// Verify each tag points back to ID 1
	for _, tag := range tagList {
		ids, _ := tags.GetIDs(tag)
		assert.Equal(t, 1, len(ids))
		assert.Equal(t, 1, ids[0])
	}
}

func TestMultipleIDsPerTag(t *testing.T) {
	path := testutil.CreateTempFile(t, "tags.dat")
	tags, err := New(path)
	assert.NilError(t, err)

	// Add same tag to multiple IDs
	tags.Add(1, "shared")
	tags.Add(2, "shared")
	tags.Add(3, "shared")

	// Verify GetIDs
	ids, err := tags.GetIDs("shared")
	assert.NilError(t, err)
	assert.Equal(t, 3, len(ids))

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
		assert.Equal(t, true, found)
	}
}

func TestComplexScenario(t *testing.T) {
	path := testutil.CreateTempFile(t, "tags.dat")
	tags, err := New(path)
	assert.NilError(t, err)

	// Complex scenario: multiple IDs, multiple tags
	tags.Add(1, "go")
	tags.Add(1, "backend")
	tags.Add(2, "go")
	tags.Add(2, "frontend")
	tags.Add(3, "python")
	tags.Add(3, "backend")

	// Test Count
	count, _ := tags.Count()
	assert.Equal(t, 3, count)

	// Test GetAllTags
	allTags, _ := tags.GetAllTags()
	assert.Equal(t, 4, len(allTags))

	// Test GetIDs for shared tag
	ids, _ := tags.GetIDs("go")
	assert.Equal(t, 2, len(ids))

	ids, _ = tags.GetIDs("backend")
	assert.Equal(t, 2, len(ids))

	// Flush and reload
	tags.Flush()

	tags2, _ := New(path)
	tags2.isLoaded = false
	tags2.load()

	// Verify data after reload
	count, _ = tags2.Count()
	assert.Equal(t, 3, count)

	allTags, _ = tags2.GetAllTags()
	assert.Equal(t, 4, len(allTags))

	// Remove a tag and verify
	tags2.Remove(1, "backend")
	tagList, _ := tags2.GetTags(1)
	assert.Equal(t, 1, len(tagList))
	assert.Equal(t, "go", tagList[0])

	ids, _ = tags2.GetIDs("backend")
	assert.Equal(t, 1, len(ids))
	assert.Equal(t, 3, ids[0])
}

func TestRemoveAll(t *testing.T) {
	path := testutil.CreateTempFile(t, "tags.dat")
	tags, err := New(path)
	assert.NilError(t, err)

	// Add tags for multiple IDs
	tags.Add(1, "tag1")
	tags.Add(1, "tag2")
	tags.Add(1, "tag3")
	tags.Add(2, "tag2") // Shared tag
	tags.Add(3, "tag4")

	// Verify initial state for ID 1
	tagList, err := tags.GetTags(1)
	assert.NilError(t, err)
	assert.Equal(t, 3, len(tagList))

	// Remove all tags for ID 1
	err = tags.RemoveAll(1)
	assert.NilError(t, err)

	// Verify ID 1 has no tags
	tagList, err = tags.GetTags(1)
	assert.NilError(t, err)
	assert.Equal(t, 0, len(tagList))

	// Verify ID 1 is removed from reverse map
	_, ok := tags.reverse[1]
	assert.Equal(t, false, ok)

	// Verify ID 1 is removed from forward map for its tags
	ids, _ := tags.GetIDs("tag1")
	assert.Equal(t, 0, len(ids))

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
