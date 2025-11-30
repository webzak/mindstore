package dataset

import (
	"testing"

	"github.com/webzak/mindstore/internal/testutil/assert"
)

func TestAddTags(t *testing.T) {
	tmpDir := t.TempDir()
	ds, err := Open(tmpDir, "test_add_tags", DefaultOptions())
	assert.NilError(t, err)
	defer ds.Close()

	// Append an item
	item := &Item{
		Data:           []byte("test data"),
		DataDescriptor: 1,
	}
	id, err := ds.Append(item)
	assert.NilError(t, err)

	// Add single tag
	err = ds.AddTags(id, "tag1")
	assert.NilError(t, err)

	// Verify tag was added
	tags, err := ds.GetTagsByID(id)
	assert.NilError(t, err)
	assert.Equal(t, 1, len(tags))
	assert.Equal(t, "tag1", tags[0])

	// Add multiple tags at once
	err = ds.AddTags(id, "tag2", "tag3", "tag4")
	assert.NilError(t, err)

	// Verify all tags
	tags, err = ds.GetTagsByID(id)
	assert.NilError(t, err)
	assert.Equal(t, 4, len(tags))
}

func TestAddTagsInvalidID(t *testing.T) {
	tmpDir := t.TempDir()
	ds, err := Open(tmpDir, "test_add_tags_invalid", DefaultOptions())
	assert.NilError(t, err)
	defer ds.Close()

	// Try to add tags to non-existent ID
	err = ds.AddTags(0, "tag1")
	assert.ErrorIs(t, ErrInvalidRecordID, err)

	err = ds.AddTags(-1, "tag1")
	assert.ErrorIs(t, ErrInvalidRecordID, err)

	err = ds.AddTags(999, "tag1")
	assert.ErrorIs(t, ErrInvalidRecordID, err)
}

func TestRemoveTags(t *testing.T) {
	tmpDir := t.TempDir()
	ds, err := Open(tmpDir, "test_remove_tags", DefaultOptions())
	assert.NilError(t, err)
	defer ds.Close()

	// Append item with tags
	item := &Item{
		Data:           []byte("test data"),
		DataDescriptor: 1,
		Tags:           []string{"tag1", "tag2", "tag3", "tag4"},
	}
	id, err := ds.Append(item)
	assert.NilError(t, err)

	// Verify all tags exist
	tags, err := ds.GetTagsByID(id)
	assert.NilError(t, err)
	assert.Equal(t, 4, len(tags))

	// Remove single tag
	err = ds.RemoveTags(id, "tag2")
	assert.NilError(t, err)

	tags, err = ds.GetTagsByID(id)
	assert.NilError(t, err)
	assert.Equal(t, 3, len(tags))

	// Remove multiple tags
	err = ds.RemoveTags(id, "tag1", "tag3")
	assert.NilError(t, err)

	tags, err = ds.GetTagsByID(id)
	assert.NilError(t, err)
	assert.Equal(t, 1, len(tags))
	assert.Equal(t, "tag4", tags[0])

	// Remove last tag
	err = ds.RemoveTags(id, "tag4")
	assert.NilError(t, err)

	tags, err = ds.GetTagsByID(id)
	assert.NilError(t, err)
	assert.Equal(t, 0, len(tags))
}

func TestRemoveTagsInvalidID(t *testing.T) {
	tmpDir := t.TempDir()
	ds, err := Open(tmpDir, "test_remove_tags_invalid", DefaultOptions())
	assert.NilError(t, err)
	defer ds.Close()

	// Try to remove tags from non-existent ID
	err = ds.RemoveTags(0, "tag1")
	assert.ErrorIs(t, ErrInvalidRecordID, err)

	err = ds.RemoveTags(-1, "tag1")
	assert.ErrorIs(t, ErrInvalidRecordID, err)

	err = ds.RemoveTags(999, "tag1")
	assert.ErrorIs(t, ErrInvalidRecordID, err)
}

func TestGetIDsByTag(t *testing.T) {
	tmpDir := t.TempDir()
	ds, err := Open(tmpDir, "test_get_ids_by_tag", DefaultOptions())
	assert.NilError(t, err)
	defer ds.Close()

	// Create multiple items with different tags
	items := []struct {
		data []byte
		tags []string
	}{
		{[]byte("item1"), []string{"common", "tag1"}},
		{[]byte("item2"), []string{"common", "tag2"}},
		{[]byte("item3"), []string{"common", "tag3"}},
		{[]byte("item4"), []string{"tag4"}},
		{[]byte("item5"), []string{"tag5"}},
	}

	ids := make([]int, len(items))
	for i, itemData := range items {
		item := &Item{
			Data:           itemData.data,
			DataDescriptor: 1,
			Tags:           itemData.tags,
		}
		id, err := ds.Append(item)
		assert.NilError(t, err)
		ids[i] = id
	}

	// Get IDs by common tag
	commonIDs, err := ds.GetIDsByTag("common")
	assert.NilError(t, err)
	assert.Equal(t, 3, len(commonIDs))

	// Verify the IDs are correct
	expectedCommon := []int{ids[0], ids[1], ids[2]}
	assert.DeepEqual(t, expectedCommon, commonIDs)

	// Get IDs by unique tag
	tag4IDs, err := ds.GetIDsByTag("tag4")
	assert.NilError(t, err)
	assert.Equal(t, 1, len(tag4IDs))
	assert.Equal(t, ids[3], tag4IDs[0])

	// Get IDs by non-existent tag
	noIDs, err := ds.GetIDsByTag("nonexistent")
	assert.NilError(t, err)
	assert.Equal(t, 0, len(noIDs))
}

func TestGetIDsByTagCaseInsensitive(t *testing.T) {
	tmpDir := t.TempDir()
	ds, err := Open(tmpDir, "test_get_ids_case", DefaultOptions())
	assert.NilError(t, err)
	defer ds.Close()

	// Add item with mixed-case tag
	item := &Item{
		Data:           []byte("test"),
		DataDescriptor: 1,
		Tags:           []string{"MixedCase"},
	}
	id, err := ds.Append(item)
	assert.NilError(t, err)

	// Search with different case variations
	tests := []string{"mixedcase", "MIXEDCASE", "MixedCase", "mIxEdCaSe"}
	for _, tagVariant := range tests {
		ids, err := ds.GetIDsByTag(tagVariant)
		assert.NilError(t, err)
		assert.Equal(t, 1, len(ids))
		assert.Equal(t, id, ids[0])
	}
}

func TestGetTagsByID(t *testing.T) {
	tmpDir := t.TempDir()
	ds, err := Open(tmpDir, "test_get_tags_by_id", DefaultOptions())
	assert.NilError(t, err)
	defer ds.Close()

	// Append item with tags
	expectedTags := []string{"tag1", "tag2", "tag3"}
	item := &Item{
		Data:           []byte("test data"),
		DataDescriptor: 1,
		Tags:           expectedTags,
	}
	id, err := ds.Append(item)
	assert.NilError(t, err)

	// Get tags by ID
	tags, err := ds.GetTagsByID(id)
	assert.NilError(t, err)
	assert.Equal(t, len(expectedTags), len(tags))
	assert.DeepEqual(t, expectedTags, tags)
}

func TestGetTagsByIDInvalidID(t *testing.T) {
	tmpDir := t.TempDir()
	ds, err := Open(tmpDir, "test_get_tags_invalid", DefaultOptions())
	assert.NilError(t, err)
	defer ds.Close()

	// Try to get tags for non-existent ID
	_, err = ds.GetTagsByID(0)
	assert.ErrorIs(t, ErrInvalidRecordID, err)

	_, err = ds.GetTagsByID(-1)
	assert.ErrorIs(t, ErrInvalidRecordID, err)

	_, err = ds.GetTagsByID(999)
	assert.ErrorIs(t, ErrInvalidRecordID, err)
}

func TestGetTagsByIDNoTags(t *testing.T) {
	tmpDir := t.TempDir()
	ds, err := Open(tmpDir, "test_get_tags_none", DefaultOptions())
	assert.NilError(t, err)
	defer ds.Close()

	// Append item without tags
	item := &Item{
		Data:           []byte("test data"),
		DataDescriptor: 1,
	}
	id, err := ds.Append(item)
	assert.NilError(t, err)

	// Get tags - should return empty slice
	tags, err := ds.GetTagsByID(id)
	assert.NilError(t, err)
	assert.Equal(t, 0, len(tags))
}

func TestTagsAfterFlush(t *testing.T) {
	tmpDir := t.TempDir()
	ds, err := Open(tmpDir, "test_tags_flush", DefaultOptions())
	assert.NilError(t, err)
	defer ds.Close()

	// Add item with tags
	item := &Item{
		Data:           []byte("test data"),
		DataDescriptor: 1,
		Tags:           []string{"tag1", "tag2"},
	}
	id, err := ds.Append(item)
	assert.NilError(t, err)

	// Flush
	assert.NilError(t, ds.Flush())

	// Verify tags still accessible
	tags, err := ds.GetTagsByID(id)
	assert.NilError(t, err)
	assert.Equal(t, 2, len(tags))

	// Add more tags after flush
	err = ds.AddTags(id, "tag3")
	assert.NilError(t, err)

	tags, err = ds.GetTagsByID(id)
	assert.NilError(t, err)
	assert.Equal(t, 3, len(tags))
}

func TestMultipleItemsSameTags(t *testing.T) {
	tmpDir := t.TempDir()
	ds, err := Open(tmpDir, "test_multiple_same_tags", DefaultOptions())
	assert.NilError(t, err)
	defer ds.Close()

	// Create multiple items with the same tag
	sharedTag := "shared"
	numItems := 10
	ids := make([]int, numItems)

	for i := 0; i < numItems; i++ {
		item := &Item{
			Data:           []byte("test"),
			DataDescriptor: 1,
			Tags:           []string{sharedTag},
		}
		id, err := ds.Append(item)
		assert.NilError(t, err)
		ids[i] = id
	}

	// Get all IDs with the shared tag
	retrievedIDs, err := ds.GetIDsByTag(sharedTag)
	assert.NilError(t, err)
	assert.Equal(t, numItems, len(retrievedIDs))
	assert.DeepEqual(t, ids, retrievedIDs)
}

func TestAddDuplicateTag(t *testing.T) {
	tmpDir := t.TempDir()
	ds, err := Open(tmpDir, "test_duplicate_tag", DefaultOptions())
	assert.NilError(t, err)
	defer ds.Close()

	// Add item with tag
	item := &Item{
		Data:           []byte("test"),
		DataDescriptor: 1,
		Tags:           []string{"tag1"},
	}
	id, err := ds.Append(item)
	assert.NilError(t, err)

	// Try to add same tag again
	err = ds.AddTags(id, "tag1")
	assert.NilError(t, err)

	// The behavior depends on the underlying tags implementation
	// but it should not error
	tags, err := ds.GetTagsByID(id)
	assert.NilError(t, err)
	if len(tags) == 0 {
		t.Error("expected at least one tag")
	}
}
