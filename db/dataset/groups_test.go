package dataset

import (
	"testing"

	"github.com/webzak/mindstore/internal/testutil/assert"
)

func TestSetGroup(t *testing.T) {
	tmpDir := t.TempDir()
	ds, err := Open(tmpDir, "test_set_group", DefaultOptions())
	assert.NilError(t, err)
	defer ds.Close()

	// Create test records
	item1, err := ds.Append(Item{Data: []byte("test1"), DataDescriptor: 1})
	assert.NilError(t, err)

	item2, err := ds.Append(Item{Data: []byte("test2"), DataDescriptor: 1})
	assert.NilError(t, err)

	// Create a group by appending with GroupID = -1
	item3, err := ds.Append(Item{
		Data:           []byte("test3"),
		DataDescriptor: 1,
		GroupID:        -1,
	})
	assert.NilError(t, err)
	groupID := item3.GroupID

	// Verify group was created (groupID should be >= 1)
	if groupID < 1 {
		t.Fatalf("expected groupID >= 1, got %d", groupID)
	}

	// Assign item1 to the group at place 1
	err = ds.SetGroup(item1.ID, groupID, 1)
	assert.NilError(t, err)

	// Read item1 to verify group assignment
	readItem, err := ds.Read(item1.ID, ReadGroup)
	assert.NilError(t, err)
	assert.Equal(t, groupID, readItem.GroupID)
	assert.Equal(t, 1, readItem.GroupPlace)

	// Assign item2 to the group at place 2
	err = ds.SetGroup(item2.ID, groupID, 2)
	assert.NilError(t, err)

	// Read item2 to verify group assignment
	readItem2, err := ds.Read(item2.ID, ReadGroup)
	assert.NilError(t, err)
	assert.Equal(t, groupID, readItem2.GroupID)
	assert.Equal(t, 2, readItem2.GroupPlace)
}

func TestSetGroupMovesBetweenGroups(t *testing.T) {
	tmpDir := t.TempDir()
	ds, err := Open(tmpDir, "test_move_groups", DefaultOptions())
	assert.NilError(t, err)
	defer ds.Close()

	// Create two groups
	item1, err := ds.Append(Item{
		Data:           []byte("group1"),
		DataDescriptor: 1,
		GroupID:        -1,
	})
	assert.NilError(t, err)
	group1 := item1.GroupID

	item2, err := ds.Append(Item{
		Data:           []byte("group2"),
		DataDescriptor: 1,
		GroupID:        -1,
	})
	assert.NilError(t, err)
	group2 := item2.GroupID

	// Create a record and assign to group1
	item3, err := ds.Append(Item{Data: []byte("test"), DataDescriptor: 1})
	assert.NilError(t, err)

	err = ds.SetGroup(item3.ID, group1, 1)
	assert.NilError(t, err)

	// Verify in group1
	readItem, err := ds.Read(item3.ID, ReadGroup)
	assert.NilError(t, err)
	assert.Equal(t, group1, readItem.GroupID)

	// Move to group2
	err = ds.SetGroup(item3.ID, group2, 1)
	assert.NilError(t, err)

	// Verify moved to group2
	readItem, err = ds.Read(item3.ID, ReadGroup)
	assert.NilError(t, err)
	assert.Equal(t, group2, readItem.GroupID)
	assert.Equal(t, 1, readItem.GroupPlace)
}

func TestSetGroupUpdatePlace(t *testing.T) {
	tmpDir := t.TempDir()
	ds, err := Open(tmpDir, "test_update_place", DefaultOptions())
	assert.NilError(t, err)
	defer ds.Close()

	// Create a group with members
	item1, err := ds.Append(Item{
		Data:           []byte("member1"),
		DataDescriptor: 1,
		GroupID:        -1,
	})
	assert.NilError(t, err)
	groupID := item1.GroupID

	item2, err := ds.Append(Item{
		Data:           []byte("member2"),
		DataDescriptor: 1,
		GroupID:        groupID,
		GroupPlace:     1,
	})
	assert.NilError(t, err)

	_, err = ds.Append(Item{
		Data:           []byte("member3"),
		DataDescriptor: 1,
		GroupID:        groupID,
		GroupPlace:     2,
	})
	assert.NilError(t, err)

	// Verify initial place (index in sorted list)
	// Members sorted by Place: item1(0), item2(1), item3(2)
	// So item2 is at index 1
	readItem, err := ds.Read(item2.ID, ReadGroup)
	assert.NilError(t, err)
	assert.Equal(t, 1, readItem.GroupPlace)

	// Update place of item2 from 1 to 3
	err = ds.SetGroup(item2.ID, groupID, 3)
	assert.NilError(t, err)

	// Verify updated place (index in sorted list)
	// Now members sorted by Place: item1(0), item3(2), item2(3)
	// So item2 is at index 2 (not 3!)
	readItem, err = ds.Read(item2.ID, ReadGroup)
	assert.NilError(t, err)
	assert.Equal(t, 2, readItem.GroupPlace)
}

func TestSetGroupInvalidID(t *testing.T) {
	tmpDir := t.TempDir()
	ds, err := Open(tmpDir, "test_set_group_invalid", DefaultOptions())
	assert.NilError(t, err)
	defer ds.Close()

	// Try with invalid record IDs
	err = ds.SetGroup(0, 1, 0)
	assert.ErrorIs(t, ErrInvalidRecordID, err)

	err = ds.SetGroup(-1, 1, 0)
	assert.ErrorIs(t, ErrInvalidRecordID, err)

	err = ds.SetGroup(999, 1, 0)
	assert.ErrorIs(t, ErrInvalidRecordID, err)
}

func TestSetGroupInvalidGroupID(t *testing.T) {
	tmpDir := t.TempDir()
	ds, err := Open(tmpDir, "test_set_group_invalid_group", DefaultOptions())
	assert.NilError(t, err)
	defer ds.Close()

	// Create a record
	item, err := ds.Append(Item{Data: []byte("test"), DataDescriptor: 1})
	assert.NilError(t, err)

	// Try to assign to non-existent group
	err = ds.SetGroup(item.ID, 999, 0)
	assert.NotNilError(t, err)

	// Try with negative group ID
	err = ds.SetGroup(item.ID, -1, 0)
	assert.NotNilError(t, err)
}

func TestSetGroupDuplicatePlace(t *testing.T) {
	tmpDir := t.TempDir()
	ds, err := Open(tmpDir, "test_duplicate_place", DefaultOptions())
	assert.NilError(t, err)
	defer ds.Close()

	// Create a group with a member at place 0
	item1, err := ds.Append(Item{
		Data:           []byte("member1"),
		DataDescriptor: 1,
		GroupID:        -1,
	})
	assert.NilError(t, err)
	groupID := item1.GroupID

	// Create another record
	item2, err := ds.Append(Item{Data: []byte("member2"), DataDescriptor: 1})
	assert.NilError(t, err)

	// Try to assign to same place (should fail)
	err = ds.SetGroup(item2.ID, groupID, 0)
	assert.NotNilError(t, err)
}

func TestSetGroupAfterFlush(t *testing.T) {
	tmpDir := t.TempDir()
	ds, err := Open(tmpDir, "test_set_group_flush", DefaultOptions())
	assert.NilError(t, err)
	defer ds.Close()

	// Create a group
	item1, err := ds.Append(Item{
		Data:           []byte("test"),
		DataDescriptor: 1,
		GroupID:        -1,
	})
	assert.NilError(t, err)
	groupID := item1.GroupID

	// Flush
	err = ds.Flush()
	assert.NilError(t, err)

	// Verify still accessible
	readItem, err := ds.Read(item1.ID, ReadGroup)
	assert.NilError(t, err)
	assert.Equal(t, groupID, readItem.GroupID)

	// Create and assign new record after flush
	item2, err := ds.Append(Item{Data: []byte("test2"), DataDescriptor: 1})
	assert.NilError(t, err)

	err = ds.SetGroup(item2.ID, groupID, 1)
	assert.NilError(t, err)

	// Verify assignment persisted
	readItem2, err := ds.Read(item2.ID, ReadGroup)
	assert.NilError(t, err)
	assert.Equal(t, groupID, readItem2.GroupID)
	assert.Equal(t, 1, readItem2.GroupPlace)
}

func TestUnsetGroup(t *testing.T) {
	tmpDir := t.TempDir()
	ds, err := Open(tmpDir, "test_unset_group", DefaultOptions())
	assert.NilError(t, err)
	defer ds.Close()

	// Create a record in a group
	item, err := ds.Append(Item{
		Data:           []byte("test"),
		DataDescriptor: 1,
		GroupID:        -1,
	})
	assert.NilError(t, err)

	// Verify in group
	readItem, err := ds.Read(item.ID, ReadGroup)
	assert.NilError(t, err)
	if readItem.GroupID < 1 {
		t.Fatalf("expected groupID >= 1, got %d", readItem.GroupID)
	}

	// Unset group
	err = ds.UnsetGroup(item.ID)
	assert.NilError(t, err)

	// Verify not in group anymore
	readItem, err = ds.Read(item.ID, ReadGroup)
	assert.NilError(t, err)
	assert.Equal(t, 0, readItem.GroupID)
	assert.Equal(t, 0, readItem.GroupPlace)
}

func TestUnsetGroupIdempotent(t *testing.T) {
	tmpDir := t.TempDir()
	ds, err := Open(tmpDir, "test_unset_idempotent", DefaultOptions())
	assert.NilError(t, err)
	defer ds.Close()

	// Create a record without a group
	item, err := ds.Append(Item{Data: []byte("test"), DataDescriptor: 1})
	assert.NilError(t, err)

	// Unset group (should succeed even though not in a group)
	err = ds.UnsetGroup(item.ID)
	assert.NilError(t, err)

	// Call again (idempotent)
	err = ds.UnsetGroup(item.ID)
	assert.NilError(t, err)

	// Verify still not in a group
	readItem, err := ds.Read(item.ID, ReadGroup)
	assert.NilError(t, err)
	assert.Equal(t, 0, readItem.GroupID)
}

func TestUnsetGroupInvalidID(t *testing.T) {
	tmpDir := t.TempDir()
	ds, err := Open(tmpDir, "test_unset_invalid", DefaultOptions())
	assert.NilError(t, err)
	defer ds.Close()

	// Try with invalid record IDs
	err = ds.UnsetGroup(0)
	assert.ErrorIs(t, ErrInvalidRecordID, err)

	err = ds.UnsetGroup(-1)
	assert.ErrorIs(t, ErrInvalidRecordID, err)

	err = ds.UnsetGroup(999)
	assert.ErrorIs(t, ErrInvalidRecordID, err)
}

func TestUnsetGroupAfterFlush(t *testing.T) {
	tmpDir := t.TempDir()
	ds, err := Open(tmpDir, "test_unset_flush", DefaultOptions())
	assert.NilError(t, err)
	defer ds.Close()

	// Create a record in a group
	item, err := ds.Append(Item{
		Data:           []byte("test"),
		DataDescriptor: 1,
		GroupID:        -1,
	})
	assert.NilError(t, err)

	// Flush
	err = ds.Flush()
	assert.NilError(t, err)

	// Unset group
	err = ds.UnsetGroup(item.ID)
	assert.NilError(t, err)

	// Verify removal
	readItem, err := ds.Read(item.ID, ReadGroup)
	assert.NilError(t, err)
	assert.Equal(t, 0, readItem.GroupID)
}

func TestUnsetGroupCleansEmptyGroups(t *testing.T) {
	tmpDir := t.TempDir()
	ds, err := Open(tmpDir, "test_unset_cleans", DefaultOptions())
	assert.NilError(t, err)
	defer ds.Close()

	// Create a group with single member
	item, err := ds.Append(Item{
		Data:           []byte("test"),
		DataDescriptor: 1,
		GroupID:        -1,
	})
	assert.NilError(t, err)
	groupID := item.GroupID

	// Unset the only member (this cleans up the empty group from the map)
	err = ds.UnsetGroup(item.ID)
	assert.NilError(t, err)

	// Verify the item is no longer in a group
	readItem, err := ds.Read(item.ID, ReadGroup)
	assert.NilError(t, err)
	assert.Equal(t, 0, readItem.GroupID)

	// Note: The groupID is still "valid" (it's < nextGroupID), so we CAN assign to it
	// This will recreate the group. This is actually reasonable behavior - the groupID
	// is reserved even if the group is currently empty.
	item2, err := ds.Append(Item{Data: []byte("test2"), DataDescriptor: 1})
	assert.NilError(t, err)

	// This should succeed and recreate the group
	err = ds.SetGroup(item2.ID, groupID, 0)
	assert.NilError(t, err)

	// Verify the group was recreated
	readItem2, err := ds.Read(item2.ID, ReadGroup)
	assert.NilError(t, err)
	assert.Equal(t, groupID, readItem2.GroupID)
}

func TestGroupsAfterFlush(t *testing.T) {
	tmpDir := t.TempDir()
	ds, err := Open(tmpDir, "test_groups_flush", DefaultOptions())
	assert.NilError(t, err)
	defer ds.Close()

	// Create group
	item, err := ds.Append(Item{
		Data:           []byte("test"),
		DataDescriptor: 1,
		GroupID:        -1,
	})
	assert.NilError(t, err)
	groupID := item.GroupID

	// Flush
	err = ds.Flush()
	assert.NilError(t, err)

	// Verify still accessible
	readItem, err := ds.Read(item.ID, ReadGroup)
	assert.NilError(t, err)
	assert.Equal(t, groupID, readItem.GroupID)

	// Modify after flush
	item2, err := ds.Append(Item{Data: []byte("test2"), DataDescriptor: 1})
	assert.NilError(t, err)

	err = ds.SetGroup(item2.ID, groupID, 1)
	assert.NilError(t, err)

	// Verify changes
	readItem2, err := ds.Read(item2.ID, ReadGroup)
	assert.NilError(t, err)
	assert.Equal(t, groupID, readItem2.GroupID)
	assert.Equal(t, 1, readItem2.GroupPlace)
}

func TestSetGroupClosed(t *testing.T) {
	tmpDir := t.TempDir()
	ds, err := Open(tmpDir, "test_set_group_closed", DefaultOptions())
	assert.NilError(t, err)

	// Create an item
	item, err := ds.Append(Item{Data: []byte("test"), DataDescriptor: 1})
	assert.NilError(t, err)

	// Close dataset
	ds.Close()

	// Try to set group on closed dataset
	err = ds.SetGroup(item.ID, 1, 0)
	assert.ErrorIs(t, ErrDatasetClosed, err)
}

func TestUnsetGroupClosed(t *testing.T) {
	tmpDir := t.TempDir()
	ds, err := Open(tmpDir, "test_unset_group_closed", DefaultOptions())
	assert.NilError(t, err)

	// Create an item
	item, err := ds.Append(Item{Data: []byte("test"), DataDescriptor: 1})
	assert.NilError(t, err)

	// Close dataset
	ds.Close()

	// Try to unset group on closed dataset
	err = ds.UnsetGroup(item.ID)
	assert.ErrorIs(t, ErrDatasetClosed, err)
}
