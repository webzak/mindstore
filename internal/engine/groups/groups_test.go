package groups

import (
	"encoding/gob"
	"path/filepath"
	"testing"
)

func init() {
	// Register Member type for gob encoding
	gob.Register([]Member{})
}

// Helper function to create a temporary test file
func createTempFile(t *testing.T) string {
	t.Helper()
	tmpDir := t.TempDir()
	return filepath.Join(tmpDir, "groups.dat")
}

func TestNew(t *testing.T) {
	path := createTempFile(t)

	groups, err := New(path)
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	if groups == nil {
		t.Fatal("New() returned nil groups")
	}

	if groups.groups == nil {
		t.Error("groups map not initialized")
	}

	if groups.indexToGroup == nil {
		t.Error("indexToGroup map not initialized")
	}

	if groups.nextGroupID != 1 {
		t.Errorf("Expected nextGroupID to be 1, got %d", groups.nextGroupID)
	}

	if !groups.isPersisted {
		t.Error("isPersisted should be true for new groups")
	}

	if !groups.isLoaded {
		t.Error("isLoaded should be true for empty storage")
	}
}

func TestCreateGroup(t *testing.T) {
	path := createTempFile(t)
	groups, err := New(path)
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	// Test creating first group
	id1, err := groups.CreateGroup()
	if err != nil {
		t.Errorf("CreateGroup() failed: %v", err)
	}
	if id1 != 1 {
		t.Errorf("Expected first group ID to be 1, got %d", id1)
	}

	// Test creating second group
	id2, err := groups.CreateGroup()
	if err != nil {
		t.Errorf("CreateGroup() failed: %v", err)
	}
	if id2 != 2 {
		t.Errorf("Expected second group ID to be 2, got %d", id2)
	}

	// Verify group is initialized as empty
	if members, ok := groups.groups[id1]; !ok || len(members) != 0 {
		t.Error("New group should be initialized with empty member list")
	}

	// Verify isPersisted is false after creating
	if groups.isPersisted {
		t.Error("isPersisted should be false after creating group")
	}
}

func TestAssign(t *testing.T) {
	path := createTempFile(t)
	groups, err := New(path)
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	// Create a group
	groupID, _ := groups.CreateGroup()

	// Test assigning index to group
	err = groups.Assign(groupID, 100, 0)
	if err != nil {
		t.Errorf("Assign() failed: %v", err)
	}

	// Verify member was added
	members := groups.groups[groupID]
	if len(members) != 1 {
		t.Fatalf("Expected 1 member, got %d", len(members))
	}
	if members[0].IndexID != 100 || members[0].Place != 0 {
		t.Errorf("Member not assigned correctly: %+v", members[0])
	}

	// Verify reverse mapping
	if groups.indexToGroup[100] != groupID {
		t.Error("indexToGroup mapping not set correctly")
	}

	// Test assigning another index to same group
	err = groups.Assign(groupID, 101, 1)
	if err != nil {
		t.Errorf("Assign() second member failed: %v", err)
	}

	members = groups.groups[groupID]
	if len(members) != 2 {
		t.Errorf("Expected 2 members, got %d", len(members))
	}

	// Test assigning to invalid group ID
	err = groups.Assign(999, 102, 0)
	if err == nil {
		t.Error("Expected error for invalid group ID")
	}

	// Test assigning with zero group ID
	err = groups.Assign(0, 103, 0)
	if err == nil {
		t.Error("Expected error for zero group ID")
	}

	// Test assigning with negative group ID
	err = groups.Assign(-1, 104, 0)
	if err == nil {
		t.Error("Expected error for negative group ID")
	}

	// Test duplicate place
	err = groups.Assign(groupID, 105, 0)
	if err == nil {
		t.Error("Expected error for duplicate place")
	}

	// Test assigning index already in another group
	groupID2, _ := groups.CreateGroup()
	err = groups.Assign(groupID2, 100, 0)
	if err == nil {
		t.Error("Expected error for index already in another group")
	}

	// Test updating place for an index already in the group
	err = groups.Assign(groupID, 100, 5)
	if err != nil {
		t.Errorf("Assign() to update place failed: %v", err)
	}

	// Verify member was updated
	members = groups.groups[groupID]
	found := false
	for _, m := range members {
		if m.IndexID == 100 {
			if m.Place != 5 {
				t.Errorf("Expected place 5, got %d", m.Place)
			}
			found = true
			break
		}
	}
	if !found {
		t.Error("Member not found after update")
	}

	// Verify isPersisted is false after assigning
	if groups.isPersisted {
		t.Error("isPersisted should be false after assigning")
	}
}

func TestGetGroup(t *testing.T) {
	path := createTempFile(t)
	groups, err := New(path)
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	// Test getting group for non-assigned index
	groupID, err := groups.GetGroup(999)
	if err != nil {
		t.Errorf("GetGroup() failed: %v", err)
	}
	if groupID != -1 {
		t.Errorf("Expected -1 for non-assigned index, got %d", groupID)
	}

	// Create group and assign index
	gid, _ := groups.CreateGroup()
	groups.Assign(gid, 100, 0)

	// Test getting group for assigned index
	groupID, err = groups.GetGroup(100)
	if err != nil {
		t.Errorf("GetGroup() failed: %v", err)
	}
	if groupID != gid {
		t.Errorf("Expected group ID %d, got %d", gid, groupID)
	}
}

func TestGetMembers(t *testing.T) {
	path := createTempFile(t)
	groups, err := New(path)
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	// Test getting members for invalid group ID
	_, err = groups.GetMembers(0)
	if err == nil {
		t.Error("Expected error for zero group ID")
	}

	_, err = groups.GetMembers(-1)
	if err == nil {
		t.Error("Expected error for negative group ID")
	}

	// Test getting members for non-existent group
	members, err := groups.GetMembers(999)
	if err != nil {
		t.Errorf("GetMembers() failed: %v", err)
	}
	if members != nil {
		t.Errorf("Expected nil for non-existent group, got %v", members)
	}

	// Create group and assign members
	groupID, _ := groups.CreateGroup()
	groups.Assign(groupID, 100, 2) // Add in non-sequential order
	groups.Assign(groupID, 101, 0)
	groups.Assign(groupID, 102, 1)

	// Test getting members (should be sorted by place)
	members, err = groups.GetMembers(groupID)
	if err != nil {
		t.Errorf("GetMembers() failed: %v", err)
	}
	if len(members) != 3 {
		t.Fatalf("Expected 3 members, got %d", len(members))
	}

	// Verify sorting by place
	if members[0] != 101 || members[1] != 102 || members[2] != 100 {
		t.Errorf("Members not sorted correctly by place: %v", members)
	}

	// Verify returned slice is a copy
	members[0] = 999
	originalMembers, _ := groups.GetMembers(groupID)
	if originalMembers[0] == 999 {
		t.Error("Modifying returned slice should not affect internal state")
	}
}

func TestCount(t *testing.T) {
	path := createTempFile(t)
	groups, err := New(path)
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	// Test empty groups
	count, err := groups.Count()
	if err != nil {
		t.Errorf("Count() failed: %v", err)
	}
	if count != 0 {
		t.Errorf("Expected count 0, got %d", count)
	}

	// Create group and assign indices
	groupID, _ := groups.CreateGroup()
	groups.Assign(groupID, 100, 0)
	groups.Assign(groupID, 101, 1)

	count, err = groups.Count()
	if err != nil {
		t.Errorf("Count() failed: %v", err)
	}
	if count != 2 {
		t.Errorf("Expected count 2, got %d", count)
	}

	// Create another group with members
	groupID2, _ := groups.CreateGroup()
	groups.Assign(groupID2, 102, 0)

	count, err = groups.Count()
	if err != nil {
		t.Errorf("Count() failed: %v", err)
	}
	if count != 3 {
		t.Errorf("Expected count 3, got %d", count)
	}

	// Create empty group (should not affect count)
	groups.CreateGroup()

	count, err = groups.Count()
	if err != nil {
		t.Errorf("Count() failed: %v", err)
	}
	if count != 3 {
		t.Errorf("Expected count 3, got %d", count)
	}
}

func TestFlush(t *testing.T) {
	path := createTempFile(t)
	groups, err := New(path)
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	// Create group and assign member
	groupID, _ := groups.CreateGroup()
	groups.Assign(groupID, 100, 0)

	// Verify isPersisted is false
	if groups.isPersisted {
		t.Error("isPersisted should be false after modifications")
	}

	// Flush
	err = groups.Flush()
	if err != nil {
		t.Errorf("Flush() failed: %v", err)
	}

	// Verify isPersisted is true
	if !groups.isPersisted {
		t.Error("isPersisted should be true after Flush()")
	}

	// Verify storage is not empty
	size, err := groups.storage.Size()
	if err != nil {
		t.Errorf("Size() failed: %v", err)
	}
	if size == 0 {
		t.Error("Expected storage size to be greater than 0 after Flush()")
	}

	// Test flushing when already persisted (should be no-op)
	err = groups.Flush()
	if err != nil {
		t.Errorf("Flush() on already persisted groups failed: %v", err)
	}
}

func TestIsPersisted(t *testing.T) {
	path := createTempFile(t)
	groups, err := New(path)
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	// Verify initial state
	if !groups.IsPersisted() {
		t.Error("IsPersisted() should be true for new groups")
	}

	// Modify data
	groups.CreateGroup()

	// Verify isPersisted is false
	if groups.IsPersisted() {
		t.Error("IsPersisted() should be false after modification")
	}

	// Flush
	groups.Flush()

	// Verify isPersisted is true
	if !groups.IsPersisted() {
		t.Error("IsPersisted() should be true after Flush()")
	}
}

func TestDestroy(t *testing.T) {
	path := createTempFile(t)
	groups, err := New(path)
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	// Create groups and assign members
	groupID1, _ := groups.CreateGroup()
	groupID2, _ := groups.CreateGroup()
	groups.Assign(groupID1, 100, 0)
	groups.Assign(groupID2, 101, 0)

	// Flush to persist
	groups.Flush()

	// Destroy
	err = groups.Destroy()
	if err != nil {
		t.Errorf("Destroy() failed: %v", err)
	}

	// Verify maps are empty
	if len(groups.groups) != 0 {
		t.Error("groups map should be empty after Destroy()")
	}
	if len(groups.indexToGroup) != 0 {
		t.Error("indexToGroup map should be empty after Destroy()")
	}

	// Verify nextGroupID is reset
	if groups.nextGroupID != 1 {
		t.Errorf("Expected nextGroupID to be 1, got %d", groups.nextGroupID)
	}

	// Verify isPersisted is true
	if !groups.isPersisted {
		t.Error("isPersisted should be true after Destroy()")
	}

	// Verify storage is empty
	size, err := groups.storage.Size()
	if err != nil {
		t.Errorf("Size() failed: %v", err)
	}
	if size != 0 {
		t.Errorf("Expected storage size to be 0, got %d", size)
	}

	// Verify count is 0
	count, _ := groups.Count()
	if count != 0 {
		t.Errorf("Expected count 0 after Destroy(), got %d", count)
	}
}

func TestPersistenceAndLoad(t *testing.T) {
	path := createTempFile(t)

	// Create and populate groups
	groups1, err := New(path)
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	groupID1, _ := groups1.CreateGroup()
	groupID2, _ := groups1.CreateGroup()
	groups1.Assign(groupID1, 100, 0)
	groups1.Assign(groupID1, 101, 1)
	groups1.Assign(groupID2, 102, 0)
	groups1.Assign(groupID2, 103, 1)
	groups1.Assign(groupID2, 104, 2)

	err = groups1.Flush()
	if err != nil {
		t.Errorf("Flush() failed: %v", err)
	}

	// Create new groups instance from same path
	groups2, err := New(path)
	if err != nil {
		t.Fatalf("New() failed on reload: %v", err)
	}

	// Force load
	groups2.isLoaded = false
	err = groups2.load()
	if err != nil {
		t.Errorf("load() failed: %v", err)
	}

	// Verify groups were loaded
	if len(groups2.groups) != 2 {
		t.Errorf("Expected 2 groups, got %d", len(groups2.groups))
	}

	// Verify members for group 1
	members, _ := groups2.GetMembers(groupID1)
	if len(members) != 2 {
		t.Errorf("Expected 2 members for group 1, got %d", len(members))
	}
	if members[0] != 100 || members[1] != 101 {
		t.Errorf("Group 1 members not loaded correctly: %v", members)
	}

	// Verify members for group 2
	members, _ = groups2.GetMembers(groupID2)
	if len(members) != 3 {
		t.Errorf("Expected 3 members for group 2, got %d", len(members))
	}
	if members[0] != 102 || members[1] != 103 || members[2] != 104 {
		t.Errorf("Group 2 members not loaded correctly: %v", members)
	}

	// Verify indexToGroup mapping
	gid, _ := groups2.GetGroup(100)
	if gid != groupID1 {
		t.Errorf("Index 100 should be in group %d, got %d", groupID1, gid)
	}
	gid, _ = groups2.GetGroup(102)
	if gid != groupID2 {
		t.Errorf("Index 102 should be in group %d, got %d", groupID2, gid)
	}

	// Verify nextGroupID
	if groups2.nextGroupID != 3 {
		t.Errorf("Expected nextGroupID to be 3, got %d", groups2.nextGroupID)
	}

	// Verify isLoaded flag
	if !groups2.isLoaded {
		t.Error("isLoaded should be true after load()")
	}

	// Verify count
	count, _ := groups2.Count()
	if count != 5 {
		t.Errorf("Expected count 5 after reload, got %d", count)
	}
}

func TestLoadEmptyFile(t *testing.T) {
	path := createTempFile(t)

	// Create groups with empty file
	groups, err := New(path)
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	// Force load on empty file
	groups.isLoaded = false
	err = groups.load()
	if err != nil {
		t.Errorf("load() on empty file failed: %v", err)
	}

	if !groups.isLoaded {
		t.Error("isLoaded should be true after loading empty file")
	}

	if len(groups.groups) != 0 || len(groups.indexToGroup) != 0 {
		t.Error("Maps should be empty after loading empty file")
	}
}

func TestLazyLoad(t *testing.T) {
	path := createTempFile(t)

	// Create and populate groups
	groups1, err := New(path)
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	groupID1, _ := groups1.CreateGroup()
	groups1.Assign(groupID1, 100, 0)
	groups1.Flush()

	// Create new instance
	groups2, err := New(path)
	if err != nil {
		t.Fatalf("New() failed on reload: %v", err)
	}

	// Mark as not loaded to simulate lazy loading
	groups2.isLoaded = false

	// Calling CreateGroup should trigger load
	groupID2, err := groups2.CreateGroup()
	if err != nil {
		t.Errorf("CreateGroup() failed: %v", err)
	}

	if !groups2.isLoaded {
		t.Error("isLoaded should be true after CreateGroup() triggers load")
	}

	// Verify old data was loaded
	gid, _ := groups2.GetGroup(100)
	if gid != groupID1 {
		t.Error("Previously persisted data should be loaded")
	}

	// Verify nextGroupID accounts for loaded data
	if groupID2 != 2 {
		t.Errorf("Expected new group ID to be 2, got %d", groupID2)
	}

	// Test lazy load with GetGroup
	groups3, _ := New(path)
	groups3.isLoaded = false

	gid, _ = groups3.GetGroup(100)
	if !groups3.isLoaded {
		t.Error("GetGroup() should trigger lazy load")
	}
	if gid != groupID1 {
		t.Error("GetGroup() should return correct data after lazy load")
	}

	// Test lazy load with GetMembers
	groups4, _ := New(path)
	groups4.isLoaded = false

	_, _ = groups4.GetMembers(groupID1)
	if !groups4.isLoaded {
		t.Error("GetMembers() should trigger lazy load")
	}

	// Test lazy load with Count
	groups5, _ := New(path)
	groups5.isLoaded = false

	_, _ = groups5.Count()
	if !groups5.isLoaded {
		t.Error("Count() should trigger lazy load")
	}

	// Test lazy load with Assign
	groups6, _ := New(path)
	groups6.isLoaded = false

	_ = groups6.Assign(groupID1, 101, 1)
	if !groups6.isLoaded {
		t.Error("Assign() should trigger lazy load")
	}

	// Test lazy load with Destroy
	groups7, _ := New(path)
	groups7.isLoaded = false

	_ = groups7.Destroy()
	if !groups7.isLoaded {
		t.Error("Destroy() should trigger lazy load")
	}
}

func TestComplexScenario(t *testing.T) {
	path := createTempFile(t)
	groups, err := New(path)
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	// Complex scenario: multiple groups with various members
	g1, _ := groups.CreateGroup()
	g2, _ := groups.CreateGroup()
	g3, _ := groups.CreateGroup()

	// Assign members to different groups
	groups.Assign(g1, 100, 0)
	groups.Assign(g1, 101, 1)
	groups.Assign(g1, 102, 2)

	groups.Assign(g2, 200, 5) // Non-sequential places
	groups.Assign(g2, 201, 1)
	groups.Assign(g2, 202, 3)

	// Leave g3 empty

	// Test Count
	count, _ := groups.Count()
	if count != 6 {
		t.Errorf("Expected count 6, got %d", count)
	}

	// Test GetMembers for each group
	members1, _ := groups.GetMembers(g1)
	if len(members1) != 3 || members1[0] != 100 || members1[1] != 101 || members1[2] != 102 {
		t.Errorf("Group 1 members incorrect: %v", members1)
	}

	members2, _ := groups.GetMembers(g2)
	if len(members2) != 3 || members2[0] != 201 || members2[1] != 202 || members2[2] != 200 {
		t.Errorf("Group 2 members not sorted correctly: %v", members2)
	}

	members3, _ := groups.GetMembers(g3)
	if len(members3) != 0 {
		t.Errorf("Group 3 should be empty, got %v", members3)
	}

	// Test GetGroup for various indices
	gid, _ := groups.GetGroup(100)
	if gid != g1 {
		t.Errorf("Index 100 should be in group %d, got %d", g1, gid)
	}

	gid, _ = groups.GetGroup(201)
	if gid != g2 {
		t.Errorf("Index 201 should be in group %d, got %d", g2, gid)
	}

	gid, _ = groups.GetGroup(999)
	if gid != -1 {
		t.Errorf("Index 999 should not be assigned, got %d", gid)
	}

	// Flush and reload
	groups.Flush()

	groups2, _ := New(path)
	groups2.isLoaded = false
	groups2.load()

	// Verify data after reload
	count, _ = groups2.Count()
	if count != 6 {
		t.Errorf("Expected count 6 after reload, got %d", count)
	}

	members1, _ = groups2.GetMembers(g1)
	if len(members1) != 3 {
		t.Errorf("Expected 3 members for group 1 after reload, got %d", len(members1))
	}

	members2, _ = groups2.GetMembers(g2)
	if len(members2) != 3 {
		t.Errorf("Expected 3 members for group 2 after reload, got %d", len(members2))
	}

	// Test updating place within same group
	err = groups2.Assign(g1, 100, 10)
	if err != nil {
		t.Errorf("Updating place failed: %v", err)
	}

	members1, _ = groups2.GetMembers(g1)
	if members1[2] != 100 { // Should now be last because place is 10
		t.Errorf("Member place not updated correctly, got %v", members1)
	}
}

func TestUpdatePlaceInSameGroup(t *testing.T) {
	path := createTempFile(t)
	groups, err := New(path)
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	groupID, _ := groups.CreateGroup()
	groups.Assign(groupID, 100, 0)
	groups.Assign(groupID, 101, 1)
	groups.Assign(groupID, 102, 2)

	// Update place for existing member
	err = groups.Assign(groupID, 101, 5)
	if err != nil {
		t.Errorf("Updating place failed: %v", err)
	}

	// Verify member count is still 3 (not duplicated)
	members, _ := groups.GetMembers(groupID)
	if len(members) != 3 {
		t.Errorf("Expected 3 members, got %d", len(members))
	}

	// Verify new ordering
	if members[0] != 100 || members[1] != 102 || members[2] != 101 {
		t.Errorf("Members not ordered correctly after place update: %v", members)
	}

	// Verify reverse mapping still works
	gid, _ := groups.GetGroup(101)
	if gid != groupID {
		t.Error("Reverse mapping broken after place update")
	}
}

func TestMultipleGroupsAndPlaces(t *testing.T) {
	path := createTempFile(t)
	groups, err := New(path)
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	// Create multiple groups
	g1, _ := groups.CreateGroup()
	g2, _ := groups.CreateGroup()

	// Assign same place in different groups (should be allowed)
	err = groups.Assign(g1, 100, 0)
	if err != nil {
		t.Errorf("Assign to g1 failed: %v", err)
	}

	err = groups.Assign(g2, 200, 0)
	if err != nil {
		t.Errorf("Assign to g2 with same place failed: %v", err)
	}

	// Verify both assignments
	members1, _ := groups.GetMembers(g1)
	if len(members1) != 1 || members1[0] != 100 {
		t.Error("Group 1 assignment failed")
	}

	members2, _ := groups.GetMembers(g2)
	if len(members2) != 1 || members2[0] != 200 {
		t.Error("Group 2 assignment failed")
	}
}
