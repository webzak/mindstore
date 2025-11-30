package groups

import (
	"path/filepath"
	"testing"

	"github.com/webzak/mindstore/internal/testutil/assert"
)

func TestNew(t *testing.T) {
	dir := t.TempDir()

	g, err := New(filepath.Join(dir, "groups.bin"))
	assert.NilError(t, err)
	assert.NotNil(t, g, "groups should not be nil")
	assert.Equal(t, 0, g.nextGroupID)
	assert.Equal(t, true, g.isPersisted)
	assert.Equal(t, true, g.isLoaded)
}

func TestCreateGroup(t *testing.T) {
	dir := t.TempDir()

	g, err := New(filepath.Join(dir, "groups.bin"))
	assert.NilError(t, err)

	// Create first group with index 0
	groupID, err := g.CreateGroup(0)
	assert.NilError(t, err)
	assert.Equal(t, 0, groupID)
	assert.Equal(t, 1, g.nextGroupID)
	assert.Equal(t, false, g.isPersisted)

	// Verify the group was created with the first member
	members, err := g.GetMembers(groupID)
	assert.NilError(t, err)
	assert.DeepEqual(t, []int{0}, members)

	// Create second group with index 5
	groupID2, err := g.CreateGroup(5)
	assert.NilError(t, err)
	assert.Equal(t, 1, groupID2)
	assert.Equal(t, 2, g.nextGroupID)

	// Verify second group
	members2, err := g.GetMembers(groupID2)
	assert.NilError(t, err)
	assert.DeepEqual(t, []int{5}, members2)
}

func TestCreateGroupDuplicateIndex(t *testing.T) {
	dir := t.TempDir()

	g, err := New(filepath.Join(dir, "groups.bin"))
	assert.NilError(t, err)

	// Create first group with index 10
	groupID, err := g.CreateGroup(10)
	assert.NilError(t, err)
	assert.Equal(t, 0, groupID)

	// Try to create another group with same index
	_, err = g.CreateGroup(10)
	assert.NotNilError(t, err)
}

func TestAssign(t *testing.T) {
	dir := t.TempDir()

	g, err := New(filepath.Join(dir, "groups.bin"))
	assert.NilError(t, err)

	// Create a group with index 0 at place 0
	groupID, err := g.CreateGroup(0)
	assert.NilError(t, err)

	// Add another member to the group at place 1
	err = g.Assign(groupID, 1, 1)
	assert.NilError(t, err)

	// Add another member at place 2
	err = g.Assign(groupID, 2, 2)
	assert.NilError(t, err)

	// Verify all members
	members, err := g.GetMembers(groupID)
	assert.NilError(t, err)
	assert.DeepEqual(t, []int{0, 1, 2}, members)
}

func TestAssignInvalidGroupID(t *testing.T) {
	dir := t.TempDir()

	g, err := New(filepath.Join(dir, "groups.bin"))
	assert.NilError(t, err)

	// Try to assign to non-existent group
	err = g.Assign(5, 0, 0)
	assert.NotNilError(t, err)

	// Try to assign to negative group ID
	err = g.Assign(-1, 0, 0)
	assert.NotNilError(t, err)
}

func TestAssignDuplicatePlace(t *testing.T) {
	dir := t.TempDir()

	g, err := New(filepath.Join(dir, "groups.bin"))
	assert.NilError(t, err)

	groupID, err := g.CreateGroup(0)
	assert.NilError(t, err)

	// Index 0 is already at place 0, try to assign index 1 to place 0
	err = g.Assign(groupID, 1, 0)
	assert.NotNilError(t, err)
}

func TestAssignIndexToMultipleGroups(t *testing.T) {
	dir := t.TempDir()

	g, err := New(filepath.Join(dir, "groups.bin"))
	assert.NilError(t, err)

	// Create first group with index 0
	_, err = g.CreateGroup(0)
	assert.NilError(t, err)

	// Create second group with index 1
	groupID2, err := g.CreateGroup(1)
	assert.NilError(t, err)

	// Try to assign index 0 to second group (should fail)
	err = g.Assign(groupID2, 0, 1)
	assert.NotNilError(t, err)
}

func TestAssignUpdatePlace(t *testing.T) {
	dir := t.TempDir()

	g, err := New(filepath.Join(dir, "groups.bin"))
	assert.NilError(t, err)

	groupID, err := g.CreateGroup(0)
	assert.NilError(t, err)

	// Add members
	err = g.Assign(groupID, 1, 1)
	assert.NilError(t, err)

	err = g.Assign(groupID, 2, 2)
	assert.NilError(t, err)

	// Update place of index 0 from 0 to 3
	err = g.Assign(groupID, 0, 3)
	assert.NilError(t, err)

	// Verify order: 1, 2, 0
	members, err := g.GetMembers(groupID)
	assert.NilError(t, err)
	assert.DeepEqual(t, []int{1, 2, 0}, members)
}

func TestGetGroup(t *testing.T) {
	dir := t.TempDir()

	g, err := New(filepath.Join(dir, "groups.bin"))
	assert.NilError(t, err)

	// Create groups
	groupID1, err := g.CreateGroup(10)
	assert.NilError(t, err)

	groupID2, err := g.CreateGroup(20)
	assert.NilError(t, err)

	// Get group for index 10
	foundGroup, err := g.GetGroup(10)
	assert.NilError(t, err)
	assert.Equal(t, groupID1, foundGroup)

	// Get group for index 20
	foundGroup, err = g.GetGroup(20)
	assert.NilError(t, err)
	assert.Equal(t, groupID2, foundGroup)

	// Get group for non-existent index
	foundGroup, err = g.GetGroup(999)
	assert.NilError(t, err)
	assert.Equal(t, -1, foundGroup)
}

func TestGetMembers(t *testing.T) {
	dir := t.TempDir()

	g, err := New(filepath.Join(dir, "groups.bin"))
	assert.NilError(t, err)

	groupID, err := g.CreateGroup(5)
	assert.NilError(t, err)

	// Add members in non-sequential order
	err = g.Assign(groupID, 3, 2)
	assert.NilError(t, err)

	err = g.Assign(groupID, 1, 1)
	assert.NilError(t, err)

	err = g.Assign(groupID, 7, 3)
	assert.NilError(t, err)

	// Members should be sorted by place: 5(0), 1(1), 3(2), 7(3)
	members, err := g.GetMembers(groupID)
	assert.NilError(t, err)
	assert.DeepEqual(t, []int{5, 1, 3, 7}, members)
}

func TestGetMembersInvalidGroupID(t *testing.T) {
	dir := t.TempDir()

	g, err := New(filepath.Join(dir, "groups.bin"))
	assert.NilError(t, err)

	// Negative group ID
	_, err = g.GetMembers(-1)
	assert.NotNilError(t, err)
}

func TestGetMembersNonExistentGroup(t *testing.T) {
	dir := t.TempDir()

	g, err := New(filepath.Join(dir, "groups.bin"))
	assert.NilError(t, err)

	// Non-existent group returns nil
	members, err := g.GetMembers(999)
	assert.NilError(t, err)
	assert.DeepEqual(t, []int(nil), members)
}

func TestCount(t *testing.T) {
	dir := t.TempDir()

	g, err := New(filepath.Join(dir, "groups.bin"))
	assert.NilError(t, err)

	// Initially zero
	count, err := g.Count()
	assert.NilError(t, err)
	assert.Equal(t, 0, count)

	// Create group with one member
	groupID, err := g.CreateGroup(0)
	assert.NilError(t, err)

	count, err = g.Count()
	assert.NilError(t, err)
	assert.Equal(t, 1, count)

	// Add more members
	err = g.Assign(groupID, 1, 1)
	assert.NilError(t, err)

	err = g.Assign(groupID, 2, 2)
	assert.NilError(t, err)

	count, err = g.Count()
	assert.NilError(t, err)
	assert.Equal(t, 3, count)

	// Create another group
	_, err = g.CreateGroup(10)
	assert.NilError(t, err)

	count, err = g.Count()
	assert.NilError(t, err)
	assert.Equal(t, 4, count)
}

func TestFlushAndLoad(t *testing.T) {
	dir := t.TempDir()

	path := filepath.Join(dir, "groups.bin")

	// Create and populate groups
	g, err := New(path)
	assert.NilError(t, err)

	groupID1, err := g.CreateGroup(0)
	assert.NilError(t, err)

	err = g.Assign(groupID1, 1, 1)
	assert.NilError(t, err)

	groupID2, err := g.CreateGroup(10)
	assert.NilError(t, err)

	err = g.Assign(groupID2, 11, 1)
	assert.NilError(t, err)

	err = g.Assign(groupID2, 12, 2)
	assert.NilError(t, err)

	// Flush to disk
	err = g.Flush()
	assert.NilError(t, err)
	assert.Equal(t, true, g.IsPersisted())

	err = g.Close()
	assert.NilError(t, err)

	// Load from disk
	g2, err := New(path)
	assert.NilError(t, err)

	// Verify groups were loaded (this triggers lazy loading)
	members1, err := g2.GetMembers(groupID1)
	assert.NilError(t, err)
	assert.DeepEqual(t, []int{0, 1}, members1)

	members2, err := g2.GetMembers(groupID2)
	assert.NilError(t, err)
	assert.DeepEqual(t, []int{10, 11, 12}, members2)

	// Verify index-to-group mapping
	foundGroup, err := g2.GetGroup(0)
	assert.NilError(t, err)
	assert.Equal(t, groupID1, foundGroup)

	foundGroup, err = g2.GetGroup(11)
	assert.NilError(t, err)
	assert.Equal(t, groupID2, foundGroup)

	// Verify count
	count, err := g2.Count()
	assert.NilError(t, err)
	assert.Equal(t, 5, count)

	// Verify nextGroupID was restored correctly (after data is loaded)
	assert.Equal(t, 2, g2.nextGroupID)
}

func TestTruncate(t *testing.T) {
	dir := t.TempDir()

	g, err := New(filepath.Join(dir, "groups.bin"))
	assert.NilError(t, err)

	// Create some groups
	_, err = g.CreateGroup(0)
	assert.NilError(t, err)

	_, err = g.CreateGroup(1)
	assert.NilError(t, err)

	count, err := g.Count()
	assert.NilError(t, err)
	assert.Equal(t, 2, count)

	// Truncate
	err = g.Truncate()
	assert.NilError(t, err)

	// Verify everything is cleared
	count, err = g.Count()
	assert.NilError(t, err)
	assert.Equal(t, 0, count)

	// Verify nextGroupID is reset to 0
	assert.Equal(t, 0, g.nextGroupID)

	// Verify isPersisted is true
	assert.Equal(t, true, g.IsPersisted())

	// Verify we can create new groups starting from 0 again
	newGroupID, err := g.CreateGroup(100)
	assert.NilError(t, err)
	assert.Equal(t, 0, newGroupID)
}

func TestIsPersisted(t *testing.T) {
	dir := t.TempDir()

	g, err := New(filepath.Join(dir, "groups.bin"))
	assert.NilError(t, err)

	// Initially persisted (empty)
	assert.Equal(t, true, g.IsPersisted())

	// After creating a group
	_, err = g.CreateGroup(0)
	assert.NilError(t, err)
	assert.Equal(t, false, g.IsPersisted())

	// After flush
	err = g.Flush()
	assert.NilError(t, err)
	assert.Equal(t, true, g.IsPersisted())

	// After assign
	err = g.Assign(0, 1, 1)
	assert.NilError(t, err)
	assert.Equal(t, false, g.IsPersisted())
}

func TestMultipleFlushes(t *testing.T) {
	dir := t.TempDir()

	g, err := New(filepath.Join(dir, "groups.bin"))
	assert.NilError(t, err)

	// First flush (empty)
	err = g.Flush()
	assert.NilError(t, err)

	// Create and flush
	_, err = g.CreateGroup(0)
	assert.NilError(t, err)
	err = g.Flush()
	assert.NilError(t, err)

	// Multiple flushes should be safe
	err = g.Flush()
	assert.NilError(t, err)
	err = g.Flush()
	assert.NilError(t, err)
}

func TestLazyLoading(t *testing.T) {
	dir := t.TempDir()

	path := filepath.Join(dir, "groups.bin")

	// Create and save data
	g1, err := New(path)
	assert.NilError(t, err)

	_, err = g1.CreateGroup(0)
	assert.NilError(t, err)

	err = g1.Flush()
	assert.NilError(t, err)

	err = g1.Close()
	assert.NilError(t, err)

	// Create new instance - should not load immediately
	g2, err := New(path)
	assert.NilError(t, err)
	assert.Equal(t, false, g2.isLoaded)

	// Accessing data triggers load
	count, err := g2.Count()
	assert.NilError(t, err)
	assert.Equal(t, 1, count)
	assert.Equal(t, true, g2.isLoaded)
}

func TestRemoveMemberFromGroup(t *testing.T) {
	dir := t.TempDir()

	g, err := New(filepath.Join(dir, "groups.bin"))
	assert.NilError(t, err)

	groupID, err := g.CreateGroup(0)
	assert.NilError(t, err)

	err = g.Assign(groupID, 1, 1)
	assert.NilError(t, err)

	err = g.Assign(groupID, 2, 2)
	assert.NilError(t, err)

	// Update place of existing member (triggers removeMemberFromGroup internally)
	err = g.Assign(groupID, 1, 3)
	assert.NilError(t, err)

	members, err := g.GetMembers(groupID)
	assert.NilError(t, err)
	// Order should be: 0(place 0), 2(place 2), 1(place 3)
	assert.DeepEqual(t, []int{0, 2, 1}, members)
}
