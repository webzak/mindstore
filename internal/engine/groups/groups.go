package groups

import (
	"encoding/gob"
	"fmt"
	"sort"

	"github.com/webzak/mindstore/internal/engine/storage"
)

// Member represents a member of a group
type Member struct {
	IndexID int
	Place   int
}

// Groups manages group-to-index relationships
type Groups struct {
	// groups maps GroupID -> []Member
	groups map[int][]Member
	// indexToGroup maps IndexID -> GroupID
	indexToGroup map[int]int
	// nextGroupID is the next available group ID
	nextGroupID int
	// storage is the underlying file storage
	storage *storage.File
}

// New creates a new groups manager
func New(path string) (*Groups, error) {
	storage := storage.NewFile(path)
	if err := storage.Init(); err != nil {
		return nil, err
	}
	return &Groups{
		groups:       make(map[int][]Member),
		indexToGroup: make(map[int]int),
		nextGroupID:  1, // Start group IDs from 1
		storage:      storage,
	}, nil
}

// Load loads groups from storage
func (g *Groups) Load() error {

	reader, err := g.storage.Reader(0)
	if err != nil {
		return err
	}
	defer reader.Close()

	// Check if file is empty
	size, err := g.storage.Size()
	if err != nil {
		return err
	}
	if size == 0 {
		return nil
	}

	// Decode the map directly
	var savedGroups map[int][]Member
	decoder := gob.NewDecoder(reader)
	if err := decoder.Decode(&savedGroups); err != nil {
		return fmt.Errorf("failed to decode groups: %w", err)
	}
	g.groups = savedGroups

	// Rebuild indexToGroup and find nextGroupID
	g.indexToGroup = make(map[int]int)
	maxGroupID := 0
	for groupID, members := range g.groups {
		if groupID > maxGroupID {
			maxGroupID = groupID
		}
		for _, member := range members {
			g.indexToGroup[member.IndexID] = groupID
		}
	}
	g.nextGroupID = maxGroupID + 1

	return nil
}

// Save saves groups to storage
func (g *Groups) Save() error {

	// Truncate file before writing
	if err := g.storage.Truncate(0); err != nil {
		return err
	}

	writer, err := g.storage.Writer(0)
	if err != nil {
		return err
	}
	defer writer.Close()

	encoder := gob.NewEncoder(writer)
	if err := encoder.Encode(g.groups); err != nil {
		return fmt.Errorf("failed to encode groups: %w", err)
	}

	return nil
}

// CreateGroup creates a new group and returns its ID
func (g *Groups) CreateGroup() int {

	id := g.nextGroupID
	g.nextGroupID++
	// Initialize empty group
	g.groups[id] = []Member{}
	return id
}

// Assign assigns an index ID to a group with a specific place
func (g *Groups) Assign(groupID int, indexID int, place int) error {

	// Check if group exists (it should be created first, or at least we check if we are creating a new one implicitly?
	// The requirement says "Create new group ID... It is the logic of upper level to provide the proper index ID and place number to add them to group."
	// But it also says "Assign integer index ID to group id".
	// If we strictly follow "Create new group ID" as a separate step, then we should probably check if group exists.
	// However, for simplicity and robustness, if the group doesn't exist in the map (e.g. it was created but has no members yet, or just a valid ID passed),
	// we can just add to it. But we should ensure the GroupID is valid (i.e. < nextGroupID).

	if groupID >= g.nextGroupID || groupID <= 0 {
		return fmt.Errorf("invalid group ID: %d", groupID)
	}

	// Check if index ID is already assigned to a group
	if existingGroup, ok := g.indexToGroup[indexID]; ok {
		if existingGroup == groupID {
			// Already in this group, maybe update place?
			// For now, let's assume we just update the place if it's the same group
			// But wait, "Note that it can belong to only single group."
			// If it's already in THIS group, we might want to update the place.
			// Let's remove the old entry for this indexID in this group first.
			g.removeMemberFromGroup(groupID, indexID)
		} else {
			return fmt.Errorf("index ID %d is already assigned to group %d", indexID, existingGroup)
		}
	}

	member := Member{
		IndexID: indexID,
		Place:   place,
	}
	g.groups[groupID] = append(g.groups[groupID], member)
	g.indexToGroup[indexID] = groupID
	return nil
}

// removeMemberFromGroup removes a member from a group's list (internal helper)
func (g *Groups) removeMemberFromGroup(groupID int, indexID int) {
	members := g.groups[groupID]
	for i, m := range members {
		if m.IndexID == indexID {
			// Remove element
			g.groups[groupID] = append(members[:i], members[i+1:]...)
			return
		}
	}
}

// GetGroup returns the group ID for a given index ID
func (g *Groups) GetGroup(indexID int) (int, bool) {

	groupID, ok := g.indexToGroup[indexID]
	return groupID, ok
}

// GetMembers returns all index IDs for a group, sorted by place
func (g *Groups) GetMembers(groupID int) []int {

	members, ok := g.groups[groupID]
	if !ok {
		return nil
	}

	// Sort members by place
	// We need to copy to avoid race conditions during sort if we were sorting in place on the original slice
	// But here we are just reading. However, sort.Slice sorts in place.
	// So we MUST copy the slice of members first.

	sortedMembers := make([]Member, len(members))
	copy(sortedMembers, members)

	sort.Slice(sortedMembers, func(i, j int) bool {
		return sortedMembers[i].Place < sortedMembers[j].Place
	})

	// Extract IndexIDs
	ids := make([]int, len(sortedMembers))
	for i, m := range sortedMembers {
		ids[i] = m.IndexID
	}

	return ids
}

// Count returns the number of records assigned to groups
func (g *Groups) Count() int {
	return len(g.indexToGroup)
}
