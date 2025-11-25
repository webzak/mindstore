package groups

import (
	"encoding/gob"
	"errors"
	"fmt"
	"sort"

	"github.com/webzak/mindstore/internal/engine/storage"
)

// Member represents a member of a group
type Member struct {
	IndexID int
	Place   int
}

// Groups manages group-to-index relationships.
// It uses lazy loading: data is loaded from storage only when first accessed.
// Note: Groups is NOT thread-safe. External synchronization is required for concurrent access.
type Groups struct {
	// storage is the underlying file storage
	storage *storage.File
	// groups maps GroupID -> []Member
	groups map[int][]Member
	// indexToGroup maps IndexID -> GroupID
	indexToGroup map[int]int
	// nextGroupID is the next available group ID
	nextGroupID int
	// isPersisted indicates if the current data has been persisted to storage
	isPersisted bool
	// isLoaded indicates if the data has been loaded from storage
	isLoaded bool
}

// New creates a new groups manager
func New(path string) (*Groups, error) {
	storage := storage.NewFile(path)
	if err := storage.Init(); err != nil {
		return nil, err
	}
	size, err := storage.Size()
	if err != nil {
		return nil, err
	}
	return &Groups{
		groups:       make(map[int][]Member),
		indexToGroup: make(map[int]int),
		nextGroupID:  1, // Start group IDs from 1
		storage:      storage,
		isPersisted:  true,
		isLoaded:     size == 0,
	}, nil
}

// load is the internal method that loads groups from storage
func (g *Groups) load() error {
	size, err := g.storage.Size()
	if err != nil {
		return err
	}
	if size == 0 {
		g.isLoaded = true
		return nil
	}

	reader, err := g.storage.Reader(0)
	if err != nil {
		return err
	}
	defer reader.Close()

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
	g.isLoaded = true

	return nil
}

// Flush saves groups to storage
func (g *Groups) Flush() error {
	if !g.isLoaded {
		return errors.New("groups are not loaded")
	}
	if g.isPersisted {
		return nil
	}

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

	g.isPersisted = true
	return nil
}

// CreateGroup creates a new group and returns its ID
func (g *Groups) CreateGroup() (int, error) {
	if !g.isLoaded {
		if err := g.load(); err != nil {
			return 0, err
		}
	}

	id := g.nextGroupID
	g.nextGroupID++
	// Initialize empty group
	g.groups[id] = []Member{}
	g.isPersisted = false
	return id, nil
}

// Assign assigns an index ID to a group with a specific place
func (g *Groups) Assign(groupID int, indexID int, place int) error {
	if !g.isLoaded {
		if err := g.load(); err != nil {
			return err
		}
	}

	if groupID >= g.nextGroupID || groupID <= 0 {
		return fmt.Errorf("invalid group ID: %d", groupID)
	}

	// Check if index ID is already assigned to a group
	if existingGroup, ok := g.indexToGroup[indexID]; ok {
		if existingGroup == groupID {
			// Already in this group, remove old entry to update place
			g.removeMemberFromGroup(groupID, indexID)
		} else {
			return fmt.Errorf("index ID %d is already assigned to group %d", indexID, existingGroup)
		}
	}

	// Check for duplicate places within the group
	if members, ok := g.groups[groupID]; ok {
		for _, m := range members {
			if m.Place == place {
				return fmt.Errorf("place %d already occupied in group %d", place, groupID)
			}
		}
	}

	member := Member{
		IndexID: indexID,
		Place:   place,
	}
	g.groups[groupID] = append(g.groups[groupID], member)
	g.indexToGroup[indexID] = groupID
	g.isPersisted = false
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

// GetGroup returns the group ID for a given index ID.
// Returns -1 if the index ID is not assigned to any group.
func (g *Groups) GetGroup(indexID int) (int, error) {
	if !g.isLoaded {
		if err := g.load(); err != nil {
			return -1, err
		}
	}

	groupID, ok := g.indexToGroup[indexID]
	if !ok {
		return -1, nil
	}
	return groupID, nil
}

// GetMembers returns all index IDs for a group, sorted by place
func (g *Groups) GetMembers(groupID int) ([]int, error) {
	if !g.isLoaded {
		if err := g.load(); err != nil {
			return nil, err
		}
	}

	if groupID <= 0 {
		return nil, fmt.Errorf("invalid group ID: %d", groupID)
	}

	members, ok := g.groups[groupID]
	if !ok {
		return nil, nil
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

	return ids, nil
}

// Count returns the number of records assigned to groups
func (g *Groups) Count() (int, error) {
	if !g.isLoaded {
		if err := g.load(); err != nil {
			return 0, err
		}
	}

	return len(g.indexToGroup), nil
}

// IsPersisted returns true if all changes have been flushed to storage
func (g *Groups) IsPersisted() bool {
	return g.isPersisted
}

// Destroy clears all groups and truncates the storage
func (g *Groups) Destroy() error {
	if !g.isLoaded {
		if err := g.load(); err != nil {
			return err
		}
	}

	// Truncate the storage file to zero size
	if err := g.storage.Truncate(0); err != nil {
		return err
	}

	// Clear in-memory data
	g.groups = make(map[int][]Member)
	g.indexToGroup = make(map[int]int)
	g.nextGroupID = 1
	g.isPersisted = true

	return nil
}
