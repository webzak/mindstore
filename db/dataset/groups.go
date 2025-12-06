package dataset

import "fmt"

// SetGroup assigns a record to a group with a specific place/position.
// If the record is already in a different group, it will be moved automatically.
// If the record is already in the specified group, only its place will be updated.
//
// Parameters:
//   - id: The record ID to assign
//   - groupID: The group ID to assign to (must be > 0)
//   - groupPlace: The position within the group
//
// Returns an error if:
//   - The dataset is closed
//   - The record ID is invalid
//   - The group ID is invalid
//   - The place is already occupied in the target group
func (ds *Dataset) SetGroup(id, groupID, groupPlace int) error {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	if ds.closed {
		return ErrDatasetClosed
	}

	if id < 0 || id >= ds.index.Count() {
		return ErrInvalidRecordID
	}

	// Check current group membership
	currentGroupID, err := ds.groups.GetGroup(id)
	if err != nil {
		return fmt.Errorf("failed to get current group: %w", err)
	}

	// If in a different group, remove first
	if currentGroupID >= 0 && currentGroupID != groupID {
		if err := ds.groups.Remove(id); err != nil {
			return fmt.Errorf("failed to remove from current group: %w", err)
		}
	}

	// Assign to new group (or update place if same group)
	if err := ds.groups.Assign(groupID, id, groupPlace); err != nil {
		return fmt.Errorf("failed to assign to group: %w", err)
	}

	return nil
}

// UnsetGroup removes a record from its group.
// If the record is not in any group, this operation succeeds silently (idempotent).
//
// Parameters:
//   - id: The record ID to remove from its group
//
// Returns an error if:
//   - The dataset is closed
//   - The record ID is invalid
func (ds *Dataset) UnsetGroup(id int) error {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	if ds.closed {
		return ErrDatasetClosed
	}

	if id < 0 || id >= ds.index.Count() {
		return ErrInvalidRecordID
	}

	// Remove from group (no-op if not in any group)
	if err := ds.groups.Remove(id); err != nil {
		return fmt.Errorf("failed to remove from group: %w", err)
	}

	return nil
}

// GetGroupItems retrieves all items in a group, ordered by place (ascending).
// Returns an empty slice if the group has no members or doesn't exist.
//
// Parameters:
//   - groupID: The group ID to retrieve items for
//   - opts: ReadOptions bitmask to control which fields to load (ReadData, ReadMeta, ReadVector, ReadTags, ReadGroup)
//
// Returns:
//   - []Item: Slice of items ordered by their place in the group (0, 1, 2, ...)
//   - error: On dataset closed or I/O errors
func (ds *Dataset) GetGroupItems(groupID int, opts ReadOptions) ([]Item, error) {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	if ds.closed {
		return nil, ErrDatasetClosed
	}

	// Get ordered member IDs from groups
	ids, err := ds.groups.GetMembers(groupID)
	if err != nil {
		return nil, fmt.Errorf("failed to get group members: %w", err)
	}

	// Return empty slice if no members (consistent with GetIDsByTag pattern)
	if len(ids) == 0 {
		return []Item{}, nil
	}

	// Read each item with requested options
	items := make([]Item, 0, len(ids))
	for _, id := range ids {
		item, err := ds.readUnlocked(id, opts)
		if err != nil {
			return nil, fmt.Errorf("failed to read item %d: %w", id, err)
		}
		items = append(items, *item)
	}

	return items, nil
}
