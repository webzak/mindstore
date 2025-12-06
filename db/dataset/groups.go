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
