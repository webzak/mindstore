package dataset

import "fmt"

const (
	// ReadData indicates whether to read data
	ReadData ReadOptions = 1 << iota
	// ReadMeta indicates whether to read metadata
	ReadMeta
	// ReadVector indicates whether to read vector data
	ReadVector
	// ReadTags indicates whether to read tags
	ReadTags
	// ReadGroup indicates whether to read group information
	ReadGroup
)

// ReadOptions specifies which components of an Item to read using bitmask flags.
// If ReadOptions is 0, only the core Data field will be populated.
type ReadOptions uint8

// has checks if a specific option is set
func (r ReadOptions) has(flag ReadOptions) bool {
	return r&flag != 0
}

// AllReadOptions returns ReadOptions with all fields set to true
func AllReadOptions() ReadOptions {
	return ReadData | ReadMeta | ReadVector | ReadTags | ReadGroup
}

// Read retrieves a record by ID and returns it as an Item.
// If opts is 0, only the index record (ID, descriptors, flags) will be populated.
// Otherwise, optional components are loaded based on the opts flags.
func (c *Dataset) Read(id int, opts ReadOptions) (*Item, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil, ErrDatasetClosed
	}

	// Get index entry
	row, err := c.index.Get(id)
	if err != nil {
		return nil, fmt.Errorf("failed to get index entry: %w", err)
	}

	// Create the item with index record data
	item := &Item{
		ID:             id,
		DataDescriptor: row.DataDescriptor,
		MetaDescriptor: row.MetaDataDescriptor,
		Flags:          row.Flags,
	}

	// If opts is 0, return only the index record
	if opts == 0 {
		return item, nil
	}

	// Read data if requested
	if opts.has(ReadData) {
		payload, err := c.data.Read(row.Offset, row.Size)
		if err != nil {
			return nil, fmt.Errorf("failed to read data: %w", err)
		}
		item.Data = payload
	}

	// Read metadata if requested and enabled
	if opts.has(ReadMeta) && c.meta != nil {
		payload, err := c.meta.Read(row.MetaOffset, row.MetaSize)
		if err != nil {
			return nil, fmt.Errorf("failed to read metadata: %w", err)
		}
		item.Meta = payload
	}

	// Read vector if requested and enabled
	if opts.has(ReadVector) && c.vectors != nil {
		// Only try to read vector if one was appended for this item
		// The vector count may be less than the index count if some items don't have vectors
		if id < c.vectors.Count() {
			vector, err := c.vectors.Get(id)
			if err != nil {
				return nil, fmt.Errorf("failed to read vector: %w", err)
			}
			item.Vector = vector
		}
	}

	// Read tags if requested and enabled
	if opts.has(ReadTags) && c.tags != nil {
		item.Tags, err = c.tags.GetTags(id)
		if err != nil {
			return nil, err
		}
		if item.Tags == nil {
			item.Tags = []string{}
		}
	}

	// Read group information if requested and enabled
	if opts.has(ReadGroup) && c.groups != nil {
		groupID, err := c.groups.GetGroup(id) // -1 means no group assinged
		if err != nil {
			return nil, err
		}
		if groupID >= 0 {
			// Find the place/position within the group
			members, err := c.groups.GetMembers(groupID)
			if err != nil {
				return nil, err
			}
			place := -1
			for i, memberID := range members {
				if memberID == id {
					place = i
					break
				}
			}
			item.GroupID = groupID
			item.GroupPlace = place
		}
	}

	return item, nil
}
