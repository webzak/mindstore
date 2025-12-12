package collection

import (
	"encoding/json"
	"fmt"

	"github.com/webzak/mindstore/db/dataset"
)

// Item is a fluent builder for creating items in a collection
type Item struct {
	collection     *Collection
	data           []byte
	meta           map[string]any // Will be JSON-serialized on Apply (nil if no metadata)
	dataDescriptor DataType       // Maps to collection.DataType (Text=1, Image=2)
	metaDescriptor uint8          // Reserved for future use, currently always 0
	flags          uint8
	vector         []float32
	tags           []string
	groupID        int
	groupPlace     int
}

// NewItem creates a new empty item builder
// Provides full control for advanced use cases
func (c *Collection) NewItem() *Item {
	return &Item{
		collection: c,
	}
}

// AddText creates a new item builder with text data
// Sets DataDescriptor to Text (1) automatically
func (c *Collection) AddText(text string) *Item {
	item := c.NewItem()
	item.data = []byte(text)
	item.dataDescriptor = Text
	return item
}

// WithMeta adds a metadata key-value pair
// Can be called multiple times to build up metadata
// Metadata will be JSON-encoded on Apply()
func (i *Item) WithMeta(key string, value any) *Item {
	if i.meta == nil {
		i.meta = make(map[string]any)
	}
	i.meta[key] = value
	return i
}

// WithMetadata sets multiple metadata pairs at once
// Merges with existing metadata
func (i *Item) WithMetadata(metadata map[string]any) *Item {
	if i.meta == nil {
		i.meta = make(map[string]any)
	}
	for k, v := range metadata {
		i.meta[k] = v
	}
	return i
}

// WithTags sets tags for this item (replaces any existing tags)
func (i *Item) WithTags(tags ...string) *Item {
	i.tags = tags
	return i
}

// WithTag adds a single tag (appends to existing tags)
func (i *Item) WithTag(tag string) *Item {
	i.tags = append(i.tags, tag)
	return i
}

// WithVector sets the vector manually
func (i *Item) WithVector(vector []float32) *Item {
	i.vector = vector
	return i
}

// WithFlags sets the flags field
func (i *Item) WithFlags(flags uint8) *Item {
	i.flags = flags
	return i
}

// WithGroup assigns this item to an existing group at specified place
func (i *Item) WithGroup(groupID int, place int) *Item {
	i.groupID = groupID
	i.groupPlace = place
	return i
}

// WithNewGroup assigns this item to a new group
// GroupID will be auto-assigned on Apply()
func (i *Item) WithNewGroup() *Item {
	i.groupID = -1
	i.groupPlace = 0
	return i
}

// Apply finalizes the item and appends it to the collection
// Returns the created item with assigned ID, or error
// Does NOT call Flush() - only appends to dataset
func (i *Item) Apply() (*dataset.Item, error) {
	// Vector size validation (if vector is set)
	if i.vector != nil {
		expectedSize := i.collection.cfg.DatasetOptions.VectorSize
		if len(i.vector) != expectedSize {
			return nil, fmt.Errorf("vector size mismatch: expected %d, got %d",
				expectedSize, len(i.vector))
		}
	}

	// Serialize metadata to JSON if present
	var metaBytes []byte
	var err error
	if len(i.meta) > 0 {
		metaBytes, err = json.Marshal(i.meta)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal metadata: %w", err)
		}
	}

	// Build dataset.Item
	item := dataset.Item{
		Data:           i.data,
		Meta:           metaBytes,
		DataDescriptor: uint8(i.dataDescriptor),
		MetaDescriptor: i.metaDescriptor,
		Flags:          i.flags,
		Vector:         i.vector,
		Tags:           i.tags,
		GroupID:        i.groupID,
		GroupPlace:     i.groupPlace,
	}

	// Append to dataset
	result, err := i.collection.dataset.Append(item)
	if err != nil {
		return nil, fmt.Errorf("failed to append item: %w", err)
	}

	return result, nil
}

// Accessor methods for Item fields

// TextData returns the data as a string if the item has Text descriptor
// Returns empty string if the item is not a Text type
func (i *Item) TextData() string {
	if i.dataDescriptor != Text {
		return ""
	}
	return string(i.data)
}

// Data returns the raw data bytes regardless of descriptor type
func (i *Item) Data() []byte {
	return i.data
}

// Meta returns the metadata map
// Returns nil if no metadata is present
func (i *Item) Meta() map[string]any {
	return i.meta
}

// MetaValue retrieves a specific metadata value by key
// Returns the value and true if found, nil and false if not found
func (i *Item) MetaValue(key string) (any, bool) {
	if i.meta == nil {
		return nil, false
	}
	value, exists := i.meta[key]
	return value, exists
}

// Tags returns the tags associated with this item
func (i *Item) Tags() []string {
	return i.tags
}

// Group returns the group ID this item belongs to
// Returns 0 if item is not part of any group
func (i *Item) Group() int {
	return i.groupID
}

// GroupPlace returns the position of this item within its group
func (i *Item) GroupPlace() int {
	return i.groupPlace
}

// Flags returns the flags field
func (i *Item) Flags() uint8 {
	return i.flags
}

// Vector returns the vector data
// Returns nil if vector was not loaded (use ReturnVector option in Read)
func (i *Item) Vector() []float32 {
	return i.vector
}

// DataDescriptor returns the data type descriptor
func (i *Item) DataDescriptor() DataType {
	return i.dataDescriptor
}
