package tags

import (
	"encoding/gob"
	"fmt"
	"strings"

	"github.com/webzak/mindstore/internal/engine/storage"
)

// Tags manages tag-to-ID relationships
type Tags struct {
	// forward maps Tag -> IDs
	forward map[string][]int
	// reverse maps ID -> Tags
	reverse map[int][]string
	// storage is the underlying file storage
	storage *storage.File
}

// New creates a new tags manager
func New(path string) (*Tags, error) {
	storage := storage.NewFile(path)
	if err := storage.Init(); err != nil {
		return nil, err
	}
	return &Tags{
		forward: make(map[string][]int),
		reverse: make(map[int][]string),
		storage: storage,
	}, nil
}

// Load loads tags from storage
func (t *Tags) Load() error {

	reader, err := t.storage.Reader(0)
	if err != nil {
		return err
	}
	defer reader.Close()

	// Check if file is empty
	size, err := t.storage.Size()
	if err != nil {
		return err
	}
	if size == 0 {
		return nil
	}

	decoder := gob.NewDecoder(reader)
	if err := decoder.Decode(&t.forward); err != nil {
		return fmt.Errorf("failed to decode tags: %w", err)
	}

	// Rebuild reverse map
	t.reverse = make(map[int][]string)
	for tag, ids := range t.forward {
		for _, id := range ids {
			t.reverse[id] = append(t.reverse[id], tag)
		}
	}

	return nil
}

// Flush saves tags to storage
func (t *Tags) Flush() error {

	// Truncate file before writing
	if err := t.storage.Truncate(0); err != nil {
		return err
	}

	writer, err := t.storage.Writer(0)
	if err != nil {
		return err
	}
	defer writer.Close()

	encoder := gob.NewEncoder(writer)
	if err := encoder.Encode(t.forward); err != nil {
		return fmt.Errorf("failed to encode tags: %w", err)
	}

	return nil
}

// Add adds a tag to an ID
func (t *Tags) Add(id int, tag string) {

	tag = strings.ToLower(tag)

	// Check if already exists to avoid duplicates
	for _, existingID := range t.forward[tag] {
		if existingID == id {
			return
		}
	}

	t.forward[tag] = append(t.forward[tag], id)
	t.reverse[id] = append(t.reverse[id], tag)
}

// GetIDs returns all IDs associated with a tag
func (t *Tags) GetIDs(tag string) []int {

	tag = strings.ToLower(tag)
	ids, ok := t.forward[tag]
	if !ok {
		return nil
	}

	// Return a copy to prevent caller from modifying internal state
	result := make([]int, len(ids))
	copy(result, ids)
	return result
}

// GetTags returns all tags associated with an ID
func (t *Tags) GetTags(id int) []string {

	tags, ok := t.reverse[id]
	if !ok {
		return nil
	}

	// Return a copy
	result := make([]string, len(tags))
	copy(result, tags)
	return result
}

// Remove removes a tag from an ID
func (t *Tags) Remove(id int, tag string) {

	tag = strings.ToLower(tag)

	// Remove from forward map
	if ids, ok := t.forward[tag]; ok {
		newIDs := make([]int, 0, len(ids))
		for _, existingID := range ids {
			if existingID != id {
				newIDs = append(newIDs, existingID)
			}
		}
		if len(newIDs) == 0 {
			delete(t.forward, tag)
		} else {
			t.forward[tag] = newIDs
		}
	}

	// Remove from reverse map
	if tags, ok := t.reverse[id]; ok {
		newTags := make([]string, 0, len(tags))
		for _, existingTag := range tags {
			if existingTag != tag {
				newTags = append(newTags, existingTag)
			}
		}
		if len(newTags) == 0 {
			delete(t.reverse, id)
		} else {
			t.reverse[id] = newTags
		}
	}
}

// Count returns the number of records with tags
func (t *Tags) Count() int {
	return len(t.reverse)
}
