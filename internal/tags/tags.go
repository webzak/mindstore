package tags

import (
	"encoding/gob"
	"errors"
	"fmt"
	"strings"

	"github.com/webzak/mindstore/internal/storage"
)

var (
	// ErrDuplicatedTag is returned when attempting to add a tag that already exists for an ID
	ErrDuplicatedTag = errors.New("tag already exists for this ID")
	// ErrTagNotFound is returned when attempting to remove a tag that doesn't exist for an ID
	ErrTagNotFound = errors.New("tag not found for this ID")
)

// Tags manages tag-to-ID relationships
type Tags struct {
	// storage is the underlying file storage
	storage *storage.File
	// isPersisted is true if the tags have been persisted to storage
	isPersisted bool
	// isLoaded is true if the tags have been loaded from storage
	isLoaded bool
	// forward maps Tag -> IDs
	forward map[string][]int
	// reverse maps ID -> Tags
	reverse map[int][]string
}

// New creates a new tags manager
func New(path string) (*Tags, error) {
	storage := storage.NewFile(path)
	if err := storage.Init(); err != nil {
		return nil, err
	}
	size, err := storage.Size()
	if err != nil {
		return nil, err
	}
	return &Tags{
		forward:     make(map[string][]int),
		reverse:     make(map[int][]string),
		storage:     storage,
		isPersisted: true,
		isLoaded:    size == 0,
	}, nil
}

// load loads tags from storage
func (t *Tags) load() error {

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
		t.isLoaded = true
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

	t.isLoaded = true

	return nil
}

// IsPersisted
func (t *Tags) IsPersisted() bool {
	return t.isPersisted
}

// Flush saves tags to storage
func (t *Tags) Flush() error {

	if t.isPersisted {
		return nil
	}
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
	t.isPersisted = true

	return nil
}

// Add adds a tag to an ID
func (t *Tags) Add(id int, tag string) error {

	if !t.isLoaded {
		if err := t.load(); err != nil {
			return err
		}
	}

	// Validate ID
	if id < 0 {
		return errors.New("id cannot be negative")
	}

	// Validate tag
	tag = strings.TrimSpace(tag)
	if tag == "" {
		return errors.New("tag cannot be empty")
	}
	tag = strings.ToLower(tag)

	// Check if already exists to avoid duplicates
	for _, existingID := range t.forward[tag] {
		if existingID == id {
			return ErrDuplicatedTag
		}
	}

	t.forward[tag] = append(t.forward[tag], id)
	t.reverse[id] = append(t.reverse[id], tag)
	t.isPersisted = false
	return nil
}

// GetIDs returns all IDs associated with a tag
func (t *Tags) GetIDs(tag string) ([]int, error) {

	if !t.isLoaded {
		if err := t.load(); err != nil {
			return nil, err
		}
	}

	tag = strings.ToLower(tag)
	ids, ok := t.forward[tag]
	if !ok {
		return nil, nil
	}

	// Return a copy to prevent caller from modifying internal state
	result := make([]int, len(ids))
	copy(result, ids)
	return result, nil
}

// GetTags returns all tags associated with an ID
func (t *Tags) GetTags(id int) ([]string, error) {

	if !t.isLoaded {
		if err := t.load(); err != nil {
			return nil, err
		}
	}

	tags, ok := t.reverse[id]
	if !ok {
		return nil, nil
	}

	// Return a copy
	result := make([]string, len(tags))
	copy(result, tags)
	return result, nil
}

// Remove removes a tag from an ID
func (t *Tags) Remove(id int, tag string) error {

	if !t.isLoaded {
		if err := t.load(); err != nil {
			return err
		}
	}

	// Validate ID
	if id < 0 {
		return errors.New("id cannot be negative")
	}

	// Validate tag
	tag = strings.TrimSpace(tag)
	if tag == "" {
		return errors.New("tag cannot be empty")
	}
	tag = strings.ToLower(tag)

	// Check if the tag-ID pair exists before attempting removal
	ids, tagExists := t.forward[tag]
	if !tagExists {
		return ErrTagNotFound
	}

	found := false
	for _, existingID := range ids {
		if existingID == id {
			found = true
			break
		}
	}

	if !found {
		return ErrTagNotFound
	}

	// Remove from forward map
	newIDs := make([]int, 0, len(ids)-1)
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

	// Remove from reverse map
	if tags, ok := t.reverse[id]; ok {
		newTags := make([]string, 0, len(tags)-1)
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
	t.isPersisted = false
	return nil
}

// RemoveAll removes all tags associated with an ID
func (t *Tags) RemoveAll(id int) error {
	tags, err := t.GetTags(id)
	if err != nil {
		return err
	}

	for _, tag := range tags {
		if err := t.Remove(id, tag); err != nil {
			return err
		}
	}

	return nil
}

// GetAllTags returns all unique tags in the system
func (t *Tags) GetAllTags() ([]string, error) {

	if !t.isLoaded {
		if err := t.load(); err != nil {
			return nil, err
		}
	}

	result := make([]string, 0, len(t.forward))
	for tag := range t.forward {
		result = append(result, tag)
	}
	return result, nil
}

// Destroy clears all tags and truncates the storage
func (t *Tags) Destroy() error {

	if !t.isLoaded {
		if err := t.load(); err != nil {
			return err
		}
	}

	// Truncate the storage file to zero size
	if err := t.storage.Truncate(0); err != nil {
		return err
	}

	// Clear in-memory maps
	t.forward = make(map[string][]int)
	t.reverse = make(map[int][]string)
	t.isPersisted = true

	return nil
}

// Count returns the number of records with tags
func (t *Tags) Count() (int, error) {

	if !t.isLoaded {
		if err := t.load(); err != nil {
			return 0, err
		}
	}

	return len(t.reverse), nil
}
