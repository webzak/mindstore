package meta

import (
	"bytes"
	"encoding/gob"
	"errors"
	"io"

	"github.com/webzak/mindstore/internal/engine/storage"
)

// Meta manages metadata associated with integer index IDs.
// It uses lazy loading: data is loaded from storage only when first accessed.
// Note: Meta is NOT thread-safe. External synchronization is required for concurrent access.
type Meta struct {
	data        map[int]map[string]any
	storage     *storage.File
	isPersisted bool
	isLoaded    bool
}

// New creates a new Meta instance.
func New(path string) (*Meta, error) {
	storage := storage.NewFile(path)
	if err := storage.Init(); err != nil {
		return nil, err
	}
	size, err := storage.Size()
	if err != nil {
		return nil, err
	}
	return &Meta{
		data:        make(map[int]map[string]any),
		storage:     storage,
		isPersisted: true,
		isLoaded:    size == 0,
	}, nil
}

// Get returns the metadata for a given ID.
// It returns a copy of the map to prevent external modification of the internal state.
// Returns an error if loading from storage fails or if ID is negative.
func (m *Meta) Get(id int) (map[string]any, error) {
	if id < 0 {
		return nil, errors.New("id cannot be negative")
	}

	if !m.isLoaded {
		if err := m.load(); err != nil {
			return nil, err
		}
	}

	if val, ok := m.data[id]; ok {
		// Return a shallow copy
		copyMap := make(map[string]any, len(val))
		for k, v := range val {
			copyMap[k] = v
		}
		return copyMap, nil
	}
	return nil, nil
}

// Set merges the metadata for a given ID.
// It updates existing keys and adds new ones without removing existing keys.
// Returns an error if loading from storage fails or if ID is negative.
func (m *Meta) Set(id int, data map[string]any) error {
	if id < 0 {
		return errors.New("id cannot be negative")
	}

	if !m.isLoaded {
		if err := m.load(); err != nil {
			return err
		}
	}

	// Handle nil data gracefully
	if data == nil {
		data = make(map[string]any)
	}

	if _, ok := m.data[id]; !ok {
		m.data[id] = make(map[string]any)
	}
	// Merge: update existing map instead of replacing
	for k, v := range data {
		m.data[id][k] = v
	}
	m.isPersisted = false
	return nil
}

// Replace replaces all metadata for a given ID.
// This will remove any existing metadata and replace it with the provided data.
// Returns an error if loading from storage fails or if ID is negative.
func (m *Meta) Replace(id int, data map[string]any) error {
	if id < 0 {
		return errors.New("id cannot be negative")
	}

	if !m.isLoaded {
		if err := m.load(); err != nil {
			return err
		}
	}

	// Handle nil data gracefully
	if data == nil {
		data = make(map[string]any)
	}

	// Store a shallow copy
	copyMap := make(map[string]any, len(data))
	for k, v := range data {
		copyMap[k] = v
	}
	m.data[id] = copyMap
	m.isPersisted = false
	return nil
}

// SetKey sets a specific key-value pair for a given ID.
// Returns an error if loading from storage fails, if ID is negative, or if key is empty.
func (m *Meta) SetKey(id int, key string, value any) error {
	if id < 0 {
		return errors.New("id cannot be negative")
	}
	if key == "" {
		return errors.New("key cannot be empty")
	}

	if !m.isLoaded {
		if err := m.load(); err != nil {
			return err
		}
	}

	if _, ok := m.data[id]; !ok {
		m.data[id] = make(map[string]any)
	}
	m.data[id][key] = value
	m.isPersisted = false
	return nil
}

// Delete removes the metadata for a given ID.
// Returns an error if loading from storage fails or if ID is negative.
func (m *Meta) Delete(id int) error {
	if id < 0 {
		return errors.New("id cannot be negative")
	}

	if !m.isLoaded {
		if err := m.load(); err != nil {
			return err
		}
	}

	delete(m.data, id)
	m.isPersisted = false
	return nil
}

// DeleteKey removes a specific key from the metadata of a given ID.
// If the ID has no metadata after deletion, the ID entry is removed entirely.
// Returns an error if loading from storage fails, if ID is negative, or if key is empty.
func (m *Meta) DeleteKey(id int, key string) error {
	if id < 0 {
		return errors.New("id cannot be negative")
	}
	if key == "" {
		return errors.New("key cannot be empty")
	}

	if !m.isLoaded {
		if err := m.load(); err != nil {
			return err
		}
	}

	if val, ok := m.data[id]; ok {
		delete(val, key)
		if len(val) == 0 {
			delete(m.data, id)
		}
		m.isPersisted = false
	}
	return nil
}

// Flush persists the metadata to storage.
func (m *Meta) Flush() error {
	if !m.isLoaded {
		return nil
	}
	if m.isPersisted {
		return nil
	}

	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(m.data); err != nil {
		return err
	}

	// We overwrite the file content
	if err := m.storage.Truncate(0); err != nil {
		return err
	}

	w, err := m.storage.Writer(0)
	if err != nil {
		return err
	}
	defer w.Close()

	if _, err := w.Write(buf.Bytes()); err != nil {
		return err
	}

	m.isPersisted = true
	return nil
}

// Load loads the metadata from storage.
func (m *Meta) Load() error {
	return m.load()
}

// load is the internal method that loads metadata from storage
func (m *Meta) load() error {
	size, err := m.storage.Size()
	if err != nil {
		return err
	}
	if size == 0 {
		m.isLoaded = true
		return nil
	}

	data := make([]byte, size)
	r, err := m.storage.Reader(0)
	if err != nil {
		return err
	}
	defer r.Close()

	if _, err := io.ReadFull(r, data); err != nil {
		return err
	}

	buf := bytes.NewBuffer(data)
	dec := gob.NewDecoder(buf)
	if err := dec.Decode(&m.data); err != nil {
		return err
	}

	m.isLoaded = true
	return nil
}

// Count returns the number of records with metadata.
// Returns an error if loading from storage fails.
func (m *Meta) Count() (int, error) {
	if !m.isLoaded {
		if err := m.load(); err != nil {
			return 0, err
		}
	}

	return len(m.data), nil
}

// IsPersisted returns true if all changes have been flushed to storage.
func (m *Meta) IsPersisted() bool {
	return m.isPersisted
}

// Destroy clears all metadata and truncates the storage file.
// Returns an error if loading from storage fails or if truncation fails.
func (m *Meta) Destroy() error {
	if !m.isLoaded {
		if err := m.load(); err != nil {
			return err
		}
	}

	// Truncate the storage file to zero size
	if err := m.storage.Truncate(0); err != nil {
		return err
	}

	// Clear in-memory data
	m.data = make(map[int]map[string]any)
	m.isPersisted = true

	return nil
}
