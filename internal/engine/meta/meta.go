package meta

import (
	"bytes"
	"encoding/gob"
	"io"

	"github.com/webzak/mindstore/internal/engine/storage"
)

// Meta manages metadata associated with integer index IDs.
type Meta struct {
	data    map[int]map[string]any
	storage *storage.File
}

// NewMeta creates a new Meta instance.
func NewMeta(storage *storage.File) *Meta {
	return &Meta{
		data:    make(map[int]map[string]any),
		storage: storage,
	}
}

// Get returns the metadata for a given ID.
// It returns a copy of the map to prevent external modification of the internal state.
func (m *Meta) Get(id int) map[string]any {
	if val, ok := m.data[id]; ok {
		// Return a shallow copy
		copyMap := make(map[string]any, len(val))
		for k, v := range val {
			copyMap[k] = v
		}
		return copyMap
	}
	return nil
}

// Set merges the metadata for a given ID.
// It updates existing keys and adds new ones without removing existing keys.
func (m *Meta) Set(id int, data map[string]any) {
	if _, ok := m.data[id]; !ok {
		m.data[id] = make(map[string]any)
	}
	// Merge: update existing map instead of replacing
	for k, v := range data {
		m.data[id][k] = v
	}
}

// Replace replaces all metadata for a given ID.
// This will remove any existing metadata and replace it with the provided data.
func (m *Meta) Replace(id int, data map[string]any) {
	// Store a shallow copy
	copyMap := make(map[string]any, len(data))
	for k, v := range data {
		copyMap[k] = v
	}
	m.data[id] = copyMap
}

// SetKey sets a specific key-value pair for a given ID.
func (m *Meta) SetKey(id int, key string, value any) {
	if _, ok := m.data[id]; !ok {
		m.data[id] = make(map[string]any)
	}
	m.data[id][key] = value
}

// Delete removes the metadata for a given ID.
func (m *Meta) Delete(id int) {
	delete(m.data, id)
}

// DeleteKey removes a specific key from the metadata of a given ID.
func (m *Meta) DeleteKey(id int, key string) {
	if val, ok := m.data[id]; ok {
		delete(val, key)
		if len(val) == 0 {
			delete(m.data, id)
		}
	}
}

// Flush persists the metadata to storage.
func (m *Meta) Flush() error {
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

	return nil
}

// Load loads the metadata from storage.
func (m *Meta) Load() error {
	size, err := m.storage.Size()
	if err != nil {
		return err
	}
	if size == 0 {
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

	return nil
}

// Count returns the number of records with metadata
func (m *Meta) Count() int {
	return len(m.data)
}
