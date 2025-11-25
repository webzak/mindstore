package db

import (
	"encoding/json"
	"os"
)

// Config defines the configuration for a collection
type Config struct {
	// MaxVectorAppendBufferSize max buffer for index to be unsynced
	MaxIndexAppendBufferSize int `json:"max_index_append_buffer_size,omitempty"`
	// VectorSize is the size of the float32 vector
	VectorSize int `json:"vector_size,omitempty"`
	// MaxVectorBufferSize is the maximum amount of vectors in memory buffer
	MaxVectorBufferSize int `json:"max_vector_buffer_size,omitempty"`
	// MaxAppendBufferSize is the maximum amount of appended vectors which triggers flush
	MaxVectorAppendBufferSize int `json:"max_vector_append_buffer_size,omitempty"`
}

// saveConfig saves the collection configuration as JSON
func saveConfig(path string, config Config) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	encoder := json.NewEncoder(f)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(config); err != nil {
		return err
	}

	return nil
}

// loadConfig loads the collection configuration from JSON
func loadConfig(path string) (Config, error) {
	var config Config

	f, err := os.Open(path)
	if err != nil {
		return config, err
	}
	defer f.Close()

	decoder := json.NewDecoder(f)
	if err := decoder.Decode(&config); err != nil {
		return config, err
	}

	return config, nil
}
