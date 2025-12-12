package collection

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/webzak/mindstore/db/dataset"
)

// Config defines the configuration for a Collection
// This structure is persisted to disk as JSON
type Config struct {
	// DatasetOptions contains all Dataset configuration
	DatasetOptions dataset.Options `json:"dataset_options"`

	// Embedders maps embedder names to their configurations
	// Key is a descriptive name (e.g., "llamacpp-text", "openai-image")
	// Value is embedder-specific configuration as raw JSON
	Embedders map[string]json.RawMessage `json:"embedders,omitempty"`

	// Description provides optional metadata about the collection
	Description string `json:"description,omitempty"`
}

// DefaultConfig returns Config with default settings
// No embedders are configured by default
func DefaultConfig() Config {
	return Config{
		DatasetOptions: dataset.DefaultOptions(),
		Embedders:      make(map[string]json.RawMessage),
	}
}

// SaveConfig saves config to <dir>/<name>.json
func SaveConfig(dir, name string, cfg Config) error {
	configPath := filepath.Join(dir, name+".json")
	f, err := os.Create(configPath)
	if err != nil {
		return fmt.Errorf("failed to create config file: %w", err)
	}
	defer f.Close()

	encoder := json.NewEncoder(f)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(cfg); err != nil {
		return fmt.Errorf("failed to encode config: %w", err)
	}

	return nil
}

// LoadConfig loads config from <dir>/<name>.json
func LoadConfig(dir, name string) (Config, error) {
	var cfg Config

	configPath := filepath.Join(dir, name+".json")
	f, err := os.Open(configPath)
	if err != nil {
		return cfg, fmt.Errorf("failed to open config file: %w", err)
	}
	defer f.Close()

	decoder := json.NewDecoder(f)
	if err := decoder.Decode(&cfg); err != nil {
		return cfg, fmt.Errorf("failed to decode config: %w", err)
	}

	return cfg, nil
}
