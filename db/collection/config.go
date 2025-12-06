package collection

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/webzak/mindstore/db/dataset"
)

// Options defines the configuration options for a Collection
// This is the public API for creating collections
type Options struct {
	// DatasetOptions contains all Dataset configuration
	DatasetOptions dataset.Options

	// Embedders maps embedder names to their configurations
	// Key is a descriptive name (e.g., "llamacpp-text", "openai-image")
	// Value is embedder-specific configuration (any type, will be JSON-serialized)
	Embedders map[string]any
}

// DefaultOptions returns Options with default settings
// No embedders are configured by default
func DefaultOptions() Options {
	return Options{
		DatasetOptions: dataset.DefaultOptions(),
		Embedders:      make(map[string]any),
	}
}

// config is the internal structure persisted to disk as JSON
// It mirrors Options but is unexported
type config struct {
	DatasetOptions dataset.Options         `json:"dataset_options"`
	Embedders      map[string]json.RawMessage `json:"embedders,omitempty"`
}

// toConfig converts Options to internal config
func (o Options) toConfig() (config, error) {
	cfg := config{
		DatasetOptions: o.DatasetOptions,
		Embedders:      make(map[string]json.RawMessage),
	}

	// Convert map[string]any to map[string]json.RawMessage
	for name, embedderCfg := range o.Embedders {
		data, err := json.Marshal(embedderCfg)
		if err != nil {
			return cfg, fmt.Errorf("failed to marshal embedder config for %s: %w", name, err)
		}
		cfg.Embedders[name] = data
	}

	return cfg, nil
}

// toOptions converts internal config to Options
func (c config) toOptions() (Options, error) {
	opts := Options{
		DatasetOptions: c.DatasetOptions,
		Embedders:      make(map[string]any),
	}

	// Convert map[string]json.RawMessage to map[string]any
	for name, rawCfg := range c.Embedders {
		var embedderCfg any
		if err := json.Unmarshal(rawCfg, &embedderCfg); err != nil {
			return opts, fmt.Errorf("failed to unmarshal embedder config for %s: %w", name, err)
		}
		opts.Embedders[name] = embedderCfg
	}

	return opts, nil
}

// getEmbedder retrieves the embedder configuration for a name
// Returns nil if no embedder is configured with that name
func (c *config) getEmbedder(name string) json.RawMessage {
	if c.Embedders == nil {
		return nil
	}
	return c.Embedders[name]
}

// hasEmbedder returns true if an embedder is configured with the given name
func (c *config) hasEmbedder(name string) bool {
	return c.getEmbedder(name) != nil
}

// saveConfig saves config to <dir>/<name>.json
func saveConfig(dir, name string, cfg config) error {
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

// loadConfig loads config from <dir>/<name>.json
func loadConfig(dir, name string) (config, error) {
	var cfg config

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
