package db

import (
	"encoding/json"
	"os"
)

// saveConfig saves the collection configuration as JSON
func saveConfig(path string, config DatasetConfig) error {
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
func loadConfig(path string) (DatasetConfig, error) {
	var config DatasetConfig

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
