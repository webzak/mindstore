package collection

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

const usage = `
Collection Management

Usage:

mindb collection <subcommand> [flags]

Available Subcommands:

  create             Create a new collection
  append             Append text to a collection
  read               Read a single record by ID
  list               List all records in collection
  info               Show information about a collection
  delete             Delete a collection
  add                Add text to a collection
  feed               Feed text from a file to a collection
  create-embeddings  Regenerate embeddings for all records
  clear-embeddings   Clear all vector embeddings from a collection
  edit-config        Edit collection configuration
  rows               Display collection records
  vector-search      Perform vector search on a collection

Use 'mindb collection <subcommand> --help' for more information about a subcommand
`

// Run executes the collection command
func Run() error {
	if len(os.Args) < 2 {
		return fmt.Errorf("collection subcommand required\nUse 'ctxdb collection --help' for usage")
	}

	subcommand := os.Args[1]
	os.Args = os.Args[1:]

	switch subcommand {
	case "create":
		return create()
	case "append":
		return appendCmd()
	case "read":
		return readCmd()
	case "list":
		return listCmd()
	case "info":
		return infoCmd()
	case "delete":
		return deleteCmd()
	case "feed":
		return feed()
	case "create-embeddings":
		return createEmbeddings()
	case "clear-embeddings":
		return clearEmbeddings()
	case "edit-config":
		return editConfig()
	case "vector-search":
		return vectorSearch()
	case "help", "-h", "--help":
		fmt.Print(usage)
		return nil
	default:
		return fmt.Errorf("unknown subcommand: %s\nUse 'ctxdb collection --help' for usage", subcommand)
	}
}

// resolvePath resolves the collection path from flag or config file.
// If flagPath is provided, it takes precedence.
// Otherwise, reads from ~/.config/mindb/config.json
// Returns error if neither flag nor config path is available.
func resolvePath(flagPath string) (string, error) {
	// Explicit flag takes precedence
	if flagPath != "" {
		return flagPath, nil
	}

	// Try to read default from config file
	home, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("--path is required (failed to get home directory: %w)", err)
	}

	configPath := filepath.Join(home, ".config", "mindb", "config.json")

	// If config file doesn't exist, path is required
	data, err := os.ReadFile(configPath)
	if err != nil {
		if os.IsNotExist(err) {
			return "", fmt.Errorf("--path is required (no default configured)")
		}
		return "", fmt.Errorf("--path is required (failed to read config: %w)", err)
	}

	// Parse config
	var cfg struct {
		Path string `json:"path"`
	}
	if err := json.Unmarshal(data, &cfg); err != nil {
		return "", fmt.Errorf("--path is required (invalid config format: %w)", err)
	}

	// Check if path is set in config
	if cfg.Path == "" {
		return "", fmt.Errorf("--path is required (not set in config)")
	}

	return cfg.Path, nil
}
