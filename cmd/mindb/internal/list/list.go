package list

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"syscall"

	"github.com/webzak/mindstore/db/collection"
)

const usage = `List all collections in a directory

Usage:
  mindb list [--path <path>]

Optional Flags:
  --path, -p string
        Directory path containing collections
        (uses default from config if not specified)

Examples:
  # List all collections in default path
  mindb list

  # List all collections in specific path
  mindb list --path /data/collections
`

type flags struct {
	path string
}

func parseFlags() (*flags, error) {
	f := &flags{}

	fs := flag.NewFlagSet("list", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	fs.Usage = func() {
		fmt.Fprint(os.Stderr, usage)
	}

	fs.StringVar(&f.path, "path", "", "Directory path containing collections")
	fs.StringVar(&f.path, "p", "", "Directory path (shorthand)")

	if err := fs.Parse(os.Args[1:]); err != nil {
		return nil, err
	}

	// Resolve path (use flag value or default from config)
	var err error
	f.path, err = resolvePath(f.path)
	if err != nil {
		return nil, err
	}

	return f, nil
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

// isCollectionLocked checks if a collection is currently locked by another process
func isCollectionLocked(collectionDir string) bool {
	lockPath := filepath.Join(collectionDir, ".lock")

	// Try to open the lock file
	lockFile, err := os.OpenFile(lockPath, os.O_RDONLY, 0666)
	if err != nil {
		// No lock file means collection is not locked
		return false
	}
	defer lockFile.Close()

	// Try to acquire exclusive lock (non-blocking)
	err = syscall.Flock(int(lockFile.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
	if err != nil {
		// Collection is locked
		return true
	}

	// Release the lock immediately
	syscall.Flock(int(lockFile.Fd()), syscall.LOCK_UN)
	return false
}

type collectionInfo struct {
	name        string
	description string
	locked      bool
	// Stats (only available if not locked)
	records       int
	vectors       int
	tags          int
	groups        int
	meta          int
	embedders     int
	embedderNames []string
}

// getCollectionInfo retrieves information about a collection
func getCollectionInfo(basePath, name string) (*collectionInfo, error) {
	collectionDir := filepath.Join(basePath, name)
	info := &collectionInfo{
		name: name,
	}

	// Check if config file exists
	configPath := filepath.Join(collectionDir, name+".json")
	if _, err := os.Stat(configPath); err != nil {
		return nil, fmt.Errorf("not a valid collection: %w", err)
	}

	// Load config to get description and embedders
	cfg, err := collection.LoadConfig(collectionDir, name)
	if err != nil {
		return nil, fmt.Errorf("failed to load config: %w", err)
	}
	info.description = cfg.Description

	// Get embedder names
	if len(cfg.Embedders) > 0 {
		info.embedderNames = make([]string, 0, len(cfg.Embedders))
		for embName := range cfg.Embedders {
			info.embedderNames = append(info.embedderNames, embName)
		}
		sort.Strings(info.embedderNames)
		info.embedders = len(info.embedderNames)
	}

	// Check if collection is locked
	info.locked = isCollectionLocked(collectionDir)

	// If not locked, open collection and get full stats
	if !info.locked {
		coll, err := collection.OpenCollection(basePath, name)
		if err != nil {
			return nil, fmt.Errorf("failed to open collection: %w", err)
		}
		defer coll.Close()

		stats, err := coll.GetStats()
		if err != nil {
			return nil, fmt.Errorf("failed to get stats: %w", err)
		}

		info.records = stats.TotalRecords
		info.vectors = stats.RecordsWithVectors
		info.tags = len(stats.TagCounts)
		info.groups = stats.TotalGroups
		info.meta = len(stats.MetadataKeyCounts)
	}

	return info, nil
}

// printCollectionInfo prints formatted information about a collection
func printCollectionInfo(info *collectionInfo) {
	// First line: name and stats
	if info.locked {
		// Locked: show only config-based info
		fmt.Printf("%s (currently in use)", info.name)
		if info.embedders > 0 {
			fmt.Printf(" - Embedders: %d", info.embedders)
		}
		fmt.Println()
	} else {
		// Available: show full stats
		fmt.Printf("%s - Records: %d", info.name, info.records)

		// Show optional stats only if present
		if info.vectors > 0 {
			fmt.Printf(", Vectors: %d", info.vectors)
		}
		if info.embedders > 0 {
			fmt.Printf(", Embedders: %d", info.embedders)
		}
		if info.tags > 0 {
			fmt.Printf(", Tags: %d", info.tags)
		}
		if info.meta > 0 {
			fmt.Printf(", Meta: %d", info.meta)
		}
		if info.groups > 0 {
			fmt.Printf(", Groups: %d", info.groups)
		}
		fmt.Println()
	}

	// Description on subsequent lines (if present)
	if info.description != "" {
		fmt.Printf("  %s\n", info.description)
	}
}

// Run is the entry point for the list command
func Run() error {
	// Parse flags
	flags, err := parseFlags()
	if err != nil {
		return err
	}

	// Check if path exists
	pathInfo, err := os.Stat(flags.path)
	if err != nil {
		if os.IsNotExist(err) {
			return fmt.Errorf("path does not exist: %s", flags.path)
		}
		return fmt.Errorf("failed to access path: %w", err)
	}

	if !pathInfo.IsDir() {
		return fmt.Errorf("path is not a directory: %s", flags.path)
	}

	// Read all subdirectories
	entries, err := os.ReadDir(flags.path)
	if err != nil {
		return fmt.Errorf("failed to read directory: %w", err)
	}

	// Collect valid collections
	var collections []*collectionInfo
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}

		// Try to get collection info
		info, err := getCollectionInfo(flags.path, entry.Name())
		if err != nil {
			// Skip non-collection directories silently
			continue
		}

		collections = append(collections, info)
	}

	// Sort collections by name
	sort.Slice(collections, func(i, j int) bool {
		return collections[i].name < collections[j].name
	})

	// Print results
	if len(collections) == 0 {
		fmt.Printf("No collections found in %s\n", flags.path)
		return nil
	}

	fmt.Printf("Collections in %s:\n\n", flags.path)
	for i, info := range collections {
		printCollectionInfo(info)
		// Add blank line between collections (but not after the last one)
		if i < len(collections)-1 {
			fmt.Println()
		}
	}

	return nil
}
