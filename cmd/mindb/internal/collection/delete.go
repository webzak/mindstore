package collection

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/webzak/mindstore/db/collection"
)

const deleteUsage = `Delete a collection and all its data

Usage:
  mindb collection delete --path <path> --name <name> [flags]

Required Flags:
  --path string
        Directory path containing the collection
  --name string
        Name of the collection

Optional Flags:
  --force
        Skip confirmation prompt (for scripts)

Examples:
  # Delete a collection (with confirmation prompt)
  mindb collection delete --name old_articles

  # Delete with explicit path
  mindb collection delete --path /data --name old_articles

  # Delete without confirmation (for scripts)
  mindb collection delete --name old_articles --force

WARNING: This permanently deletes all collection data including:
  - All records and metadata
  - All vectors and embeddings
  - All tags and groups
  - Configuration files

This operation CANNOT be undone.
`

type deleteFlags struct {
	path  string
	name  string
	force bool
}

func parseDeleteFlags() (*deleteFlags, error) {
	flags := &deleteFlags{}

	fs := flag.NewFlagSet("delete", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	fs.Usage = func() {
		fmt.Fprint(os.Stderr, deleteUsage)
	}

	fs.StringVar(&flags.path, "path", "", "Directory path containing the collection")
	fs.StringVar(&flags.name, "name", "", "Name of the collection (required)")
	fs.BoolVar(&flags.force, "force", false, "Skip confirmation prompt (for scripts)")

	if err := fs.Parse(os.Args[1:]); err != nil {
		return nil, err
	}

	// Resolve path (use flag value or default from config)
	var err error
	flags.path, err = resolvePath(flags.path)
	if err != nil {
		return nil, err
	}

	// Validate required flags
	if flags.name == "" {
		return nil, fmt.Errorf("--name is required\n\n%s", deleteUsage)
	}

	return flags, nil
}

// checkCollectionNotLocked verifies that the collection is not currently locked by another process.
// It attempts to acquire an exclusive lock on the .lock file and immediately releases it.
func checkCollectionNotLocked(collectionDir string) error {
	lockPath := filepath.Join(collectionDir, ".lock")

	// Try to open the lock file
	lockFile, err := os.OpenFile(lockPath, os.O_RDONLY, 0666)
	if err != nil {
		if os.IsNotExist(err) {
			// No lock file means collection is not locked
			return nil
		}
		return fmt.Errorf("failed to check lock status: %w", err)
	}
	defer lockFile.Close()

	// Try to acquire exclusive lock (non-blocking)
	err = syscall.Flock(int(lockFile.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
	if err != nil {
		if err == syscall.EWOULDBLOCK {
			return fmt.Errorf("collection is currently in use by another process")
		}
		return fmt.Errorf("failed to check lock status: %w", err)
	}

	// Release the lock immediately
	syscall.Flock(int(lockFile.Fd()), syscall.LOCK_UN)

	return nil
}

// getCollectionInfoForDelete retrieves collection statistics for display before deletion.
func getCollectionInfoForDelete(path, name string) (collectionPath string, recordCount int, totalSize int64, err error) {
	collectionPath = filepath.Join(path, name)

	// Open the collection
	coll, err := collection.OpenCollection(path, name)
	if err != nil {
		return collectionPath, 0, 0, fmt.Errorf("failed to open collection: %w", err)
	}
	defer coll.Close()

	// Get record count
	recordCount = coll.Count()

	// Calculate total directory size
	entries, err := os.ReadDir(collectionPath)
	if err != nil {
		return collectionPath, recordCount, 0, fmt.Errorf("failed to read directory: %w", err)
	}

	for _, entry := range entries {
		if !entry.IsDir() {
			info, err := entry.Info()
			if err != nil {
				continue
			}
			totalSize += info.Size()
		}
	}

	return collectionPath, recordCount, totalSize, nil
}

// formatSize formats bytes into a human-readable string.
func formatSize(bytes int64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(bytes)/float64(div), "KMGTPE"[exp])
}

// confirmDeletion prompts the user to confirm deletion by typing "DELETE".
func confirmDeletion(collectionPath, name string, recordCount int, totalSize int64) (bool, error) {
	fmt.Println()
	fmt.Printf("WARNING: You are about to permanently delete collection '%s'.\n", name)
	fmt.Println()
	fmt.Println("Collection Details:")
	fmt.Printf("  Location: %s\n", collectionPath)
	fmt.Printf("  Records:  %d\n", recordCount)
	fmt.Printf("  Size:     %s\n", formatSize(totalSize))
	fmt.Println()
	fmt.Println("This will delete all records, metadata, vectors, tags, groups, and configuration.")
	fmt.Println("This operation CANNOT be undone.")
	fmt.Println()
	fmt.Printf("Type 'DELETE' to confirm deletion of collection '%s': ", name)

	reader := bufio.NewReader(os.Stdin)
	input, err := reader.ReadString('\n')
	if err != nil {
		return false, fmt.Errorf("failed to read input: %w", err)
	}

	input = strings.TrimSpace(input)
	return input == "DELETE", nil
}

// deleteCmd is the main entry point for the delete command.
func deleteCmd() error {
	// Parse flags
	flags, err := parseDeleteFlags()
	if err != nil {
		return err
	}

	// Build collection directory path
	collectionDir := filepath.Join(flags.path, flags.name)

	// Check if directory exists
	info, err := os.Stat(collectionDir)
	if err != nil {
		if os.IsNotExist(err) {
			return fmt.Errorf("collection '%s' not found at %s", flags.name, flags.path)
		}
		return fmt.Errorf("failed to access collection: %w", err)
	}

	// Ensure it's actually a directory
	if !info.IsDir() {
		return fmt.Errorf("'%s' is not a directory", collectionDir)
	}

	// Check if collection is locked
	if err := checkCollectionNotLocked(collectionDir); err != nil {
		return fmt.Errorf("cannot delete collection '%s': %w", flags.name, err)
	}

	// If not force mode, get info and confirm
	if !flags.force {
		collectionPath, recordCount, totalSize, err := getCollectionInfoForDelete(flags.path, flags.name)
		if err != nil {
			return err
		}

		confirmed, err := confirmDeletion(collectionPath, flags.name, recordCount, totalSize)
		if err != nil {
			return err
		}

		if !confirmed {
			fmt.Println("\nDeletion cancelled.")
			return nil
		}
	}

	// Delete the directory
	if err := os.RemoveAll(collectionDir); err != nil {
		return fmt.Errorf("failed to delete collection: %w", err)
	}

	fmt.Printf("\nSuccessfully deleted collection '%s'\n", flags.name)
	return nil
}
