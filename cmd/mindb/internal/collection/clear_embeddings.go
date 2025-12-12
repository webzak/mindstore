package collection

import (
	"flag"
	"fmt"
	"os"

	"github.com/webzak/mindstore/db/collection"
)

const clearEmbeddingsUsage = `Clear all embeddings from a collection

Usage:
  mindb collection clear-embeddings --path <path> --name <name>

Required Flags:
  --path string
        Directory path where the collection is stored
  --name string
        Name of the collection

Description:
  This command removes all vector embeddings from the collection.
  The records themselves (data, metadata, tags, groups) are preserved.
  You can regenerate embeddings later using 'create-embeddings' command.

Example:
  mindb collection clear-embeddings --path /storage/mindb --name mydata
`

type clearEmbeddingsFlags struct {
	path string
	name string
}

func parseClearEmbeddingsFlags() (*clearEmbeddingsFlags, error) {
	flags := &clearEmbeddingsFlags{}

	fs := flag.NewFlagSet("clear-embeddings", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	fs.Usage = func() {
		fmt.Fprint(os.Stderr, clearEmbeddingsUsage)
	}

	fs.StringVar(&flags.path, "path", "", "Directory path (required)")
	fs.StringVar(&flags.path, "p", "", "Directory path (shorthand)")
	fs.StringVar(&flags.name, "name", "", "Collection name (required)")
	fs.StringVar(&flags.name, "n", "", "Collection name (shorthand)")

	if err := fs.Parse(os.Args[1:]); err != nil {
		return nil, err
	}

	// Resolve path
	var err error
	flags.path, err = resolvePath(flags.path)
	if err != nil {
		return nil, err
	}

	// Validate required flags
	if flags.name == "" {
		return nil, fmt.Errorf("--name is required")
	}

	return flags, nil
}

func clearEmbeddings() error {
	// Parse flags
	flags, err := parseClearEmbeddingsFlags()
	if err != nil {
		fmt.Fprint(os.Stderr, clearEmbeddingsUsage)
		return err
	}

	// Open collection
	coll, err := collection.OpenCollection(flags.path, flags.name)
	if err != nil {
		return fmt.Errorf("failed to open collection: %w", err)
	}
	defer coll.Close()

	// Get dataset and clear vectors
	ds := coll.GetDataset()
	if err := ds.ClearVectors(); err != nil {
		return fmt.Errorf("failed to clear vectors: %w", err)
	}

	fmt.Printf("Successfully cleared all embeddings from collection '%s'\n", flags.name)
	fmt.Println("Records (data, metadata, tags, groups) are preserved.")
	fmt.Println("Use 'create-embeddings' command to regenerate embeddings.")

	return nil
}
