package collection

import (
	"flag"
	"fmt"
	"os"

	visual "github.com/webzak/mindstore/cmd/mindb/internal/content/preview"
	"github.com/webzak/mindstore/db/collection"
)

const readUsage = `Read a single record from a collection by ID

Usage:
  mindb collection read --path <path> --name <name> --index <id> [flags]
  mindb collection read --path <path> --name <name> -i <id> [flags]

Required Flags:
  --path string
        Directory path containing the collection
  --name string
        Name of the collection
  --index, -i int
        Record ID to read

Optional Flags:
  --vector
        Include vector data in output

Examples:
  # Read record with ID 5
  mindb collection read --path /data --name articles --index 5

  # Read record with vector
  mindb collection read --path /data --name articles -i 10 --vector
`

type readFlags struct {
	path   string
	name   string
	index  int
	vector bool
}

func parseReadFlags() (*readFlags, error) {
	flags := &readFlags{}

	fs := flag.NewFlagSet("read", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	fs.Usage = func() {
		fmt.Fprint(os.Stderr, readUsage)
	}

	// Required flags
	fs.StringVar(&flags.path, "path", "", "Directory path containing the collection (required)")
	fs.StringVar(&flags.path, "p", "", "Directory path (shorthand)")
	fs.StringVar(&flags.name, "name", "", "Name of the collection (required)")
	fs.StringVar(&flags.name, "n", "", "Name of the collection (shorthand)")
	fs.IntVar(&flags.index, "index", -1, "Record ID to read (required)")
	fs.IntVar(&flags.index, "i", -1, "Record ID to read (shorthand)")

	// Optional flags
	fs.BoolVar(&flags.vector, "vector", false, "Include vector data in output")

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
		return nil, fmt.Errorf("--name is required")
	}
	if flags.index < 0 {
		return nil, fmt.Errorf("--index/-i is required and must be >= 0")
	}

	return flags, nil
}

func readCmd() error {
	// Parse flags
	flags, err := parseReadFlags()
	if err != nil {
		return err
	}

	// Open collection
	coll, err := collection.OpenCollection(flags.path, flags.name)
	if err != nil {
		return fmt.Errorf("failed to open collection: %w", err)
	}
	defer coll.Close()

	// Determine read options
	var opts collection.ReadOptions
	if flags.vector {
		opts = collection.ReturnVector
	}

	// Read item
	item, err := coll.Read(flags.index, opts)
	if err != nil {
		return fmt.Errorf("failed to read item: %w", err)
	}

	// Format and display output (human-readable format)
	fmt.Printf("Record ID: %d\n", flags.index)
	fmt.Println("---")

	// Data - show FULL content (not preview)
	if item.DataDescriptor() == collection.Text {
		fmt.Printf("Data (Text):\n%s\n\n", item.TextData())
	} else {
		fmt.Printf("Data: %d bytes (type: %d)\n\n", len(item.Data()), item.DataDescriptor())
	}

	// Metadata
	if meta := item.Meta(); len(meta) > 0 {
		fmt.Printf("Metadata:\n%s\n\n", visual.MetadataShort(meta))
	}

	// Tags
	if tags := item.Tags(); len(tags) > 0 {
		fmt.Printf("Tags:\n%s\n\n", visual.TagsShort(tags))
	}

	// Group
	if group := item.Group(); group > 0 {
		fmt.Printf("Group: %d (place: %d)\n\n", group, item.GroupPlace())
	}

	// Vector (if requested) - show in preview format
	if flags.vector {
		if vec := item.Vector(); vec != nil {
			fmt.Printf("Vector:\n%s\n", visual.VectorShort(vec))
		} else {
			fmt.Println("Vector: none")
		}
	}

	return nil
}
