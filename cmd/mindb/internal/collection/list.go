package collection

import (
	"flag"
	"fmt"
	"os"

	visual "github.com/webzak/mindstore/cmd/mindb/internal/content/preview"
	"github.com/webzak/mindstore/db/collection"
)

const listUsage = `List all records in a collection with short-form previews

Usage:
  mindb collection list --path <path> --name <name>

Required Flags:
  --path string
        Directory path containing the collection
  --name string
        Name of the collection

Examples:
  # List all records in a collection
  mindb collection list --path /data --name articles

  # List records in default path
  mindb collection list --name articles
`

type listFlags struct {
	path string
	name string
}

func parseListFlags() (*listFlags, error) {
	flags := &listFlags{}

	fs := flag.NewFlagSet("list", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	fs.Usage = func() {
		fmt.Fprint(os.Stderr, listUsage)
	}

	// Required flags
	fs.StringVar(&flags.path, "path", "", "Directory path containing the collection (required)")
	fs.StringVar(&flags.path, "p", "", "Directory path (shorthand)")
	fs.StringVar(&flags.name, "name", "", "Name of the collection (required)")
	fs.StringVar(&flags.name, "n", "", "Name of the collection (shorthand)")

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

	return flags, nil
}

func listCmd() error {
	// Parse flags
	flags, err := parseListFlags()
	if err != nil {
		return err
	}

	// Open collection
	coll, err := collection.OpenCollection(flags.path, flags.name)
	if err != nil {
		return fmt.Errorf("failed to open collection: %w", err)
	}
	defer coll.Close()

	// Get total count
	count := coll.Count()
	if count == 0 {
		fmt.Printf("Collection '%s' is empty (0 records)\n", flags.name)
		return nil
	}

	fmt.Printf("Collection '%s' - %d record(s)\n\n", flags.name, count)

	// Iterate through all records
	for id := 0; id < count; id++ {
		// Read item with vector
		item, err := coll.Read(id, collection.ReturnVector)
		if err != nil {
			fmt.Printf("Record ID: %d\n", id)
			fmt.Printf("Error: failed to read item: %v\n", err)
			fmt.Println("================================================================================")
			continue
		}

		// Display record in block format
		fmt.Printf("Record ID: %d\n", id)
		fmt.Println("---")

		// Data - show preview for text, size for binary
		if item.DataDescriptor() == collection.Text {
			fmt.Printf("Data (Text):\n%s\n\n", visual.TextShort(item.TextData()))
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

		// Vector (show in preview format when it exists)
		if vec := item.Vector(); len(vec) > 0 {
			fmt.Printf("Vector:\n%s\n\n", visual.VectorShort(vec))
		}

		// Separator between records
		fmt.Println("================================================================================")
	}

	return nil
}
