package collection

import (
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/webzak/mindstore/content/text/parsers/ct7"
	"github.com/webzak/mindstore/db/collection"
)

const feedUsage = `Feed text from a file to a collection using a parser

Usage:
  mindb collection feed --path <path> --name <name> --file <file> --parser <parser>

Required Flags:
  --path string
        Directory path containing the collection
  --name string
        Name of the collection
  --file string
        Path to input file to parse
  --parser string
        Parser to use (currently supported: ct7)

Examples:
  # Feed a file using ct7 parser
  mindb collection feed --path /data --name articles \
    --file document.txt --parser ct7

  # Using default path from config
  mindb collection feed --name docs --file notes.txt --parser ct7
`

type feedFlags struct {
	path   string
	name   string
	file   string
	parser string
}

func parseFeedFlags() (*feedFlags, error) {
	flags := &feedFlags{}

	fs := flag.NewFlagSet("feed", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	fs.Usage = func() {
		fmt.Fprint(os.Stderr, feedUsage)
	}

	// Required flags
	fs.StringVar(&flags.path, "path", "", "Directory path containing the collection")
	fs.StringVar(&flags.name, "name", "", "Name of the collection")
	fs.StringVar(&flags.file, "file", "", "Path to input file to parse")
	fs.StringVar(&flags.parser, "parser", "", "Parser to use (ct7)")

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
	if flags.file == "" {
		return nil, fmt.Errorf("--file is required")
	}
	if flags.parser == "" {
		return nil, fmt.Errorf("--parser is required (available: ct7)")
	}

	return flags, nil
}

// transformCT7Chunk converts a ct7.Chunk to a collection item
func transformCT7Chunk(coll *collection.Collection, chunk *ct7.Chunk) error {
	// Create item with text data (sets descriptor to Text automatically)
	item := coll.AddText(chunk.Text)

	// Add title as metadata if present
	if chunk.Title != "" {
		item = item.WithMeta("title", chunk.Title)
	}

	// Apply to collection
	_, err := item.Apply()
	return err
}

func feed() error {
	// Parse flags
	flags, err := parseFeedFlags()
	if err != nil {
		return err
	}

	// Open collection
	coll, err := collection.OpenCollection(flags.path, flags.name)
	if err != nil {
		return fmt.Errorf("failed to open collection: %w", err)
	}
	defer coll.Close()

	// Open input file
	file, err := os.Open(flags.file)
	if err != nil {
		return fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	// Process based on parser type
	var itemCount int

	switch flags.parser {
	case "ct7":
		// Create ct7 parser with default config (no size constraints)
		cfg := ct7.Config{}
		parser := ct7.NewParser(file, cfg)

		// Process chunks
		for {
			chunk, err := parser.Next()
			if err == io.EOF {
				break
			}
			if err != nil {
				return fmt.Errorf("parser error: %w", err)
			}

			if err := transformCT7Chunk(coll, chunk); err != nil {
				return fmt.Errorf("failed to append chunk: %w", err)
			}
			itemCount++
		}

	default:
		return fmt.Errorf("unsupported parser: %s (available: ct7)", flags.parser)
	}

	// Flush all changes to disk
	if err := coll.Flush(); err != nil {
		return fmt.Errorf("failed to flush collection: %w", err)
	}

	// Print summary
	fmt.Printf("Fed %d items to collection '%s'\n", itemCount, flags.name)

	return nil
}
