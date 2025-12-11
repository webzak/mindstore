package collection

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/webzak/mindstore/db/collection"
)

const appendUsage = `Append text content to a collection

Usage:
  mindb collection append --path <path> --name <name> --text <text> [flags]
  mindb collection append --path <path> --name <name> --text-file <file> [flags]

Required Flags:
  --path string
        Directory path containing the collection
  --name string
        Name of the collection
  --text string
        Text content to append (mutually exclusive with --text-file)
  --text-file string
        Path to file containing text to append (mutually exclusive with --text)

Optional Flags:
  --tags string
        Comma-separated tags (e.g., "tag1,tag2,tag3")
  --meta string
        Metadata key=value pairs (repeatable)

Examples:
  # Append text with tags
  mindb collection append --path /data --name articles \
    --text "Hello world" --tags "greeting,test"

  # Append from file with metadata
  mindb collection append --path /data --name docs \
    --text-file article.txt \
    --meta title="My Article" \
    --meta author="John Doe" \
    --meta year=2024

  # Append with tags and metadata
  mindb collection append --path /data --name notes \
    --text "Important note" \
    --tags "urgent,todo" \
    --meta priority=1 \
    --meta reviewed=false
`

type appendFlags struct {
	path     string
	name     string
	text     string
	textFile string
	tags     string
	meta     []string
}

func parseAppendFlags() (*appendFlags, error) {
	flags := &appendFlags{}

	fs := flag.NewFlagSet("append", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	fs.Usage = func() {
		fmt.Fprint(os.Stderr, appendUsage)
	}

	// Required flags
	fs.StringVar(&flags.path, "path", "", "Directory path containing the collection (required)")
	fs.StringVar(&flags.name, "name", "", "Name of the collection (required)")

	// Text input flags (mutually exclusive)
	fs.StringVar(&flags.text, "text", "", "Text content to append (mutually exclusive with --text-file)")
	fs.StringVar(&flags.textFile, "text-file", "", "Path to file containing text to append (mutually exclusive with --text)")

	// Optional flags
	fs.StringVar(&flags.tags, "tags", "", "Comma-separated tags (e.g., \"tag1,tag2,tag3\")")

	// Repeatable metadata flag
	fs.Func("meta", "Metadata key=value pair (repeatable)", func(s string) error {
		flags.meta = append(flags.meta, s)
		return nil
	})

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

	// Validate text input (mutually exclusive)
	if flags.text != "" && flags.textFile != "" {
		return nil, fmt.Errorf("--text and --text-file are mutually exclusive")
	}
	if flags.text == "" && flags.textFile == "" {
		return nil, fmt.Errorf("either --text or --text-file is required")
	}

	return flags, nil
}

func parseTags(tagsStr string) []string {
	if tagsStr == "" {
		return nil
	}

	parts := strings.Split(tagsStr, ",")
	tags := make([]string, 0, len(parts))

	for _, tag := range parts {
		trimmed := strings.TrimSpace(tag)
		if trimmed != "" {
			tags = append(tags, trimmed)
		}
	}

	return tags
}

func parseMetadata(metaFlags []string) (map[string]any, error) {
	if len(metaFlags) == 0 {
		return nil, nil
	}

	metadata := make(map[string]any)

	for _, pair := range metaFlags {
		parts := strings.SplitN(pair, "=", 2)
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid metadata format: %q (expected key=value)", pair)
		}

		key := strings.TrimSpace(parts[0])
		value := parts[1] // Don't trim - preserve whitespace in values

		if key == "" {
			return nil, fmt.Errorf("metadata key cannot be empty in: %q", pair)
		}

		// Try to parse as JSON for type detection (handles numbers, booleans, null)
		var jsonValue any
		if err := json.Unmarshal([]byte(value), &jsonValue); err == nil {
			metadata[key] = jsonValue
		} else {
			// Fall back to string if not valid JSON
			metadata[key] = value
		}
	}

	return metadata, nil
}

func appendCmd() error {
	// Parse flags
	flags, err := parseAppendFlags()
	if err != nil {
		return err
	}

	// Read text from file if specified
	if flags.textFile != "" {
		content, err := os.ReadFile(flags.textFile)
		if err != nil {
			return fmt.Errorf("failed to read text file: %w", err)
		}
		flags.text = string(content)
	}

	// Validate text is not empty
	if flags.text == "" {
		return fmt.Errorf("text content is empty")
	}

	// Parse tags
	tags := parseTags(flags.tags)

	// Parse metadata
	metadata, err := parseMetadata(flags.meta)
	if err != nil {
		return err
	}

	// Open collection
	coll, err := collection.OpenCollection(flags.path, flags.name)
	if err != nil {
		return fmt.Errorf("failed to open collection: %w", err)
	}
	defer coll.Close()

	// Build and append item
	item := coll.AddText(flags.text)

	if metadata != nil && len(metadata) > 0 {
		item = item.WithMetadata(metadata)
	}

	if len(tags) > 0 {
		item = item.WithTags(tags...)
	}

	result, err := item.Apply()
	if err != nil {
		return fmt.Errorf("failed to append item: %w", err)
	}

	// Flush to disk
	if err := coll.Flush(); err != nil {
		return fmt.Errorf("failed to flush collection: %w", err)
	}

	// Print success message
	fmt.Printf("Item appended to collection '%s'\n", flags.name)
	fmt.Printf("  ID: %d\n", result.ID)
	fmt.Printf("  Size: %d bytes\n", len(result.Data))
	if len(tags) > 0 {
		fmt.Printf("  Tags: %d\n", len(tags))
	}
	if metadata != nil && len(metadata) > 0 {
		fmt.Printf("  Metadata: %d fields\n", len(metadata))
	}

	return nil
}
