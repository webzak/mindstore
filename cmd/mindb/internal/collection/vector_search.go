package collection

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"

	visual "github.com/webzak/mindstore/cmd/mindb/internal/content/text/preview"
	"github.com/webzak/mindstore/db/collection"
	"github.com/webzak/mindstore/embeddings"
	"github.com/webzak/mindstore/embeddings/llamacpp"
)

const vectorSearchUsage = `Perform vector similarity search on a collection

Usage:
  mindb collection vector-search --path <path> --name <name> --text <query> [flags]

Required Flags:
  --path string
        Directory path containing the collection
  --name string
        Name of the collection
  --text string
        Search query text

Optional Flags:
  --limit int
        Maximum number of results to return (default: 3)

Description:
  This command performs semantic search on a collection using natural language queries.
  It generates an embedding for the query text using the collection's configured embedder,
  then finds and displays the most similar records sorted by distance (descending).

  Currently supported embedders:
  - llamacpp: llama.cpp server embeddings

Examples:
  # Search for similar records (default 3 results)
  mindb collection vector-search --path /data --name articles --text "machine learning"

  # Search with custom result limit
  mindb collection vector-search --path /data --name docs --text "tutorial" --limit 5

  # Using default path from config
  mindb collection vector-search --name articles --text "AI and robotics"
`

type vectorSearchFlags struct {
	path  string
	name  string
	text  string
	limit int
}

func parseVectorSearchFlags() (*vectorSearchFlags, error) {
	flags := &vectorSearchFlags{}

	fs := flag.NewFlagSet("vector-search", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	fs.Usage = func() {
		fmt.Fprint(os.Stderr, vectorSearchUsage)
	}

	// Required flags
	fs.StringVar(&flags.path, "path", "", "Directory path containing the collection")
	fs.StringVar(&flags.name, "name", "", "Name of the collection")
	fs.StringVar(&flags.text, "text", "", "Search query text")

	// Optional flags
	fs.IntVar(&flags.limit, "limit", 3, "Maximum number of results to return")

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
	if flags.text == "" {
		return nil, fmt.Errorf("--text is required")
	}

	// Validate limit
	if flags.limit < 1 {
		return nil, fmt.Errorf("--limit must be >= 1")
	}

	return flags, nil
}

func vectorSearch() error {
	// Parse flags
	flags, err := parseVectorSearchFlags()
	if err != nil {
		return err
	}

	// Open collection
	coll, err := collection.OpenCollection(flags.path, flags.name)
	if err != nil {
		return fmt.Errorf("failed to open collection: %w", err)
	}
	defer coll.Close()

	// Get embedders config
	embeddersConfig, err := coll.GetEmbeddersConfig()
	if err != nil {
		return fmt.Errorf("failed to get embedders config: %w", err)
	}

	// Check if any embedders are configured
	if len(embeddersConfig) == 0 {
		return fmt.Errorf("no embedders configured for collection '%s'", flags.name)
	}

	// Find the first llamacpp embedder
	var embedderName string
	var embedderCfg llamacppConfig
	for name, cfg := range embeddersConfig {
		// Try to unmarshal as llamacpp config
		cfgBytes, err := json.Marshal(cfg)
		if err != nil {
			continue
		}

		var llamaCfg llamacppConfig
		if err := json.Unmarshal(cfgBytes, &llamaCfg); err != nil {
			continue
		}

		// Validate that base_url is present
		if llamaCfg.BaseURL != "" {
			embedderName = name
			embedderCfg = llamaCfg
			break
		}
	}

	if embedderName == "" {
		return fmt.Errorf("no valid llamacpp embedder found in collection config")
	}

	// Create llamacpp client
	client := llamacpp.New(embedderCfg.BaseURL)

	// Generate query embedding
	ctx := context.Background()
	queryVector, err := client.Embed(ctx, []byte(flags.text))
	if err != nil {
		return fmt.Errorf("failed to generate query embedding: %w", err)
	}

	// Perform vector search
	searchOpts := collection.VectorSearchOptions{
		Limit:     flags.limit,
		SortOrder: embeddings.SortDesc, // Most similar first
	}
	results, err := coll.VectorSearch(queryVector, searchOpts)
	if err != nil {
		return fmt.Errorf("failed to search: %w", err)
	}

	// Display results
	fmt.Printf("Search Results for: %q\n\n", flags.text)
	fmt.Printf("Found %d results\n", len(results))

	if len(results) == 0 {
		fmt.Println("\nNo results found")
		return nil
	}

	// Print each result
	for i, result := range results {
		fmt.Println("\n---")
		fmt.Printf("Result %d (Distance: %.3f)\n\n", i+1, result.Distance)

		// Data - show FULL content (not preview)
		if result.Item.DataDescriptor() == collection.Text {
			fmt.Printf("Data (Text):\n%s\n\n", result.Item.TextData())
		} else {
			fmt.Printf("Data: %d bytes (type: %d)\n\n", len(result.Item.Data()), result.Item.DataDescriptor())
		}

		// Metadata
		if meta := result.Item.Meta(); meta != nil && len(meta) > 0 {
			fmt.Printf("Metadata:\n%s\n\n", visual.MetadataShort(meta))
		}

		// Tags
		if tags := result.Item.Tags(); len(tags) > 0 {
			fmt.Printf("Tags:\n%s\n\n", visual.TagsShort(tags))
		}

		// Group
		if group := result.Item.Group(); group > 0 {
			fmt.Printf("Group: %d (place: %d)\n\n", group, result.Item.GroupPlace())
		}
	}

	return nil
}
