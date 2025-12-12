package collection

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/webzak/mindstore/db/collection"
	"github.com/webzak/mindstore/embeddings/llamacpp"
)

const createEmbeddingsUsage = `Generate embeddings for all records in a collection

Usage:
  mindb collection create-embeddings --path <path> --name <name>

Required Flags:
  --path string
        Directory path containing the collection
  --name string
        Name of the collection

Description:
  This command regenerates embeddings for all records in a collection.
  It reads the embedder configuration from the collection's config file
  and processes each record sequentially.

  Currently supported embedders:
  - llamacpp: llama.cpp server embeddings

Examples:
  # Generate embeddings for a collection
  mindb collection create-embeddings --path /data --name articles

  # Using default path from config
  mindb collection create-embeddings --name articles
`

type createEmbeddingsFlags struct {
	path string
	name string
}

func parseCreateEmbeddingsFlags() (*createEmbeddingsFlags, error) {
	flags := &createEmbeddingsFlags{}

	fs := flag.NewFlagSet("create-embeddings", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	fs.Usage = func() {
		fmt.Fprint(os.Stderr, createEmbeddingsUsage)
	}

	// Required flags
	fs.StringVar(&flags.path, "path", "", "Directory path containing the collection")
	fs.StringVar(&flags.path, "p", "", "Directory path (shorthand)")
	fs.StringVar(&flags.name, "name", "", "Name of the collection")
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

// llamacppConfig represents the configuration for llamacpp embedder
type llamacppConfig struct {
	BaseURL string `json:"base_url"`
	Model   string `json:"model"`
}

func createEmbeddings() error {
	// Parse flags
	flags, err := parseCreateEmbeddingsFlags()
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
		log.Println(name, cfg)
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

	fmt.Printf("Using embedder: %s (base_url: %s)\n", embedderName, embedderCfg.BaseURL)

	// Create llamacpp client
	client := llamacpp.New(embedderCfg.BaseURL)

	// Get total records
	totalRecords := coll.Count()
	if totalRecords == 0 {
		fmt.Println("Collection is empty, nothing to do")
		return nil
	}

	fmt.Printf("Processing %d records...\n", totalRecords)

	// Process each record
	ctx := context.Background()
	processedCount := 0
	errorCount := 0

	for id := 0; id < totalRecords; id++ {
		// Read the record (data only, no vector)
		item, err := coll.Read(id, 0)
		if err != nil {
			fmt.Printf("Warning: failed to read record %d: %v\n", id, err)
			errorCount++
			continue
		}

		// Skip if no data
		if len(item.Data()) == 0 {
			continue
		}

		// Generate embedding
		vector, err := client.Embed(ctx, item.Data())
		if err != nil {
			fmt.Printf("Warning: failed to generate embedding for record %d: %v\n", id, err)
			errorCount++
			continue
		}

		// Update the vector
		if err := coll.SetVector(id, vector); err != nil {
			fmt.Printf("Warning: failed to set vector for record %d: %v\n", id, err)
			errorCount++
			continue
		}

		processedCount++

		// Print progress every 10 records
		if (id+1)%10 == 0 || id == totalRecords-1 {
			fmt.Printf("Progress: %d/%d records processed\n", id+1, totalRecords)
		}
	}

	// Flush all changes to disk
	fmt.Println("Flushing changes to disk...")
	if err := coll.Flush(); err != nil {
		return fmt.Errorf("failed to flush collection: %w", err)
	}

	// Print summary
	fmt.Printf("\nCompleted!\n")
	fmt.Printf("  Successfully processed: %d records\n", processedCount)
	if errorCount > 0 {
		fmt.Printf("  Errors: %d records\n", errorCount)
	}

	return nil
}
