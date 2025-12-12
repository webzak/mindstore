package collection

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"github.com/webzak/mindstore/db/collection"
	"github.com/webzak/mindstore/db/dataset"
)

const editConfigUsage = `Edit collection configuration

Usage:
  mindb collection edit-config --path <path> --name <name> [flags]

Required Flags:
  --path string
        Directory path where the collection is stored
  --name string
        Name of the collection

Optional Flags:
  --description string
        Description of the collection
  --vector-size int
        Vector dimensions (WARNING: changing this will delete all existing vectors)
  --max-data-buffer int
        Data append buffer size in bytes
  --max-meta-buffer int
        Metadata append buffer size in bytes
  --max-index-buffer int
        Index append buffer size (number of records)
  --max-vector-buffer int
        Vector buffer size (number of vectors)
  --max-vector-append-buffer int
        Vector append buffer size (number of vectors)
  --embedders string
        Embedder configurations as JSON string (merges with existing embedders)
  --embedders-file string
        Path to JSON file containing embedder configurations (merges with existing)

Description:
  This command allows you to modify collection configuration after creation.
  Only the specified flags will be updated; all other settings remain unchanged.

  Embedders are merged (added/updated) rather than replaced. To clear embedders,
  use the clear-embeddings command.

Examples:

  # Change buffer sizes for performance tuning
  mindb collection edit-config --path /storage/mindb --name mydata \
    --max-data-buffer 262144 --max-meta-buffer 65536

  # Add or update embedders
  mindb collection edit-config --path /storage/mindb --name mydata \
    --embedders '{"llamacpp-text": {"base_url": "http://localhost:3311", "model": "text"}}'

  # Change vector size (WILL DELETE ALL VECTORS)
  mindb collection edit-config --path /storage/mindb --name mydata --vector-size 512
`

type editConfigFlags struct {
	path string
	name string

	// Track which flags were explicitly set
	setFlags map[string]bool

	// Optional configuration values
	description                 string
	vectorSize                  int
	maxDataAppendBufferSize     int
	maxMetaDataAppendBufferSize int
	maxIndexAppendBufferSize    int
	maxVectorBufferSize         int
	maxVectorAppendBufferSize   int
	embedders                   string
	embeddersFile               string
}

func parseEditConfigFlags() (*editConfigFlags, error) {
	flags := &editConfigFlags{
		setFlags: make(map[string]bool),
	}

	fs := flag.NewFlagSet("edit-config", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	fs.Usage = func() {
		fmt.Fprint(os.Stderr, editConfigUsage)
	}

	// Required flags
	fs.StringVar(&flags.path, "path", "", "Directory path where the collection is stored (required)")
	fs.StringVar(&flags.path, "p", "", "Directory path (shorthand)")
	fs.StringVar(&flags.name, "name", "", "Name of the collection (required)")
	fs.StringVar(&flags.name, "n", "", "Name of the collection (shorthand)")

	// Optional collection flags
	fs.StringVar(&flags.description, "description", "", "Description of the collection")

	// Optional dataset flags with defaults (we'll track if they were explicitly set)
	fs.IntVar(&flags.vectorSize, "vector-size", dataset.DefaultVectorSize, "Vector dimensions")
	fs.IntVar(&flags.maxDataAppendBufferSize, "max-data-buffer", dataset.DefaultMaxDataAppendBufferSize, "Data append buffer size in bytes")
	fs.IntVar(&flags.maxMetaDataAppendBufferSize, "max-meta-buffer", dataset.DefaultMaxMetaDataAppendBufferSize, "Metadata append buffer size in bytes")
	fs.IntVar(&flags.maxIndexAppendBufferSize, "max-index-buffer", dataset.DefaultMaxIndexAppendBufferSize, "Index append buffer size (number of records)")
	fs.IntVar(&flags.maxVectorBufferSize, "max-vector-buffer", dataset.DefaultMaxVectorBufferSize, "Vector buffer size (number of vectors)")
	fs.IntVar(&flags.maxVectorAppendBufferSize, "max-vector-append-buffer", dataset.DefaultMaxVectorAppendBufferSize, "Vector append buffer size (number of vectors)")

	// Optional embedder flags
	fs.StringVar(&flags.embedders, "embedders", "", "Embedder configurations as JSON string")
	fs.StringVar(&flags.embeddersFile, "embedders-file", "", "Path to JSON file containing embedder configurations")

	if err := fs.Parse(os.Args[1:]); err != nil {
		return nil, err
	}

	// Track which flags were explicitly provided using Visit
	fs.Visit(func(f *flag.Flag) {
		flags.setFlags[f.Name] = true
	})

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

func editConfig() error {
	// Step 1: Parse flags
	flags, err := parseEditConfigFlags()
	if err != nil {
		fmt.Fprint(os.Stderr, editConfigUsage)
		return err
	}

	// Step 2: Load existing config
	dir := filepath.Join(flags.path, flags.name)
	cfg, err := collection.LoadConfig(dir, flags.name)
	if err != nil {
		return fmt.Errorf("failed to load config: %w", err)
	}

	// Keep a copy for comparison
	oldCfg := cfg

	// Step 3: Check if vector-size is changing
	vectorSizeChanged := false
	if flags.setFlags["vector-size"] && flags.vectorSize != cfg.DatasetOptions.VectorSize {
		fmt.Printf("WARNING: Changing vector size from %d to %d will delete ALL existing vectors!\n",
			cfg.DatasetOptions.VectorSize, flags.vectorSize)
		fmt.Print("Are you sure this is not a mistake? Continue? (y/n): ")

		var response string
		fmt.Scanln(&response)
		if response != "y" && response != "Y" {
			fmt.Println("Operation cancelled.")
			return nil
		}

		vectorSizeChanged = true
		cfg.DatasetOptions.VectorSize = flags.vectorSize
	}

	// Step 4: Apply collection-level changes
	if flags.setFlags["description"] {
		cfg.Description = flags.description
	}

	// Step 5: Apply other dataset option changes
	if flags.setFlags["max-data-buffer"] {
		cfg.DatasetOptions.MaxDataAppendBufferSize = flags.maxDataAppendBufferSize
	}
	if flags.setFlags["max-meta-buffer"] {
		cfg.DatasetOptions.MaxMetaDataAppendBufferSize = flags.maxMetaDataAppendBufferSize
	}
	if flags.setFlags["max-index-buffer"] {
		cfg.DatasetOptions.MaxIndexAppendBufferSize = flags.maxIndexAppendBufferSize
	}
	if flags.setFlags["max-vector-buffer"] {
		cfg.DatasetOptions.MaxVectorBufferSize = flags.maxVectorBufferSize
	}
	if flags.setFlags["max-vector-append-buffer"] {
		cfg.DatasetOptions.MaxVectorAppendBufferSize = flags.maxVectorAppendBufferSize
	}

	// Step 6: Handle embedder configuration
	// Initialize embedders map if nil
	if cfg.Embedders == nil {
		cfg.Embedders = make(map[string]json.RawMessage)
	}

	// First, try to load from file if provided
	if flags.embeddersFile != "" {
		fileData, err := os.ReadFile(flags.embeddersFile)
		if err != nil {
			return fmt.Errorf("failed to read embedders file: %w", err)
		}
		var embedders map[string]any
		if err := json.Unmarshal(fileData, &embedders); err != nil {
			return fmt.Errorf("failed to parse embedders file JSON: %w", err)
		}
		// Merge embedders from file
		for name, embedderCfg := range embedders {
			data, err := json.Marshal(embedderCfg)
			if err != nil {
				return fmt.Errorf("failed to marshal embedder %s: %w", name, err)
			}
			cfg.Embedders[name] = data
		}
	}

	// Then, merge with inline JSON if provided (takes precedence)
	if flags.embedders != "" {
		var embedders map[string]any
		if err := json.Unmarshal([]byte(flags.embedders), &embedders); err != nil {
			return fmt.Errorf("failed to parse embedders JSON: %w", err)
		}
		// Merge embedders from inline JSON
		for name, embedderCfg := range embedders {
			data, err := json.Marshal(embedderCfg)
			if err != nil {
				return fmt.Errorf("failed to marshal embedder %s: %w", name, err)
			}
			cfg.Embedders[name] = data
		}
	}

	// Step 7: Save updated config
	if err := collection.SaveConfig(dir, flags.name, cfg); err != nil {
		return fmt.Errorf("failed to save config: %w", err)
	}

	// Step 8: If vector size changed, clear vectors using dataset
	if vectorSizeChanged {
		coll, err := collection.OpenCollection(flags.path, flags.name)
		if err != nil {
			return fmt.Errorf("failed to open collection: %w", err)
		}
		defer coll.Close()

		ds := coll.GetDataset()
		if err := ds.ClearVectors(); err != nil {
			return fmt.Errorf("failed to clear vectors: %w", err)
		}

		fmt.Println("All vectors cleared due to vector size change.")
	}

	// Step 9: Display what changed
	printConfigChanges(flags.name, oldCfg, cfg, vectorSizeChanged)

	return nil
}

func printConfigChanges(name string, oldCfg, newCfg collection.Config, vectorsCleared bool) {
	fmt.Printf("\nCollection '%s' configuration updated.\n\n", name)

	hasChanges := false

	// Check collection-level changes
	if oldCfg.Description != newCfg.Description {
		hasChanges = true
		fmt.Printf("  description: \"%s\" -> \"%s\"\n", oldCfg.Description, newCfg.Description)
	}

	// Check dataset options changes
	if oldCfg.DatasetOptions.VectorSize != newCfg.DatasetOptions.VectorSize {
		hasChanges = true
		suffix := ""
		if vectorsCleared {
			suffix = " (all vectors cleared)"
		}
		fmt.Printf("  vector_size: %d -> %d%s\n",
			oldCfg.DatasetOptions.VectorSize, newCfg.DatasetOptions.VectorSize, suffix)
	}

	if oldCfg.DatasetOptions.MaxDataAppendBufferSize != newCfg.DatasetOptions.MaxDataAppendBufferSize {
		hasChanges = true
		fmt.Printf("  max_data_append_buffer_size: %d -> %d\n",
			oldCfg.DatasetOptions.MaxDataAppendBufferSize, newCfg.DatasetOptions.MaxDataAppendBufferSize)
	}

	if oldCfg.DatasetOptions.MaxMetaDataAppendBufferSize != newCfg.DatasetOptions.MaxMetaDataAppendBufferSize {
		hasChanges = true
		fmt.Printf("  max_meta_data_append_buffer_size: %d -> %d\n",
			oldCfg.DatasetOptions.MaxMetaDataAppendBufferSize, newCfg.DatasetOptions.MaxMetaDataAppendBufferSize)
	}

	if oldCfg.DatasetOptions.MaxIndexAppendBufferSize != newCfg.DatasetOptions.MaxIndexAppendBufferSize {
		hasChanges = true
		fmt.Printf("  max_index_append_buffer_size: %d -> %d\n",
			oldCfg.DatasetOptions.MaxIndexAppendBufferSize, newCfg.DatasetOptions.MaxIndexAppendBufferSize)
	}

	if oldCfg.DatasetOptions.MaxVectorBufferSize != newCfg.DatasetOptions.MaxVectorBufferSize {
		hasChanges = true
		fmt.Printf("  max_vector_buffer_size: %d -> %d\n",
			oldCfg.DatasetOptions.MaxVectorBufferSize, newCfg.DatasetOptions.MaxVectorBufferSize)
	}

	if oldCfg.DatasetOptions.MaxVectorAppendBufferSize != newCfg.DatasetOptions.MaxVectorAppendBufferSize {
		hasChanges = true
		fmt.Printf("  max_vector_append_buffer_size: %d -> %d\n",
			oldCfg.DatasetOptions.MaxVectorAppendBufferSize, newCfg.DatasetOptions.MaxVectorAppendBufferSize)
	}

	// Check embedders changes
	if len(newCfg.Embedders) != len(oldCfg.Embedders) ||
		!embeddersEqual(oldCfg.Embedders, newCfg.Embedders) {
		hasChanges = true
		fmt.Println("  embedders:")

		// Find added/updated embedders
		for name := range newCfg.Embedders {
			if _, exists := oldCfg.Embedders[name]; !exists {
				fmt.Printf("    + %s (added)\n", name)
			} else if string(oldCfg.Embedders[name]) != string(newCfg.Embedders[name]) {
				fmt.Printf("    ~ %s (updated)\n", name)
			}
		}
	}

	if !hasChanges {
		fmt.Println("No changes were made to the configuration.")
	}
}

func embeddersEqual(a, b map[string]json.RawMessage) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if bv, ok := b[k]; !ok || string(v) != string(bv) {
			return false
		}
	}
	return true
}
