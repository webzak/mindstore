package collection

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"

	"github.com/webzak/mindstore/db/collection"
	"github.com/webzak/mindstore/db/dataset"
)

const createUsage = `Create a new collection

Usage:
  mindb collection create --path <path> --name <name> [flags]

Required Flags:
  --path string
        Directory path where the collection will be stored
  --name string
        Name of the collection

Optional Flags:
  --description string
        Description of the collection
  --vector-size int
        Vector dimensions (default: 768)
  --max-data-buffer int
        Data append buffer size in bytes (default: 131072)
  --max-meta-buffer int
        Metadata append buffer size in bytes (default: 32768)
  --max-index-buffer int
        Index append buffer size (number of records) (default: 64)
  --max-vector-buffer int
        Vector buffer size (number of vectors) (default: 64)
  --max-vector-append-buffer int
        Vector append buffer size (number of vectors) (default: 64)
  --embedders string
        Embedder configurations as JSON string (takes precedence over --embedders-file)
  --embedders-file string
        Path to JSON file containing embedder configurations

Examples:

  # Create a basic collection with default settings
  mindb collection create --path /storage/mindb --name mydata

  # Create collection with custom vector size
  mindb collection create --path /storage/mindb --name mydata --vector-size 384

  # Create collection with custom buffer sizes for performance tuning
  mindb collection create --path /storage/mindb --name mydata \
    --vector-size 768 \
    --max-data-buffer 262144 \
    --max-meta-buffer 65536

  # Create collection with embedders (inline JSON)
  mindb collection create --path /storage/mindb --name foo --vector-size 768 \
    --embedders '{"llamacpp-text": {"base_url": "http://localhost:3311", "model": "text"}}'

  # Create collection with embedders (from file)
  mindb collection create --path /storage/mindb --name bar --vector-size 384 \
    --embedders-file /path/to/embedders.json

Embedder JSON Format:
  {
    "llamacpp-text": {
      "base_url": "http://localhost:3311",
      "model": "text"
    },
    "openai-image": {
      "api_key": "sk-test",
      "model": "clip"
    }
  }
`

type createFlags struct {
	path                        string
	name                        string
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

func parseCreateFlags() (*createFlags, error) {
	flags := &createFlags{}

	fs := flag.NewFlagSet("create", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	fs.Usage = func() {
		fmt.Fprint(os.Stderr, createUsage)
	}

	// Required flags
	fs.StringVar(&flags.path, "path", "", "Directory path where the collection will be stored (required)")
	fs.StringVar(&flags.path, "p", "", "Directory path (shorthand)")
	fs.StringVar(&flags.name, "name", "", "Name of the collection (required)")
	fs.StringVar(&flags.name, "n", "", "Name of the collection (shorthand)")

	// Optional collection flags
	fs.StringVar(&flags.description, "description", "", "Description of the collection")

	// Optional dataset flags with defaults
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

func create() error {
	// Step 1: Parse flags
	flags, err := parseCreateFlags()
	if err != nil {
		fmt.Fprint(os.Stderr, createUsage)
		return err
	}

	// Step 2: Build collection config
	cfg := collection.DefaultConfig()
	cfg.Description = flags.description
	cfg.DatasetOptions.VectorSize = flags.vectorSize
	cfg.DatasetOptions.MaxDataAppendBufferSize = flags.maxDataAppendBufferSize
	cfg.DatasetOptions.MaxMetaDataAppendBufferSize = flags.maxMetaDataAppendBufferSize
	cfg.DatasetOptions.MaxIndexAppendBufferSize = flags.maxIndexAppendBufferSize
	cfg.DatasetOptions.MaxVectorBufferSize = flags.maxVectorBufferSize
	cfg.DatasetOptions.MaxVectorAppendBufferSize = flags.maxVectorAppendBufferSize

	// Step 3: Handle embedder configuration
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
		// Convert to json.RawMessage
		for name, embedderCfg := range embedders {
			data, err := json.Marshal(embedderCfg)
			if err != nil {
				return fmt.Errorf("failed to marshal embedder %s: %w", name, err)
			}
			cfg.Embedders[name] = data
		}
	}

	// Then, override with inline JSON if provided (takes precedence)
	if flags.embedders != "" {
		var embedders map[string]any
		if err := json.Unmarshal([]byte(flags.embedders), &embedders); err != nil {
			return fmt.Errorf("failed to parse embedders JSON: %w", err)
		}
		// Convert to json.RawMessage
		for name, embedderCfg := range embedders {
			data, err := json.Marshal(embedderCfg)
			if err != nil {
				return fmt.Errorf("failed to marshal embedder %s: %w", name, err)
			}
			cfg.Embedders[name] = data
		}
	}

	// Step 4: Create collection
	coll, err := collection.CreateCollection(flags.path, flags.name, cfg)
	if err != nil {
		return fmt.Errorf("failed to create collection: %w", err)
	}
	defer coll.Close()

	fmt.Printf("Collection '%s' created successfully at %s\n", flags.name, flags.path)
	fmt.Printf("  Vector size: %d\n", flags.vectorSize)
	if len(cfg.Embedders) > 0 {
		fmt.Printf("  Embedders configured: %d\n", len(cfg.Embedders))
		for name := range cfg.Embedders {
			fmt.Printf("    - %s\n", name)
		}
	}

	return nil
}
