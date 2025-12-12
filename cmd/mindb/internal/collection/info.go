package collection

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/webzak/mindstore/db/collection"
)

const infoUsage = `Show statistics and information for a collection

Usage:
  mindb collection info --path <path> --name <name>

Required Flags:
  --path string
        Directory path containing the collection
  --name string
        Name of the collection

Examples:
  # Show collection info
  mindb collection info --path /data --name articles

  # Show info with default path from config
  mindb collection info --name articles
`

type infoFlags struct {
	path string
	name string
}

func parseInfoFlags() (*infoFlags, error) {
	flags := &infoFlags{}

	fs := flag.NewFlagSet("info", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	fs.Usage = func() {
		fmt.Fprint(os.Stderr, infoUsage)
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

func infoCmd() error {
	// Parse flags
	flags, err := parseInfoFlags()
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

	// Get collection stats
	stats, err := coll.GetStats()
	if err != nil {
		return fmt.Errorf("failed to get statistics: %w", err)
	}

	// Get file sizes
	dir := filepath.Join(flags.path, flags.name)
	fileSizes, err := getFileSizes(dir, flags.name)
	if err != nil {
		return fmt.Errorf("failed to get file sizes: %w", err)
	}

	// Get collection description
	description := coll.GetDescription()

	// Display output
	printInfo(flags.name, description, embeddersConfig, stats, fileSizes)

	return nil
}

// getFileSizes returns file sizes in bytes for all collection files
func getFileSizes(dir, name string) (map[string]int64, error) {
	sizes := make(map[string]int64)
	files := []string{
		filepath.Join(dir, name+".dat"),
		filepath.Join(dir, name+".met"),
		filepath.Join(dir, name+".idx"),
		filepath.Join(dir, name+".vec"),
		filepath.Join(dir, name+".tag"),
		filepath.Join(dir, name+".grp"),
	}

	for _, file := range files {
		info, err := os.Stat(file)
		if err != nil {
			if os.IsNotExist(err) {
				sizes[filepath.Base(file)] = 0
				continue
			}
			return nil, fmt.Errorf("failed to stat %s: %w", file, err)
		}
		sizes[filepath.Base(file)] = info.Size() // Keep in bytes
	}

	return sizes, nil
}

// formatBytes formats a byte count with comma separators for thousands
func formatBytes(bytes int64) string {
	if bytes == 0 {
		return "0"
	}

	// Convert to string
	s := fmt.Sprintf("%d", bytes)

	// Add commas from right to left
	var result strings.Builder
	for i, c := range s {
		if i > 0 && (len(s)-i)%3 == 0 {
			result.WriteRune(',')
		}
		result.WriteRune(c)
	}

	return result.String()
}

// printInfo displays collection information in human-readable format
func printInfo(name, description string, embeddersConfig map[string]any, stats *collection.Stats, fileSizes map[string]int64) {
	fmt.Printf("Collection: %s\n", name)
	fmt.Println(strings.Repeat("=", len(name)+12))
	fmt.Println()

	// Description section
	if description != "" {
		fmt.Println("Description:")
		fmt.Printf("  %s\n", description)
		fmt.Println()
	}

	// Configuration section
	fmt.Println("Configuration:")
	fmt.Println("  Embedders:")
	if len(embeddersConfig) == 0 {
		fmt.Println("    (none)")
	} else {
		// Marshal to prettified JSON
		jsonBytes, err := json.MarshalIndent(embeddersConfig, "    ", "  ")
		if err != nil {
			// Fallback to simple formatting if JSON marshaling fails
			for key, value := range embeddersConfig {
				fmt.Printf("    %s: %v\n", key, value)
			}
		} else {
			// Print with proper indentation
			fmt.Printf("    %s\n", string(jsonBytes))
		}
	}
	fmt.Println()

	// Statistics section
	fmt.Println("Statistics:")
	fmt.Printf("  Total Records:          %d\n", stats.TotalRecords)
	fmt.Printf("  Records with Tags:      %d\n", stats.RecordsWithTags)
	fmt.Printf("  Records with Metadata:  %d\n", stats.RecordsWithMetadata)
	fmt.Printf("  Records with Groups:    %d\n", stats.RecordsWithGroups)
	fmt.Printf("  Records with Vectors:   %d\n", stats.RecordsWithVectors)
	fmt.Println()

	// Tags section
	if len(stats.TagCounts) > 0 {
		fmt.Println("Tags usage:")
		// Sort tags alphabetically for consistent output
		tags := make([]string, 0, len(stats.TagCounts))
		for tag := range stats.TagCounts {
			tags = append(tags, tag)
		}
		sort.Strings(tags)

		for _, tag := range tags {
			count := stats.TagCounts[tag]
			fmt.Printf("  %s: %d\n", tag, count)
		}
		fmt.Println()
	}

	// Metadata Keys section
	if len(stats.MetadataKeyCounts) > 0 {
		fmt.Println("Metadata keys usage:")
		// Sort keys alphabetically for consistent output
		keys := make([]string, 0, len(stats.MetadataKeyCounts))
		for key := range stats.MetadataKeyCounts {
			keys = append(keys, key)
		}
		sort.Strings(keys)

		for _, key := range keys {
			count := stats.MetadataKeyCounts[key]
			fmt.Printf("  %s: %d\n", key, count)
		}
		fmt.Println()
	}

	// Groups section
	fmt.Println("Groups:")
	fmt.Printf("  Total:        %d\n", stats.TotalGroups)
	fmt.Printf("  Records:      %d\n", stats.RecordsWithGroups)
	fmt.Println()

	// File sizes section
	fmt.Println("Storage:")
	fmt.Printf("  Data:       %15s b\n", formatBytes(fileSizes[name+".dat"]))
	fmt.Printf("  Metadata:   %15s b\n", formatBytes(fileSizes[name+".met"]))
	fmt.Printf("  Index:      %15s b\n", formatBytes(fileSizes[name+".idx"]))
	fmt.Printf("  Vectors:    %15s b\n", formatBytes(fileSizes[name+".vec"]))
	fmt.Printf("  Tags:       %15s b\n", formatBytes(fileSizes[name+".tag"]))
	fmt.Printf("  Groups:     %15s b\n", formatBytes(fileSizes[name+".grp"]))

	// Calculate total
	total := int64(0)
	for _, size := range fileSizes {
		total += size
	}
	fmt.Println()
	fmt.Printf("  Total:           %15s bytes\n", formatBytes(total))
}
