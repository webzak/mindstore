package db

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/webzak/mindstore/internal/engine/data"
	"github.com/webzak/mindstore/internal/engine/groups"
	"github.com/webzak/mindstore/internal/engine/index"
	"github.com/webzak/mindstore/internal/engine/meta"
	"github.com/webzak/mindstore/internal/engine/tags"
	"github.com/webzak/mindstore/internal/engine/vectors"
)

type DataType uint8

const (
	Text  = DataType(data.Text)
	Image = DataType(data.Image)
	Video = DataType(data.Video)
	Audio = DataType(data.Audio)
)

var (
	ErrCollectionExists   = errors.New("collection already exists")
	ErrCollectionNotFound = errors.New("collection not found")
	ErrInvalidID          = errors.New("invalid record ID")
	ErrTagsNotEnabled     = errors.New("tags not enabled for this collection")
)

// Collection represents a database collection
type Collection struct {
	path   string
	name   string
	config Config

	data  *data.Data
	index *index.Index

	vectors *vectors.Vectors
	tags    *tags.Tags
	meta    *meta.Meta
	groups  *groups.Groups
}

// Group represents group membership information
type Group struct {
	// ID is the group identifier
	ID int
	// Place is the position within the group
	Place int
}

// Item represents a complete record with all its associated data
type Item struct {
	// ID is the record identifier
	ID int
	// Data is the main record data
	Data []byte
	// Type is the type of data
	Type DataType
	// Vector is the vector data
	Vector []float32
	// Tags is the list of tags
	Tags []string
	// Meta is the metadata
	Meta map[string]any
	// Group contains group membership information
	Group *Group
}

// ReadOptions specifies which components of an Item to read.
// If ReadOptions is nil, only the core Data field will be populated.
type ReadOptions struct {
	// Vector indicates whether to read vector data
	Vector bool
	// Tags indicates whether to read tags
	Tags bool
	// Meta indicates whether to read metadata
	Meta bool
	// Group indicates whether to read group information
	Group bool
}

// AllReadOptions returns ReadOptions with all fields set to true
func AllReadOptions() *ReadOptions {
	return &ReadOptions{
		Vector: true,
		Tags:   true,
		Meta:   true,
		Group:  true,
	}
}

// DataOnlyReadOptions returns ReadOptions that only reads the core data.
// This is equivalent to passing nil as ReadOptions.
func DataOnlyReadOptions() *ReadOptions {
	return &ReadOptions{}
}

// NewCollection creates a new collection with the specified configuration
func NewCollection(path, name string, config Config) (*Collection, error) {
	// Create collection directory
	collectionDir := filepath.Join(path, name)

	// Check if collection already exists, else create directory
	if _, err := os.Stat(collectionDir); err == nil {
		if err = os.MkdirAll(collectionDir, 0755); err != nil {
			return nil, fmt.Errorf("failed to create collection directory: %w", err)
		}
	}

	c := &Collection{
		path:   path,
		name:   name,
		config: config,
	}

	// init data
	data, err := data.New(filepath.Join(collectionDir, name+".dat"))
	if err != nil {
		return nil, fmt.Errorf("failed init data storage: %w", err)
	}
	c.data = data

	// init index
	indexOptions := index.DefaultIndexOptions()
	if config.MaxIndexAppendBufferSize > 0 {
		indexOptions.MaxAppendBufferSize = config.MaxIndexAppendBufferSize
	}

	index, err := index.New(filepath.Join(collectionDir, name+".idx"), &indexOptions)
	if err != nil {
		return nil, fmt.Errorf("failed to create index: %w", err)
	}
	c.index = index

	// init vectors
	vectorsOptions := vectors.DefaultVectorsOptions()
	if config.VectorSize > 0 {
		vectorsOptions.VectorSize = config.VectorSize
	}
	if config.MaxVectorBufferSize > 0 {
		vectorsOptions.MaxBufferSize = config.MaxVectorBufferSize
	}
	if config.MaxVectorAppendBufferSize > 0 {
		vectorsOptions.MaxAppendBufferSize = config.MaxIndexAppendBufferSize
	}

	vectors, err := vectors.New(filepath.Join(collectionDir, name+".vec"), &vectorsOptions)
	if err != nil {
		return nil, fmt.Errorf("failed to init vectors: %w", err)
	}
	c.vectors = vectors

	// init tags
	tags, err := tags.New(filepath.Join(collectionDir, name+".tag"))
	if err != nil {
		return nil, fmt.Errorf("failed to init tags: %w", err)
	}
	c.tags = tags

	// init meta
	meta, err := meta.New(filepath.Join(collectionDir, name+".meta"))
	if err != nil {
		return nil, fmt.Errorf("failed to init meta: %w", err)
	}
	c.meta = meta

	// init groups
	groups, err := groups.New(filepath.Join(collectionDir, name+".grp"))
	if err != nil {
		return nil, fmt.Errorf("failed to init groups: %w", err)
	}
	c.groups = groups

	// Save configuration
	if err := saveConfig(filepath.Join(collectionDir, "config.json"), config); err != nil {
		return nil, fmt.Errorf("failed to save config: %w", err)
	}
	return c, nil
}

// IsPersisted returns true if all data is saved to storage
func (c *Collection) IsPersisted() bool {
	return c.index.IsPersisted() && c.vectors.IsPersisted() && c.tags.IsPersisted() && c.meta.IsPersisted() && c.groups.IsPersisted()
}

// Flush persists all in-memory changes to disk
func (c *Collection) Flush() error {
	// Flush index
	if err := c.index.Flush(); err != nil {
		return fmt.Errorf("failed to flush index: %w", err)
	}

	if err := c.vectors.Flush(); err != nil {
		return fmt.Errorf("failed to flush vectors: %w", err)
	}

	if err := c.tags.Flush(); err != nil {
		return fmt.Errorf("failed to flush tags: %w", err)
	}

	if err := c.meta.Flush(); err != nil {
		return fmt.Errorf("failed to flush meta: %w", err)
	}

	if err := c.groups.Flush(); err != nil {
		return fmt.Errorf("failed to flush groups: %w", err)
	}
	return nil
}

// Append adds a new record to the collection
// func (c *Collection) Append(item *Item) (int, error) {

// 	// Append data to storage
// 	offset, size, err := c.data.Append(item.Data)
// 	if err != nil {
// 		return 0, fmt.Errorf("failed to append data: %w", err)
// 	}

// 	// Add index entry
// 	row := index.Row{
// 		Offset: offset,
// 		Size:   size,
// 		Type:   uint8(item.Type),
// 	}
// 	if err := c.index.Append(row); err != nil {
// 		return 0, fmt.Errorf("failed to add index entry: %w", err)
// 	}

// 	// Get the new record ID (current count - 1)
// 	recordID := c.index.Count() - 1

// 	if len(item.Vector) > 0 {

// 		// Vectors are enabled (VectorLength > 0)
// 		if c.embedder != nil {
// 			// Case 3: Embedder is available
// 			if len(item.Vector) > 0 {
// 				return 0, fmt.Errorf("cannot provide manual vector when embedder is configured")
// 			}
// 			// Generate embedding
// 			vec, err := c.embedder.Embed(context.Background(), item.Data, types.DataType(item.Type))
// 			if err != nil {
// 				return 0, fmt.Errorf("failed to generate embedding: %w", err)
// 			}
// 			item.Vector = vec
// 		} else {
// 			// Case 2: No embedder
// 			if len(item.Vector) == 0 {
// 				return 0, fmt.Errorf("vector is required but not provided and no embedder configured")
// 			}
// 			if len(item.Vector) != c.config.VectorSize {
// 				return 0, fmt.Errorf("vector length mismatch: expected %d, got %d", c.config.VectorSize, len(item.Vector))
// 			}
// 		}

// 		// Append vector
// 		if _, err := c.vectors.Append(item.Vector); err != nil {
// 			return 0, fmt.Errorf("failed to append vector: %w", err)
// 		}
// 	}

// 	// Add tags if tags are enabled
// 	if c.tags != nil && len(item.Tags) > 0 {
// 		for _, tag := range item.Tags {
// 			c.tags.Add(recordID, tag)
// 		}
// 	}

// 	// Set metadata if meta is enabled
// 	if c.meta != nil && len(item.Meta) > 0 {
// 		c.meta.Set(recordID, item.Meta)
// 	}

// 	// Assign to group if groups are enabled
// 	if c.groups != nil && item.Group != nil && item.Group.ID > 0 {
// 		if err := c.groups.Assign(item.Group.ID, recordID, item.Group.Place); err != nil {
// 			return 0, fmt.Errorf("failed to assign to group: %w", err)
// 		}
// 	}

// 	// Set the item ID to the generated record ID
// 	item.ID = recordID

// 	return recordID, nil
// }

// // Read retrieves a record by ID and returns it as an Item.
// // If opts is nil, only the core Data field will be populated.
// // Otherwise, optional components are loaded based on the opts flags.
// func (c *Collection) Read(id int, opts *ReadOptions) (*Item, error) {
// 	// Get index entry
// 	row, err := c.index.Get(id)
// 	if err != nil {
// 		return nil, fmt.Errorf("failed to get index entry: %w", err)
// 	}

// 	// Read data (always read)
// 	recordData, err := c.data.Read(row.Offset, int(row.Size))
// 	if err != nil {
// 		return nil, fmt.Errorf("failed to read data: %w", err)
// 	}

// 	// Create the item with core data
// 	item := &Item{
// 		ID:   id,
// 		Data: recordData,
// 		Type: row.DataType,
// 	}

// 	// If opts is nil, return only the data
// 	if opts == nil {
// 		return item, nil
// 	}

// 	// Read vector if requested and enabled
// 	if opts.Vector && c.vectors != nil {
// 		vector, err := c.vectors.Get(id)
// 		if err != nil {
// 			return nil, fmt.Errorf("failed to read vector: %w", err)
// 		}
// 		item.Vector = vector
// 	}

// 	// Read tags if requested and enabled
// 	if opts.Tags && c.tags != nil {
// 		item.Tags = c.tags.GetTags(id)
// 		if item.Tags == nil {
// 			item.Tags = []string{}
// 		}
// 	}

// 	// Read metadata if requested and enabled
// 	if opts.Meta && c.meta != nil {
// 		item.Meta = c.meta.Get(id)
// 	}

// 	// Read group information if requested and enabled
// 	if opts.Group && c.groups != nil {
// 		groupID, ok := c.groups.GetGroup(id)
// 		if ok {
// 			// Find the place/position within the group
// 			members := c.groups.GetMembers(groupID)
// 			place := -1
// 			for i, memberID := range members {
// 				if memberID == id {
// 					place = i
// 					break
// 				}
// 			}
// 			item.Group = &Group{
// 				ID:    groupID,
// 				Place: place,
// 			}
// 		}
// 	}

// 	return item, nil
// }

// // Count returns the number of records in the collection
// func (c *Collection) Count() int {
// 	return c.index.Count()
// }

// // Config returns the collection configuration
// func (c *Collection) Config() Config {
// 	return c.config
// }

// // UpdateConfig updates the collection configuration and persists it to disk
// func (c *Collection) UpdateConfig(config Config) error {
// 	c.config = config
// 	configPath := filepath.Join(c.path, c.name, "config.json")
// 	if err := saveConfig(configPath, config); err != nil {
// 		return fmt.Errorf("failed to save config: %w", err)
// 	}
// 	return nil
// }

// // ClearEmbeddings removes the vector storage file and resets the vectors instance
// // func (c *Collection) ClearEmbeddings() error {
// // 	return nil
// // }

// // FileStat represents file statistics
// type FileStat struct {
// 	Path         string    `json:"path"`
// 	Size         int64     `json:"size"`
// 	LastModified time.Time `json:"last_modified"`
// }

// // ComponentStats represents statistics for a collection component
// type ComponentStats struct {
// 	File        FileStat `json:"file"`
// 	RecordCount int      `json:"record_count"`
// }

// // CollectionStats represents the complete statistics for a collection
// type CollectionStats struct {
// 	Records        int                    `json:"records"`
// 	Config         FileStat               `json:"config_file"`
// 	Data           ComponentStats         `json:"data"`
// 	Index          ComponentStats         `json:"index"`
// 	Vectors        *ComponentStats        `json:"vectors,omitempty"`
// 	Tags           *ComponentStats        `json:"tags,omitempty"`
// 	Meta           *ComponentStats        `json:"meta,omitempty"`
// 	Groups         *ComponentStats        `json:"groups,omitempty"`
// 	MaxSize        int64                  `json:"max_size"`
// 	MinSize        int64                  `json:"min_size"`
// 	AvgSize        int64                  `json:"avg_size"`
// 	DataTypeCounts map[types.DataType]int `json:"data_type_counts"`
// }

// // Stats returns statistics about the collection
// func (c *Collection) Stats() (CollectionStats, error) {
// 	stats := CollectionStats{
// 		Records: c.Count(),
// 	}

// 	// Helper to get file stats
// 	getFileStat := func(path string) (FileStat, error) {
// 		info, err := os.Stat(path)
// 		if err != nil {
// 			return FileStat{}, err
// 		}
// 		return FileStat{
// 			Path:         path,
// 			Size:         info.Size(),
// 			LastModified: info.ModTime(),
// 		}, nil
// 	}

// 	// Config file
// 	configPath := filepath.Join(c.path, c.name, "config.json")
// 	if fs, err := getFileStat(configPath); err == nil {
// 		stats.Config = fs
// 	}

// 	// Data file
// 	dataPath := filepath.Join(c.path, c.name, c.name+".dat")
// 	if fs, err := getFileStat(dataPath); err == nil {
// 		stats.Data = ComponentStats{
// 			File:        fs,
// 			RecordCount: c.Count(), // Data always has all records
// 		}
// 	}

// 	// Index file
// 	indexPath := filepath.Join(c.path, c.name, c.name+".idx")
// 	if fs, err := getFileStat(indexPath); err == nil {
// 		stats.Index = ComponentStats{
// 			File:        fs,
// 			RecordCount: c.Count(), // Index always has all records
// 		}
// 	}

// 	// Vectors
// 	if c.vectors != nil {
// 		vectorsPath := filepath.Join(c.path, c.name, c.name+".vec")
// 		if fs, err := getFileStat(vectorsPath); err == nil {
// 			stats.Vectors = &ComponentStats{
// 				File:        fs,
// 				RecordCount: c.vectors.Count(),
// 			}
// 		}
// 	}

// 	// Tags
// 	if c.tags != nil {
// 		tagsPath := filepath.Join(c.path, c.name, c.name+".tag")
// 		if fs, err := getFileStat(tagsPath); err == nil {
// 			stats.Tags = &ComponentStats{
// 				File:        fs,
// 				RecordCount: c.tags.Count(),
// 			}
// 		}
// 	}

// 	// Meta
// 	if c.meta != nil {
// 		metaPath := filepath.Join(c.path, c.name, c.name+".meta")
// 		if fs, err := getFileStat(metaPath); err == nil {
// 			stats.Meta = &ComponentStats{
// 				File:        fs,
// 				RecordCount: c.meta.Count(),
// 			}
// 		}
// 	}

// 	// Groups
// 	if c.groups != nil {
// 		groupsPath := filepath.Join(c.path, c.name, c.name+".grp")
// 		if fs, err := getFileStat(groupsPath); err == nil {
// 			stats.Groups = &ComponentStats{
// 				File:        fs,
// 				RecordCount: c.groups.Count(),
// 			}
// 		}
// 	}

// 	// Collect data statistics in a single pass
// 	stats.DataTypeCounts = make(map[types.DataType]int)
// 	count := c.index.Count()
// 	if count > 0 {
// 		var totalSize int64
// 		stats.MinSize = -1 // Will be set on first record

// 		// Iterate over all records in a single pass
// 		for i := 0; i < count; i++ {
// 			row, err := c.index.Get(i)
// 			if err != nil {
// 				return stats, fmt.Errorf("failed to get index entry %d: %w", i, err)
// 			}

// 			// Track data type usage
// 			dataType := types.DataType(row.DataType)
// 			stats.DataTypeCounts[dataType]++

// 			// Track size statistics
// 			size := row.Size
// 			totalSize += size

// 			if stats.MinSize == -1 || size < stats.MinSize {
// 				stats.MinSize = size
// 			}
// 			if size > stats.MaxSize {
// 				stats.MaxSize = size
// 			}
// 		}

// 		// Calculate average
// 		stats.AvgSize = totalSize / int64(count)

// 		// Reset MinSize if it's still -1 (shouldn't happen, but for safety)
// 		if stats.MinSize == -1 {
// 			stats.MinSize = 0
// 		}
// 	}

// 	return stats, nil
// }

// // embeddingsValid checks if the embeddings are consistent with the index
// func (c *Collection) embeddingsValid() bool {
// 	if c.config.VectorSize == 0 {
// 		return true
// 	}
// 	// If VectorLength > 0, vectors must be initialized
// 	if c.vectors == nil {
// 		return false
// 	}
// 	return c.index.Count() == c.vectors.Count()
// }

// // LoadEmbedderForCollection loads the embedder for a collection without opening the collection itself.
// // It reads the collection's config.json to get the embedder name, then loads the embedder from embedders.json.
// func LoadEmbedderForCollection(path, name string) (embeddings.Embedder, error) {
// 	// Load collection config
// 	configPath := filepath.Join(path, name, "config.json")
// 	config, err := loadConfig(configPath)
// 	if err != nil {
// 		return nil, fmt.Errorf("failed to load collection config: %w", err)
// 	}

// 	// Check if embedder is configured
// 	if config.Embedder == "" {
// 		return nil, fmt.Errorf("collection does not have an embedder configured")
// 	}

// 	// Load embedders.json
// 	embeddersConfigPath := filepath.Join(path, "embedders.json")
// 	data, err := os.ReadFile(embeddersConfigPath)
// 	if err != nil {
// 		if os.IsNotExist(err) {
// 			return nil, fmt.Errorf("embedder '%s' specified but %s not found", config.Embedder, embeddersConfigPath)
// 		}
// 		return nil, fmt.Errorf("failed to read embedders.json: %w", err)
// 	}

// 	// Parse embedders.json
// 	var allEmbedders map[string]map[string]any
// 	if err := json.Unmarshal(data, &allEmbedders); err != nil {
// 		return nil, fmt.Errorf("failed to parse embedders.json: %w", err)
// 	}

// 	// Validate embedder exists
// 	rawConfig, ok := allEmbedders[config.Embedder]
// 	if !ok {
// 		return nil, fmt.Errorf("embedder '%s' not found in %s", config.Embedder, embeddersConfigPath)
// 	}

// 	// Convert config for embedder factory (map[string]string)
// 	embedderConfig := make(map[string]string)
// 	for k, v := range rawConfig {
// 		if k == "vector_size" {
// 			continue
// 		}
// 		embedderConfig[k] = fmt.Sprintf("%v", v)
// 	}

// 	// Create embedder
// 	return embeddings.NewEmbedder(config.Embedder, embedderConfig)
// }
