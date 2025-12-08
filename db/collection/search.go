package collection

import (
	"fmt"
	"sort"

	"github.com/webzak/mindstore/db/dataset"
	"github.com/webzak/mindstore/embeddings"
)

// VectorSearchResult represents a single search result with similarity score
type VectorSearchResult struct {
	Distance float32       // Cosine similarity score (-1 to 1, higher is more similar)
	Item     *dataset.Item // Complete dataset item
}

// VectorSearchOptions configures vector search behavior
type VectorSearchOptions struct {
	Limit     int                  // Maximum results to return (0 or negative = all)
	SortOrder embeddings.SortOrder // Sort by distance (SortAsc or SortDesc)
}

// VectorSearch performs similarity search against all vectors in the collection.
// Returns results sorted by cosine similarity according to opt.SortOrder.
// If opt.Limit > 0, returns at most that many results.
// Returns empty slice (not error) if collection has no vectors.
func (c *Collection) VectorSearch(vector []float32, opt VectorSearchOptions) ([]*VectorSearchResult, error) {

	type itemDistance struct {
		id       int
		distance float32
	}

	// Validate query vector size
	expectedSize := c.cfg.DatasetOptions.VectorSize
	if len(vector) != expectedSize {
		return nil, fmt.Errorf("vector size mismatch: expected %d, got %d", expectedSize, len(vector))
	}

	// Build intermediate results with distances
	var distances []itemDistance

	for id, vec := range c.dataset.VectorsIterator() {
		// Calculate cosine similarity
		distance := embeddings.CosineSim(vector, vec)

		distances = append(distances, itemDistance{
			id:       id,
			distance: distance,
		})
	}

	// Handle empty results (no vectors in collection)
	if len(distances) == 0 {
		return []*VectorSearchResult{}, nil
	}

	// Sort by distance according to SortOrder
	if opt.SortOrder == embeddings.SortAsc {
		sort.Slice(distances, func(i, j int) bool {
			return distances[i].distance < distances[j].distance
		})
	} else {
		// SortDesc - higher similarity first (default for cosine similarity)
		sort.Slice(distances, func(i, j int) bool {
			return distances[i].distance > distances[j].distance
		})
	}

	// Apply limit
	resultCount := len(distances)
	if opt.Limit > 0 && resultCount > opt.Limit {
		resultCount = opt.Limit
	}

	// Load full items for top results (excluding vectors - we already used them for search)
	results := make([]*VectorSearchResult, resultCount)
	readOpts := dataset.ReadData | dataset.ReadMeta | dataset.ReadTags | dataset.ReadGroup
	for i := 0; i < resultCount; i++ {
		item, err := c.dataset.Read(distances[i].id, readOpts)
		if err != nil {
			return nil, fmt.Errorf("failed to read item %d: %w", distances[i].id, err)
		}

		results[i] = &VectorSearchResult{
			Distance: distances[i].distance,
			Item:     item,
		}
	}

	return results, nil
}
