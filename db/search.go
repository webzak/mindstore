package db

// import (
// 	"fmt"

// 	"github.com/webzak/mindstore/internal/engine/math"
// )

// // SortOrder
// type SortOrder int

// const (
// 	SortAsc  = SortOrder(math.SortAsc)
// 	SortDesc = SortOrder(math.SortDesc)
// )

// // VectorSearchMethod represents the search/similarity algorithm to use
// type VectorSearchMethod int

// const (
// 	// CosineSimMethod uses cosine similarity for search
// 	CosineSimMethod = VectorSearchMethod(math.CosineSimMethod)
// )

// // VectorSearchResult
// type VectorSearchResult struct {
// 	Distance float32 // vector distance value
// 	Item     *Item   // item data
// }

// // VectorSearch
// func (c *Collection) VectorSearch(vector []float32, searchType VectorSearchMethod, sortOrder SortOrder, limit int, opts *ReadOptions) ([]VectorSearchResult, error) {
// 	// Check if vectors are enabled
// 	if c.config.VectorSize != len(vector) {
// 		return nil, fmt.Errorf("search vector length mismatch, expected: %d, provided: %d", c.config.VectorSize, len(vector))
// 	}

// 	distances, err := c.vectors.Search(vector, math.VectorSearchMethod(searchType), math.SortOrder(sortOrder), limit)
// 	if err != nil {
// 		return nil, fmt.Errorf("failed to search vectors: %w", err)
// 	}

// 	// Convert distances to VectorSearchResult and read items
// 	results := make([]VectorSearchResult, len(distances))
// 	for i, dist := range distances {
// 		// Read the item for this ID
// 		item, err := c.Read(dist.ID, opts)
// 		if err != nil {
// 			return nil, fmt.Errorf("failed to read item %d: %w", dist.ID, err)
// 		}

// 		results[i] = VectorSearchResult{
// 			Distance: dist.Value,
// 			Item:     item,
// 		}
// 	}

// 	return results, nil
// }
