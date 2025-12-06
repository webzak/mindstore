package embeddings

import (
	"context"
)

// Embedder defines the interface for generating embeddings
type Embedder interface {
	// Embed converts data to a vector representation
	Embed(ctx context.Context, data []byte) ([]float32, error)

	// EmbedBatch generates embedding vectors for multiple data items
	EmbedBatch(ctx context.Context, chunks [][]byte) ([][]float32, error)
}
