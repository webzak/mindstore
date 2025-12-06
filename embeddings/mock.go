package embeddings

import (
	"context"
	"math/rand"
)

// MockEmbedder is a mock embedder that generates random vectors for testing
type MockEmbedder struct {
	vectorLength int
}

// NewMock creates a new mock embedder with the specified vector length
func NewMock(vectorLength int) *MockEmbedder {
	return &MockEmbedder{
		vectorLength: vectorLength,
	}
}

// Embed generates a random vector with values between -1 and 1
func (m *MockEmbedder) Embed(ctx context.Context, data []byte) ([]float32, error) {
	vec := make([]float32, m.vectorLength)
	for i := 0; i < m.vectorLength; i++ {
		// Generate random float between -1 and 1
		vec[i] = rand.Float32()*2 - 1
	}
	return vec, nil
}

// EmbedBatch generates random vectors for multiple data items
func (m *MockEmbedder) EmbedBatch(ctx context.Context, chunks [][]byte) ([][]float32, error) {
	results := make([][]float32, len(chunks))
	for i := range chunks {
		vec, err := m.Embed(ctx, chunks[i])
		if err != nil {
			return nil, err
		}
		results[i] = vec
	}
	return results, nil
}
