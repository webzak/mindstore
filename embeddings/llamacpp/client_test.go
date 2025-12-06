package llamacpp

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestClient_Embed(t *testing.T) {
	// Create a mock server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Verify request
		if r.Method != http.MethodPost {
			t.Errorf("Expected POST request, got %s", r.Method)
		}
		if r.URL.Path != "/v1/embeddings" {
			t.Errorf("Expected /v1/embeddings path, got %s", r.URL.Path)
		}
		if r.Header.Get("Content-Type") != "application/json" {
			t.Errorf("Expected Content-Type: application/json")
		}

		// Decode request
		var req request
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Errorf("Failed to decode request: %v", err)
		}

		// Send mock response
		resp := response{
			Object: "list",
			Data: []struct {
				Object    string    `json:"object"`
				Embedding []float32 `json:"embedding"`
				Index     int       `json:"index"`
			}{
				{
					Object:    "embedding",
					Embedding: []float32{0.1, 0.2, 0.3, 0.4},
					Index:     0,
				},
			},
			Model: "test-model",
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	// Create client with mock server URL
	client := New(server.URL)

	// Test Embed
	ctx := context.Background()
	embedding, err := client.Embed(ctx, []byte("test text"))
	if err != nil {
		t.Fatalf("Embed failed: %v", err)
	}

	// Verify embedding
	expected := []float32{0.1, 0.2, 0.3, 0.4}
	if len(embedding) != len(expected) {
		t.Fatalf("Expected embedding length %d, got %d", len(expected), len(embedding))
	}

	for i, v := range expected {
		if embedding[i] != v {
			t.Errorf("Expected embedding[%d] = %f, got %f", i, v, embedding[i])
		}
	}
}

func TestClient_Embed_EmptyText(t *testing.T) {
	client := New("http://localhost:3311")
	ctx := context.Background()

	_, err := client.Embed(ctx, []byte(""))
	if err != ErrEmptyText {
		t.Errorf("Expected ErrEmptyText, got %v", err)
	}
}

func TestClient_Embed_ServerError(t *testing.T) {
	// Create a mock server that returns an error
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("internal server error"))
	}))
	defer server.Close()

	client := New(server.URL)
	ctx := context.Background()

	_, err := client.Embed(ctx, []byte("test text"))
	if err == nil {
		t.Fatal("Expected error, got nil")
	}
}

func TestClient_EmbedBatch(t *testing.T) {
	callCount := 0

	// Create a mock server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callCount++

		// Send different embeddings for each call
		resp := response{
			Object: "list",
			Data: []struct {
				Object    string    `json:"object"`
				Embedding []float32 `json:"embedding"`
				Index     int       `json:"index"`
			}{
				{
					Object:    "embedding",
					Embedding: []float32{float32(callCount), float32(callCount * 2)},
					Index:     0,
				},
			},
			Model: "test-model",
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	client := New(server.URL)
	ctx := context.Background()

	// Test EmbedBatch
	texts := [][]byte{[]byte("text1"), []byte("text2"), []byte("text3")}
	embeddings, err := client.EmbedBatch(ctx, texts)
	if err != nil {
		t.Fatalf("EmbedBatch failed: %v", err)
	}

	// Verify we got 3 embeddings
	if len(embeddings) != 3 {
		t.Fatalf("Expected 3 embeddings, got %d", len(embeddings))
	}

	// Verify each embedding is different
	for i, emb := range embeddings {
		if len(emb) != 2 {
			t.Errorf("Expected embedding %d to have length 2, got %d", i, len(emb))
		}
		if emb[0] != float32(i+1) {
			t.Errorf("Expected embedding[%d][0] = %f, got %f", i, float32(i+1), emb[0])
		}
	}
}

func TestClient_EmbedBatch_EmptySlice(t *testing.T) {
	client := New("http://localhost:3311")
	ctx := context.Background()

	embeddings, err := client.EmbedBatch(ctx, [][]byte{})
	if err != nil {
		t.Errorf("EmbedBatch on empty slice should not error, got: %v", err)
	}
	if embeddings != nil {
		t.Errorf("Expected nil embeddings for empty input, got %v", embeddings)
	}
}
