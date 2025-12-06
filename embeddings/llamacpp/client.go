package llamacpp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
)

var (
	ErrEmptyText       = errors.New("empty text provided")
	ErrServerResponse  = errors.New("server returned error response")
	ErrInvalidResponse = errors.New("invalid response format")
	ErrNoData          = errors.New("no embedding data in response")
)

// Client implements the Embedder interface for llama-cpp server
type Client struct {
	baseURL    string
	httpClient *http.Client
}

// New creates a new llama-cpp server client
// baseURL should be the llama-cpp server address, e.g., "http://localhost:3311"
func New(baseURL string) *Client {
	return &Client{
		baseURL:    baseURL,
		httpClient: &http.Client{},
	}
}

// request represents the request body for llama-cpp embeddings API
type request struct {
	Input string `json:"input"`
}

// response represents the response from llama-cpp embeddings API
type response struct {
	Object string `json:"object"`
	Data   []struct {
		Object    string    `json:"object"`
		Embedding []float32 `json:"embedding"`
		Index     int       `json:"index"`
	} `json:"data"`
	Model string `json:"model"`
	Usage struct {
		PromptTokens int `json:"prompt_tokens"`
		TotalTokens  int `json:"total_tokens"`
	} `json:"usage"`
}

// Embed generates an embedding vector for the given data
func (c *Client) Embed(ctx context.Context, chunk []byte) ([]float32, error) {
	text := string(chunk)
	if text == "" {
		return nil, ErrEmptyText
	}

	// Prepare request
	reqBody := request{
		Input: text,
	}

	jsonData, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %w", err)
	}

	// Create HTTP request
	url := c.baseURL + "/v1/embeddings"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewBuffer(jsonData))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")

	// Send request
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to send request: %w", err)
	}
	defer resp.Body.Close()

	// Read response body
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	// Check status code
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("%w: status %d, body: %s", ErrServerResponse, resp.StatusCode, string(body))
	}

	// Parse response
	var embedResp response
	if err := json.Unmarshal(body, &embedResp); err != nil {
		return nil, fmt.Errorf("%w: %s", ErrInvalidResponse, err.Error())
	}

	// Extract embedding
	if len(embedResp.Data) == 0 {
		return nil, ErrNoData
	}

	return embedResp.Data[0].Embedding, nil
}

// EmbedBatch generates embedding vectors for multiple data items
func (c *Client) EmbedBatch(ctx context.Context, chunks [][]byte) ([][]float32, error) {
	if len(chunks) == 0 {
		return nil, nil
	}
	embeddings := make([][]float32, len(chunks))

	// Process each item individually
	// Note: llama-cpp server typically processes one text at a time
	for i, d := range chunks {
		embedding, err := c.Embed(ctx, d)
		if err != nil {
			return nil, fmt.Errorf("failed to embed item at index %d: %w", i, err)
		}
		embeddings[i] = embedding
	}

	return embeddings, nil
}
