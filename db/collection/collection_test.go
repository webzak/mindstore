package collection

import (
	"os"
	"path/filepath"
	"testing"
)

func TestCreateCollection(t *testing.T) {
	tempDir := t.TempDir()

	// Create options with embedders
	opts := DefaultOptions()
	opts.DatasetOptions.VectorSize = 384
	opts.Embedders["llamacpp-text"] = map[string]any{
		"base_url": "http://localhost:3311",
	}

	// Create collection
	coll, err := CreateCollection(tempDir, "test_collection", opts)
	if err != nil {
		t.Fatalf("failed to create collection: %v", err)
	}
	defer coll.Close()

	// Verify collection directory was created
	collDir := filepath.Join(tempDir, "test_collection")
	if _, err := os.Stat(collDir); os.IsNotExist(err) {
		t.Error("collection directory was not created")
	}

	// Verify config file was created
	configPath := filepath.Join(collDir, "test_collection.json")
	if _, err := os.Stat(configPath); os.IsNotExist(err) {
		t.Error("config file was not created")
	}

	// Verify collection fields
	if coll.path != tempDir {
		t.Errorf("expected path %s, got %s", tempDir, coll.path)
	}
	if coll.name != "test_collection" {
		t.Errorf("expected name test_collection, got %s", coll.name)
	}
	if coll.dataset == nil {
		t.Error("expected dataset to be initialized")
	}
	if coll.embedders == nil {
		t.Error("expected embedders map to be initialized")
	}
}

func TestOpenCollection(t *testing.T) {
	tempDir := t.TempDir()

	// Create a collection first
	opts := DefaultOptions()
	opts.DatasetOptions.VectorSize = 512
	opts.Embedders["openai-image"] = map[string]any{
		"api_key": "sk-test",
		"model":   "clip",
	}

	coll1, err := CreateCollection(tempDir, "test_open", opts)
	if err != nil {
		t.Fatalf("failed to create collection: %v", err)
	}
	coll1.Close()

	// Open the existing collection
	coll2, err := OpenCollection(tempDir, "test_open")
	if err != nil {
		t.Fatalf("failed to open collection: %v", err)
	}
	defer coll2.Close()

	// Verify config was loaded correctly
	if coll2.cfg.DatasetOptions.VectorSize != 512 {
		t.Errorf("expected VectorSize 512, got %d", coll2.cfg.DatasetOptions.VectorSize)
	}

	// Verify embedder config was loaded
	embedders, err := coll2.GetEmbeddersConfig()
	if err != nil {
		t.Fatalf("failed to get embedders config: %v", err)
	}

	if len(embedders) != 1 {
		t.Errorf("expected 1 embedder, got %d", len(embedders))
	}

	openaiCfg, ok := embedders["openai-image"].(map[string]any)
	if !ok {
		t.Error("openai-image config is not map[string]any")
	} else {
		if openaiCfg["api_key"] != "sk-test" {
			t.Errorf("api_key mismatch: expected sk-test, got %v", openaiCfg["api_key"])
		}
		if openaiCfg["model"] != "clip" {
			t.Errorf("model mismatch: expected clip, got %v", openaiCfg["model"])
		}
	}
}

func TestCreateOpenRoundTrip(t *testing.T) {
	tempDir := t.TempDir()

	// Create options with multiple embedders and custom options
	originalOpts := DefaultOptions()
	originalOpts.DatasetOptions.VectorSize = 768
	originalOpts.DatasetOptions.MaxDataAppendBufferSize = 512 * 1024

	originalOpts.Embedders["llamacpp-text"] = map[string]any{
		"base_url": "http://localhost:3311",
		"model":    "text",
	}
	originalOpts.Embedders["openai-image"] = map[string]any{
		"api_key": "sk-test",
		"model":   "clip",
	}
	originalOpts.Embedders["custom"] = map[string]any{
		"endpoint": "https://example.com",
	}

	// Create collection
	coll1, err := CreateCollection(tempDir, "roundtrip", originalOpts)
	if err != nil {
		t.Fatalf("failed to create collection: %v", err)
	}
	coll1.Close()

	// Open collection
	coll2, err := OpenCollection(tempDir, "roundtrip")
	if err != nil {
		t.Fatalf("failed to open collection: %v", err)
	}
	defer coll2.Close()

	// Verify dataset options match
	if coll2.cfg.DatasetOptions.VectorSize != originalOpts.DatasetOptions.VectorSize {
		t.Errorf("VectorSize mismatch: expected %d, got %d",
			originalOpts.DatasetOptions.VectorSize, coll2.cfg.DatasetOptions.VectorSize)
	}
	if coll2.cfg.DatasetOptions.MaxDataAppendBufferSize != originalOpts.DatasetOptions.MaxDataAppendBufferSize {
		t.Errorf("MaxDataAppendBufferSize mismatch: expected %d, got %d",
			originalOpts.DatasetOptions.MaxDataAppendBufferSize, coll2.cfg.DatasetOptions.MaxDataAppendBufferSize)
	}

	// Verify all embedders match
	embedders, err := coll2.GetEmbeddersConfig()
	if err != nil {
		t.Fatalf("failed to get embedders config: %v", err)
	}

	if len(embedders) != len(originalOpts.Embedders) {
		t.Errorf("embedders count mismatch: expected %d, got %d",
			len(originalOpts.Embedders), len(embedders))
	}

	// Verify each embedder
	for name := range originalOpts.Embedders {
		if _, ok := embedders[name]; !ok {
			t.Errorf("embedder %s not found in opened collection", name)
		}
	}
}

func TestGetEmbeddersConfig(t *testing.T) {
	tempDir := t.TempDir()

	opts := DefaultOptions()
	opts.Embedders["llamacpp-text"] = map[string]any{
		"base_url": "http://localhost:3311",
		"model":    "text",
	}
	opts.Embedders["openai-image"] = map[string]any{
		"api_key": "sk-test",
	}

	coll, err := CreateCollection(tempDir, "test_get", opts)
	if err != nil {
		t.Fatalf("failed to create collection: %v", err)
	}
	defer coll.Close()

	// Get embedders config
	embedders, err := coll.GetEmbeddersConfig()
	if err != nil {
		t.Fatalf("failed to get embedders config: %v", err)
	}

	if len(embedders) != 2 {
		t.Errorf("expected 2 embedders, got %d", len(embedders))
	}

	// Verify llamacpp-text
	llamaCfg, ok := embedders["llamacpp-text"].(map[string]any)
	if !ok {
		t.Error("llamacpp-text config is not map[string]any")
	} else {
		if llamaCfg["base_url"] != "http://localhost:3311" {
			t.Errorf("base_url mismatch: expected http://localhost:3311, got %v", llamaCfg["base_url"])
		}
		if llamaCfg["model"] != "text" {
			t.Errorf("model mismatch: expected text, got %v", llamaCfg["model"])
		}
	}

	// Verify openai-image
	openaiCfg, ok := embedders["openai-image"].(map[string]any)
	if !ok {
		t.Error("openai-image config is not map[string]any")
	} else {
		if openaiCfg["api_key"] != "sk-test" {
			t.Errorf("api_key mismatch: expected sk-test, got %v", openaiCfg["api_key"])
		}
	}
}

func TestGetEmbeddersConfigEmpty(t *testing.T) {
	tempDir := t.TempDir()

	// Create collection with no embedders
	opts := DefaultOptions()
	coll, err := CreateCollection(tempDir, "test_empty", opts)
	if err != nil {
		t.Fatalf("failed to create collection: %v", err)
	}
	defer coll.Close()

	// Get embedders config
	embedders, err := coll.GetEmbeddersConfig()
	if err != nil {
		t.Fatalf("failed to get embedders config: %v", err)
	}

	if len(embedders) != 0 {
		t.Errorf("expected empty embedders map, got %d entries", len(embedders))
	}
}

func TestOpenNonExistentCollection(t *testing.T) {
	tempDir := t.TempDir()

	// Try to open a collection that doesn't exist
	_, err := OpenCollection(tempDir, "non_existent")
	if err == nil {
		t.Error("expected error when opening non-existent collection, got nil")
	}
}

func TestCreateCollectionInvalidPath(t *testing.T) {
	// Try to create collection in a path that can't be created (e.g., inside a file)
	tempDir := t.TempDir()

	// Create a file
	filePath := filepath.Join(tempDir, "not_a_dir")
	if err := os.WriteFile(filePath, []byte("test"), 0644); err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}

	// Try to create collection "inside" the file
	opts := DefaultOptions()
	_, err := CreateCollection(filePath, "test", opts)
	if err == nil {
		t.Error("expected error when creating collection in invalid path, got nil")
	}
}
