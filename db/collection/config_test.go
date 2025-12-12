package collection

import (
	"encoding/json"
	"testing"

	"github.com/webzak/mindstore/db/dataset"
)

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()

	// Verify dataset options are set to defaults
	defaultDatasetOpts := dataset.DefaultOptions()
	if cfg.DatasetOptions.VectorSize != defaultDatasetOpts.VectorSize {
		t.Errorf("expected VectorSize %d, got %d", defaultDatasetOpts.VectorSize, cfg.DatasetOptions.VectorSize)
	}
	if cfg.DatasetOptions.MaxDataAppendBufferSize != defaultDatasetOpts.MaxDataAppendBufferSize {
		t.Errorf("expected MaxDataAppendBufferSize %d, got %d", defaultDatasetOpts.MaxDataAppendBufferSize, cfg.DatasetOptions.MaxDataAppendBufferSize)
	}

	// Verify embedders map is initialized but empty
	if cfg.Embedders == nil {
		t.Error("expected Embedders map to be initialized")
	}
	if len(cfg.Embedders) != 0 {
		t.Errorf("expected Embedders map to be empty, got %d entries", len(cfg.Embedders))
	}

	// Verify description is empty by default
	if cfg.Description != "" {
		t.Errorf("expected empty Description, got %q", cfg.Description)
	}
}

func TestConfigWithEmbedders(t *testing.T) {
	cfg := DefaultConfig()

	// Add embedders as json.RawMessage
	cfg.Embedders["llamacpp-text"] = json.RawMessage(`{"base_url":"http://localhost:3311","model":"text-embedding"}`)
	cfg.Embedders["openai-image"] = json.RawMessage(`{"api_key":"sk-test","model":"clip"}`)

	if len(cfg.Embedders) != 2 {
		t.Errorf("expected 2 embedders, got %d", len(cfg.Embedders))
	}

	// Verify embedders can be unmarshaled
	var llamaCfg map[string]any
	if err := json.Unmarshal(cfg.Embedders["llamacpp-text"], &llamaCfg); err != nil {
		t.Errorf("failed to unmarshal llamacpp-text: %v", err)
	} else if llamaCfg["base_url"] != "http://localhost:3311" {
		t.Errorf("llamacpp-text base_url mismatch")
	}

	var openaiCfg map[string]any
	if err := json.Unmarshal(cfg.Embedders["openai-image"], &openaiCfg); err != nil {
		t.Errorf("failed to unmarshal openai-image: %v", err)
	} else if openaiCfg["api_key"] != "sk-test" {
		t.Errorf("openai-image api_key mismatch")
	}
}

func TestConfigJSONSerialization(t *testing.T) {
	// Create Config with embedders
	cfg := DefaultConfig()
	cfg.DatasetOptions.VectorSize = 384
	cfg.Description = "Test collection"

	cfg.Embedders["llamacpp-text"] = json.RawMessage(`{"base_url":"http://localhost:3311"}`)

	// Serialize to JSON
	data, err := json.Marshal(cfg)
	if err != nil {
		t.Fatalf("failed to marshal config: %v", err)
	}

	// Deserialize from JSON
	var decoded Config
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("failed to unmarshal config: %v", err)
	}

	// Verify dataset options
	if decoded.DatasetOptions.VectorSize != cfg.DatasetOptions.VectorSize {
		t.Errorf("VectorSize mismatch: expected %d, got %d",
			cfg.DatasetOptions.VectorSize, decoded.DatasetOptions.VectorSize)
	}

	// Verify description
	if decoded.Description != cfg.Description {
		t.Errorf("Description mismatch: expected %q, got %q",
			cfg.Description, decoded.Description)
	}

	// Verify embedders
	if len(decoded.Embedders) != len(cfg.Embedders) {
		t.Errorf("embedders count mismatch: expected %d, got %d",
			len(cfg.Embedders), len(decoded.Embedders))
	}
}

func TestConfigWithDescription(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Description = "My test collection for embeddings"
	cfg.DatasetOptions.VectorSize = 512

	// Serialize to JSON
	data, err := json.Marshal(cfg)
	if err != nil {
		t.Fatalf("failed to marshal config: %v", err)
	}

	// Deserialize
	var decoded Config
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("failed to unmarshal config: %v", err)
	}

	// Verify description persisted
	if decoded.Description != cfg.Description {
		t.Errorf("expected Description %q, got %q", cfg.Description, decoded.Description)
	}
}

func TestConfigDescriptionOmitEmpty(t *testing.T) {
	// Config without description
	cfg := DefaultConfig()
	cfg.DatasetOptions.VectorSize = 384

	// Serialize to JSON
	data, err := json.Marshal(cfg)
	if err != nil {
		t.Fatalf("failed to marshal config: %v", err)
	}

	// Verify "description" field is not in JSON
	var raw map[string]any
	if err := json.Unmarshal(data, &raw); err != nil {
		t.Fatalf("failed to unmarshal to map: %v", err)
	}

	if _, exists := raw["description"]; exists {
		t.Error("expected description field to be omitted when empty")
	}
}
