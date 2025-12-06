package collection

import (
	"encoding/json"
	"testing"

	"github.com/webzak/mindstore/db/dataset"
)

func TestDefaultOptions(t *testing.T) {
	opts := DefaultOptions()

	// Verify dataset options are set to defaults
	defaultDatasetOpts := dataset.DefaultOptions()
	if opts.DatasetOptions.VectorSize != defaultDatasetOpts.VectorSize {
		t.Errorf("expected VectorSize %d, got %d", defaultDatasetOpts.VectorSize, opts.DatasetOptions.VectorSize)
	}
	if opts.DatasetOptions.MaxDataAppendBufferSize != defaultDatasetOpts.MaxDataAppendBufferSize {
		t.Errorf("expected MaxDataAppendBufferSize %d, got %d", defaultDatasetOpts.MaxDataAppendBufferSize, opts.DatasetOptions.MaxDataAppendBufferSize)
	}

	// Verify embedders map is initialized but empty
	if opts.Embedders == nil {
		t.Error("expected Embedders map to be initialized")
	}
	if len(opts.Embedders) != 0 {
		t.Errorf("expected Embedders map to be empty, got %d entries", len(opts.Embedders))
	}
}

func TestOptionsWithEmbedders(t *testing.T) {
	opts := DefaultOptions()

	// Add embedders directly to map
	opts.Embedders["llamacpp-text"] = map[string]any{
		"base_url": "http://localhost:3311",
		"model":    "text-embedding",
	}

	opts.Embedders["openai-image"] = map[string]any{
		"api_key": "sk-test",
		"model":   "clip",
	}

	if len(opts.Embedders) != 2 {
		t.Errorf("expected 2 embedders, got %d", len(opts.Embedders))
	}

	// Verify configs are stored correctly
	llamaCfg, ok := opts.Embedders["llamacpp-text"].(map[string]any)
	if !ok {
		t.Error("llamacpp-text config is not map[string]any")
	} else if llamaCfg["base_url"] != "http://localhost:3311" {
		t.Errorf("llamacpp-text base_url mismatch")
	}

	openaiCfg, ok := opts.Embedders["openai-image"].(map[string]any)
	if !ok {
		t.Error("openai-image config is not map[string]any")
	} else if openaiCfg["api_key"] != "sk-test" {
		t.Errorf("openai-image api_key mismatch")
	}
}

func TestInternalConfigConversion(t *testing.T) {
	// Create Options with embedders
	opts := DefaultOptions()
	opts.DatasetOptions.VectorSize = 384
	opts.DatasetOptions.MaxDataAppendBufferSize = 256 * 1024

	opts.Embedders["llamacpp-text"] = map[string]any{
		"base_url": "http://localhost:3311",
		"model":    "text",
	}
	opts.Embedders["openai-image"] = map[string]any{
		"api_key": "sk-test",
		"model":   "clip",
	}

	// Convert to internal config
	cfg, err := opts.toConfig()
	if err != nil {
		t.Fatalf("failed to convert to config: %v", err)
	}

	// Verify dataset options
	if cfg.DatasetOptions.VectorSize != opts.DatasetOptions.VectorSize {
		t.Errorf("VectorSize mismatch: expected %d, got %d",
			opts.DatasetOptions.VectorSize, cfg.DatasetOptions.VectorSize)
	}

	// Verify embedders were marshaled
	if len(cfg.Embedders) != len(opts.Embedders) {
		t.Errorf("embedders count mismatch: expected %d, got %d",
			len(opts.Embedders), len(cfg.Embedders))
	}

	// Verify embedders are json.RawMessage
	for name := range opts.Embedders {
		if _, ok := cfg.Embedders[name]; !ok {
			t.Errorf("embedder %s not found in config", name)
		}
	}

	// Convert back to Options
	opts2, err := cfg.toOptions()
	if err != nil {
		t.Fatalf("failed to convert back to options: %v", err)
	}

	// Verify round-trip
	if opts2.DatasetOptions.VectorSize != opts.DatasetOptions.VectorSize {
		t.Errorf("VectorSize mismatch after round-trip: expected %d, got %d",
			opts.DatasetOptions.VectorSize, opts2.DatasetOptions.VectorSize)
	}

	if len(opts2.Embedders) != len(opts.Embedders) {
		t.Errorf("embedders count mismatch after round-trip: expected %d, got %d",
			len(opts.Embedders), len(opts2.Embedders))
	}
}

func TestInternalConfigJSONSerialization(t *testing.T) {
	// Create Options and convert to internal config
	opts := DefaultOptions()
	opts.DatasetOptions.VectorSize = 384

	opts.Embedders["llamacpp-text"] = map[string]any{
		"base_url": "http://localhost:3311",
	}

	// Convert to internal config
	cfg, err := opts.toConfig()
	if err != nil {
		t.Fatalf("failed to convert to config: %v", err)
	}

	// Serialize to JSON
	data, err := json.Marshal(cfg)
	if err != nil {
		t.Fatalf("failed to marshal config: %v", err)
	}

	// Deserialize from JSON
	var decoded config
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("failed to unmarshal config: %v", err)
	}

	// Verify dataset options
	if decoded.DatasetOptions.VectorSize != cfg.DatasetOptions.VectorSize {
		t.Errorf("VectorSize mismatch: expected %d, got %d",
			cfg.DatasetOptions.VectorSize, decoded.DatasetOptions.VectorSize)
	}

	// Verify embedders
	if len(decoded.Embedders) != len(cfg.Embedders) {
		t.Errorf("embedders count mismatch: expected %d, got %d",
			len(cfg.Embedders), len(decoded.Embedders))
	}
}
