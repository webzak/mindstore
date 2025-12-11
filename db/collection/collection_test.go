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

func TestCollectionRead(t *testing.T) {
	tempDir := t.TempDir()

	opts := DefaultOptions()
	opts.DatasetOptions.VectorSize = 4

	coll, err := CreateCollection(tempDir, "test_read", opts)
	if err != nil {
		t.Fatalf("failed to create collection: %v", err)
	}
	defer coll.Close()

	// Add test items with various data
	vector1 := []float32{1.0, 2.0, 3.0, 4.0}
	_, err = coll.AddText("first item").
		WithMeta("key1", "value1").
		WithMeta("num", 42).
		WithTag("tag1").
		WithTag("tag2").
		WithVector(vector1).
		Apply()
	if err != nil {
		t.Fatalf("failed to add first item: %v", err)
	}

	if err := coll.Flush(); err != nil {
		t.Fatalf("failed to flush: %v", err)
	}

	// Test: Read without vector (default)
	item, err := coll.Read(0, 0)
	if err != nil {
		t.Fatalf("failed to read item: %v", err)
	}

	// Verify data
	if item.TextData() != "first item" {
		t.Errorf("expected TextData() 'first item', got %q", item.TextData())
	}

	// Verify metadata
	meta := item.Meta()
	if meta == nil {
		t.Fatal("expected metadata, got nil")
	}
	if meta["key1"] != "value1" {
		t.Errorf("expected key1=value1, got %v", meta["key1"])
	}
	if num, ok := meta["num"].(float64); !ok || num != 42 {
		t.Errorf("expected num=42, got %v", meta["num"])
	}

	// Verify tags
	tags := item.Tags()
	if len(tags) != 2 {
		t.Errorf("expected 2 tags, got %d", len(tags))
	}

	// Verify vector is nil (not loaded)
	if item.Vector() != nil {
		t.Errorf("expected vector to be nil when not requested, got %v", item.Vector())
	}

	// Verify descriptor
	if item.DataDescriptor() != Text {
		t.Errorf("expected DataDescriptor Text, got %v", item.DataDescriptor())
	}
}

func TestCollectionReadWithVector(t *testing.T) {
	tempDir := t.TempDir()

	opts := DefaultOptions()
	opts.DatasetOptions.VectorSize = 4

	coll, err := CreateCollection(tempDir, "test_read_vec", opts)
	if err != nil {
		t.Fatalf("failed to create collection: %v", err)
	}
	defer coll.Close()

	// Add item with vector
	vector := []float32{5.0, 6.0, 7.0, 8.0}
	_, err = coll.AddText("test item").
		WithVector(vector).
		Apply()
	if err != nil {
		t.Fatalf("failed to add item: %v", err)
	}

	if err := coll.Flush(); err != nil {
		t.Fatalf("failed to flush: %v", err)
	}

	// Test: Read with ReturnVector option
	item, err := coll.Read(0, ReturnVector)
	if err != nil {
		t.Fatalf("failed to read item: %v", err)
	}

	// Verify vector is loaded
	itemVec := item.Vector()
	if itemVec == nil {
		t.Fatal("expected vector to be loaded, got nil")
	}
	if len(itemVec) != 4 {
		t.Errorf("expected vector length 4, got %d", len(itemVec))
	}
	for i, expected := range vector {
		if itemVec[i] != expected {
			t.Errorf("vector[%d]: expected %f, got %f", i, expected, itemVec[i])
		}
	}

	// Test: Read without ReturnVector option
	itemNoVec, err := coll.Read(0, 0)
	if err != nil {
		t.Fatalf("failed to read item without vector: %v", err)
	}

	// Verify vector is nil
	if itemNoVec.Vector() != nil {
		t.Errorf("expected vector to be nil without ReturnVector option, got %v", itemNoVec.Vector())
	}
}

func TestCollectionReadNonExistent(t *testing.T) {
	tempDir := t.TempDir()

	coll, err := CreateCollection(tempDir, "test_read_err", DefaultOptions())
	if err != nil {
		t.Fatalf("failed to create collection: %v", err)
	}
	defer coll.Close()

	// Try to read non-existent item
	_, err = coll.Read(999, 0)
	if err == nil {
		t.Error("expected error when reading non-existent item, got nil")
	}
}

func TestCollectionReadNoMetadata(t *testing.T) {
	tempDir := t.TempDir()

	coll, err := CreateCollection(tempDir, "test_read_nometa", DefaultOptions())
	if err != nil {
		t.Fatalf("failed to create collection: %v", err)
	}
	defer coll.Close()

	// Add item without metadata
	_, err = coll.AddText("item without metadata").Apply()
	if err != nil {
		t.Fatalf("failed to add item: %v", err)
	}

	if err := coll.Flush(); err != nil {
		t.Fatalf("failed to flush: %v", err)
	}

	// Read item
	item, err := coll.Read(0, 0)
	if err != nil {
		t.Fatalf("failed to read item: %v", err)
	}

	// Verify metadata is nil
	if item.Meta() != nil {
		t.Errorf("expected nil metadata for item without metadata, got %v", item.Meta())
	}

	// Verify data is present
	if item.TextData() != "item without metadata" {
		t.Errorf("expected TextData() 'item without metadata', got %q", item.TextData())
	}
}

func TestItemTextData(t *testing.T) {
	tempDir := t.TempDir()

	opts := DefaultOptions()
	opts.DatasetOptions.VectorSize = 2

	coll, err := CreateCollection(tempDir, "test_textdata", opts)
	if err != nil {
		t.Fatalf("failed to create collection: %v", err)
	}
	defer coll.Close()

	// Add Text item
	_, err = coll.AddText("hello world").Apply()
	if err != nil {
		t.Fatalf("failed to add text item: %v", err)
	}

	// Add Image item (non-text)
	imageItem := coll.NewItem()
	imageItem.data = []byte{0x01, 0x02, 0x03}
	imageItem.dataDescriptor = Image
	imageItem.vector = []float32{1.0, 2.0}
	_, err = imageItem.Apply()
	if err != nil {
		t.Fatalf("failed to add image item: %v", err)
	}

	if err := coll.Flush(); err != nil {
		t.Fatalf("failed to flush: %v", err)
	}

	// Test: TextData() on Text item
	textItem, err := coll.Read(0, 0)
	if err != nil {
		t.Fatalf("failed to read text item: %v", err)
	}

	if textItem.TextData() != "hello world" {
		t.Errorf("expected TextData() 'hello world', got %q", textItem.TextData())
	}

	// Test: TextData() on Image item returns empty string
	imgItem, err := coll.Read(1, 0)
	if err != nil {
		t.Fatalf("failed to read image item: %v", err)
	}

	if imgItem.TextData() != "" {
		t.Errorf("expected TextData() to return empty string for Image item, got %q", imgItem.TextData())
	}

	// Verify Data() works for both
	if string(textItem.Data()) != "hello world" {
		t.Errorf("expected Data() 'hello world', got %q", string(textItem.Data()))
	}

	if len(imgItem.Data()) != 3 {
		t.Errorf("expected Data() length 3 for image, got %d", len(imgItem.Data()))
	}
}

func TestItemMetaValue(t *testing.T) {
	tempDir := t.TempDir()

	coll, err := CreateCollection(tempDir, "test_metavalue", DefaultOptions())
	if err != nil {
		t.Fatalf("failed to create collection: %v", err)
	}
	defer coll.Close()

	// Add item with metadata
	_, err = coll.AddText("test").
		WithMeta("author", "Alice").
		WithMeta("count", 5).
		Apply()
	if err != nil {
		t.Fatalf("failed to add item: %v", err)
	}

	// Add item without metadata
	_, err = coll.AddText("no meta").Apply()
	if err != nil {
		t.Fatalf("failed to add item without meta: %v", err)
	}

	if err := coll.Flush(); err != nil {
		t.Fatalf("failed to flush: %v", err)
	}

	// Test: MetaValue() for existing key
	item, err := coll.Read(0, 0)
	if err != nil {
		t.Fatalf("failed to read item: %v", err)
	}

	value, exists := item.MetaValue("author")
	if !exists {
		t.Error("expected author key to exist")
	}
	if value != "Alice" {
		t.Errorf("expected author=Alice, got %v", value)
	}

	// Test: MetaValue() for non-existing key
	value, exists = item.MetaValue("nonexistent")
	if exists {
		t.Error("expected nonexistent key to not exist")
	}
	if value != nil {
		t.Errorf("expected nil value for nonexistent key, got %v", value)
	}

	// Test: MetaValue() on item without metadata
	itemNoMeta, err := coll.Read(1, 0)
	if err != nil {
		t.Fatalf("failed to read item without meta: %v", err)
	}

	value, exists = itemNoMeta.MetaValue("any")
	if exists {
		t.Error("expected key to not exist on item without metadata")
	}
	if value != nil {
		t.Errorf("expected nil value, got %v", value)
	}
}

func TestItemTags(t *testing.T) {
	tempDir := t.TempDir()

	coll, err := CreateCollection(tempDir, "test_tags", DefaultOptions())
	if err != nil {
		t.Fatalf("failed to create collection: %v", err)
	}
	defer coll.Close()

	// Add item with tags
	_, err = coll.AddText("tagged").
		WithTags("tag1", "tag2", "tag3").
		Apply()
	if err != nil {
		t.Fatalf("failed to add item with tags: %v", err)
	}

	// Add item without tags
	_, err = coll.AddText("untagged").Apply()
	if err != nil {
		t.Fatalf("failed to add item without tags: %v", err)
	}

	if err := coll.Flush(); err != nil {
		t.Fatalf("failed to flush: %v", err)
	}

	// Test: Tags() on item with tags
	itemWithTags, err := coll.Read(0, 0)
	if err != nil {
		t.Fatalf("failed to read item with tags: %v", err)
	}

	tags := itemWithTags.Tags()
	if len(tags) != 3 {
		t.Errorf("expected 3 tags, got %d", len(tags))
	}
	expectedTags := []string{"tag1", "tag2", "tag3"}
	for i, tag := range tags {
		if tag != expectedTags[i] {
			t.Errorf("tag[%d]: expected %q, got %q", i, expectedTags[i], tag)
		}
	}

	// Test: Tags() on item without tags
	itemNoTags, err := coll.Read(1, 0)
	if err != nil {
		t.Fatalf("failed to read item without tags: %v", err)
	}

	noTags := itemNoTags.Tags()
	if noTags != nil && len(noTags) != 0 {
		t.Errorf("expected nil or empty tags, got %v", noTags)
	}
}

func TestItemGroupAndPlace(t *testing.T) {
	tempDir := t.TempDir()

	coll, err := CreateCollection(tempDir, "test_group", DefaultOptions())
	if err != nil {
		t.Fatalf("failed to create collection: %v", err)
	}
	defer coll.Close()

	// Add items to a group
	_, err = coll.AddText("first").WithNewGroup().Apply()
	if err != nil {
		t.Fatalf("failed to add first item: %v", err)
	}

	// Get the group ID from the first item
	if err := coll.Flush(); err != nil {
		t.Fatalf("failed to flush: %v", err)
	}

	firstItem, err := coll.Read(0, 0)
	if err != nil {
		t.Fatalf("failed to read first item: %v", err)
	}

	groupID := firstItem.Group()
	if groupID == 0 {
		t.Error("expected group ID to be non-zero")
	}

	// Add more items to the same group
	_, err = coll.AddText("second").WithGroup(groupID, 1).Apply()
	if err != nil {
		t.Fatalf("failed to add second item: %v", err)
	}

	_, err = coll.AddText("third").WithGroup(groupID, 2).Apply()
	if err != nil {
		t.Fatalf("failed to add third item: %v", err)
	}

	// Add item without group
	_, err = coll.AddText("ungrouped").Apply()
	if err != nil {
		t.Fatalf("failed to add ungrouped item: %v", err)
	}

	if err := coll.Flush(); err != nil {
		t.Fatalf("failed to flush: %v", err)
	}

	// Test: Group() and GroupPlace() on grouped items
	item2, err := coll.Read(1, 0)
	if err != nil {
		t.Fatalf("failed to read item 2: %v", err)
	}

	if item2.Group() != groupID {
		t.Errorf("expected group ID %d, got %d", groupID, item2.Group())
	}
	if item2.GroupPlace() != 1 {
		t.Errorf("expected group place 1, got %d", item2.GroupPlace())
	}

	item3, err := coll.Read(2, 0)
	if err != nil {
		t.Fatalf("failed to read item 3: %v", err)
	}

	if item3.GroupPlace() != 2 {
		t.Errorf("expected group place 2, got %d", item3.GroupPlace())
	}

	// Test: Group() on ungrouped item
	ungrouped, err := coll.Read(3, 0)
	if err != nil {
		t.Fatalf("failed to read ungrouped item: %v", err)
	}

	if ungrouped.Group() != 0 {
		t.Errorf("expected group ID 0 for ungrouped item, got %d", ungrouped.Group())
	}
}

func TestItemFlags(t *testing.T) {
	tempDir := t.TempDir()

	coll, err := CreateCollection(tempDir, "test_flags", DefaultOptions())
	if err != nil {
		t.Fatalf("failed to create collection: %v", err)
	}
	defer coll.Close()

	// Add item with flags
	_, err = coll.AddText("flagged").WithFlags(42).Apply()
	if err != nil {
		t.Fatalf("failed to add item: %v", err)
	}

	// Add item without flags
	_, err = coll.AddText("no flags").Apply()
	if err != nil {
		t.Fatalf("failed to add item without flags: %v", err)
	}

	if err := coll.Flush(); err != nil {
		t.Fatalf("failed to flush: %v", err)
	}

	// Test: Flags() on item with flags
	flaggedItem, err := coll.Read(0, 0)
	if err != nil {
		t.Fatalf("failed to read flagged item: %v", err)
	}

	if flaggedItem.Flags() != 42 {
		t.Errorf("expected flags 42, got %d", flaggedItem.Flags())
	}

	// Test: Flags() on item without flags
	noFlagsItem, err := coll.Read(1, 0)
	if err != nil {
		t.Fatalf("failed to read item without flags: %v", err)
	}

	if noFlagsItem.Flags() != 0 {
		t.Errorf("expected flags 0, got %d", noFlagsItem.Flags())
	}
}

func TestItemDataDescriptor(t *testing.T) {
	tempDir := t.TempDir()

	opts := DefaultOptions()
	opts.DatasetOptions.VectorSize = 2

	coll, err := CreateCollection(tempDir, "test_descriptor", opts)
	if err != nil {
		t.Fatalf("failed to create collection: %v", err)
	}
	defer coll.Close()

	// Add Text item
	_, err = coll.AddText("text item").
		WithVector([]float32{1.0, 2.0}).
		Apply()
	if err != nil {
		t.Fatalf("failed to add text item: %v", err)
	}

	// Add Image item
	imageItem := coll.NewItem()
	imageItem.data = []byte{0xff, 0xd8, 0xff}
	imageItem.dataDescriptor = Image
	imageItem.vector = []float32{3.0, 4.0}
	_, err = imageItem.Apply()
	if err != nil {
		t.Fatalf("failed to add image item: %v", err)
	}

	if err := coll.Flush(); err != nil {
		t.Fatalf("failed to flush: %v", err)
	}

	// Test: DataDescriptor() on Text item
	textItem, err := coll.Read(0, 0)
	if err != nil {
		t.Fatalf("failed to read text item: %v", err)
	}

	if textItem.DataDescriptor() != Text {
		t.Errorf("expected DataDescriptor Text, got %v", textItem.DataDescriptor())
	}

	// Test: DataDescriptor() on Image item
	imgItem, err := coll.Read(1, 0)
	if err != nil {
		t.Fatalf("failed to read image item: %v", err)
	}

	if imgItem.DataDescriptor() != Image {
		t.Errorf("expected DataDescriptor Image, got %v", imgItem.DataDescriptor())
	}
}

func TestCollectionApplyReadRoundtrip(t *testing.T) {
	tempDir := t.TempDir()

	opts := DefaultOptions()
	opts.DatasetOptions.VectorSize = 4

	coll, err := CreateCollection(tempDir, "test_roundtrip", opts)
	if err != nil {
		t.Fatalf("failed to create collection: %v", err)
	}
	defer coll.Close()

	// Apply item with all fields populated
	originalVector := []float32{1.0, 2.0, 3.0, 4.0}
	dsItem, err := coll.AddText("roundtrip test data").
		WithMeta("author", "Bob").
		WithMeta("version", 3).
		WithMeta("active", true).
		WithTags("test", "integration", "roundtrip").
		WithFlags(123).
		WithVector(originalVector).
		WithNewGroup().
		Apply()
	if err != nil {
		t.Fatalf("failed to apply item: %v", err)
	}

	originalID := dsItem.ID

	if err := coll.Flush(); err != nil {
		t.Fatalf("failed to flush: %v", err)
	}

	// Test 1: Read back without vector
	t.Run("ReadWithoutVector", func(t *testing.T) {
		item, err := coll.Read(originalID, 0)
		if err != nil {
			t.Fatalf("failed to read item: %v", err)
		}

		// Verify all accessor methods
		if item.TextData() != "roundtrip test data" {
			t.Errorf("TextData() mismatch: got %q", item.TextData())
		}

		if string(item.Data()) != "roundtrip test data" {
			t.Errorf("Data() mismatch: got %q", string(item.Data()))
		}

		meta := item.Meta()
		if meta == nil {
			t.Fatal("Meta() returned nil")
		}
		if meta["author"] != "Bob" {
			t.Errorf("Meta author mismatch: got %v", meta["author"])
		}
		if meta["version"] != float64(3) {
			t.Errorf("Meta version mismatch: got %v", meta["version"])
		}
		if meta["active"] != true {
			t.Errorf("Meta active mismatch: got %v", meta["active"])
		}

		author, exists := item.MetaValue("author")
		if !exists || author != "Bob" {
			t.Errorf("MetaValue(author) mismatch: got %v, exists=%v", author, exists)
		}

		tags := item.Tags()
		if len(tags) != 3 {
			t.Errorf("Tags() length mismatch: got %d", len(tags))
		}
		expectedTags := []string{"test", "integration", "roundtrip"}
		for i, tag := range expectedTags {
			if tags[i] != tag {
				t.Errorf("Tags[%d] mismatch: expected %q, got %q", i, tag, tags[i])
			}
		}

		if item.Flags() != 123 {
			t.Errorf("Flags() mismatch: got %d", item.Flags())
		}

		if item.Group() == 0 {
			t.Error("Group() should be non-zero")
		}

		if item.GroupPlace() != 0 {
			t.Errorf("GroupPlace() mismatch: got %d", item.GroupPlace())
		}

		if item.DataDescriptor() != Text {
			t.Errorf("DataDescriptor() mismatch: got %v", item.DataDescriptor())
		}

		// Vector should be nil
		if item.Vector() != nil {
			t.Errorf("Vector() should be nil without ReturnVector, got %v", item.Vector())
		}
	})

	// Test 2: Read back with vector
	t.Run("ReadWithVector", func(t *testing.T) {
		item, err := coll.Read(originalID, ReturnVector)
		if err != nil {
			t.Fatalf("failed to read item with vector: %v", err)
		}

		// Verify vector is loaded correctly
		vec := item.Vector()
		if vec == nil {
			t.Fatal("Vector() returned nil with ReturnVector option")
		}
		if len(vec) != 4 {
			t.Errorf("Vector() length mismatch: got %d", len(vec))
		}
		for i, expected := range originalVector {
			if vec[i] != expected {
				t.Errorf("Vector[%d] mismatch: expected %f, got %f", i, expected, vec[i])
			}
		}

		// Verify other fields are still present
		if item.TextData() != "roundtrip test data" {
			t.Errorf("TextData() mismatch with vector: got %q", item.TextData())
		}
		if item.Flags() != 123 {
			t.Errorf("Flags() mismatch with vector: got %d", item.Flags())
		}
	})
}
