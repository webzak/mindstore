package collection

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestAddText(t *testing.T) {
	tmpDir := t.TempDir()
	coll, err := CreateCollection(tmpDir, "test", DefaultOptions())
	if err != nil {
		t.Fatalf("failed to create collection: %v", err)
	}
	defer coll.Close()

	item := coll.AddText("hello world")

	if item.collection != coll {
		t.Error("collection reference not set")
	}
	if string(item.data) != "hello world" {
		t.Errorf("data = %q, want %q", item.data, "hello world")
	}
	if item.dataDescriptor != Text {
		t.Errorf("dataDescriptor = %d, want %d", item.dataDescriptor, Text)
	}
	if item.meta != nil {
		t.Errorf("meta should be nil, got %v", item.meta)
	}
}

func TestNewItem(t *testing.T) {
	tmpDir := t.TempDir()
	coll, err := CreateCollection(tmpDir, "test", DefaultOptions())
	if err != nil {
		t.Fatalf("failed to create collection: %v", err)
	}
	defer coll.Close()

	item := coll.NewItem()

	if item.collection != coll {
		t.Error("collection reference not set")
	}
	if item.data != nil {
		t.Errorf("data should be nil, got %v", item.data)
	}
	if item.dataDescriptor != 0 {
		t.Errorf("dataDescriptor should be 0, got %d", item.dataDescriptor)
	}
}

func TestWithMeta(t *testing.T) {
	tmpDir := t.TempDir()
	coll, err := CreateCollection(tmpDir, "test", DefaultOptions())
	if err != nil {
		t.Fatalf("failed to create collection: %v", err)
	}
	defer coll.Close()

	item := coll.AddText("test").
		WithMeta("author", "John").
		WithMeta("count", 42)

	if item.meta == nil {
		t.Fatal("meta map not initialized")
	}
	if item.meta["author"] != "John" {
		t.Errorf("author = %v, want John", item.meta["author"])
	}
	if item.meta["count"] != 42 {
		t.Errorf("count = %v, want 42", item.meta["count"])
	}
}

func TestWithMetadata(t *testing.T) {
	tmpDir := t.TempDir()
	coll, err := CreateCollection(tmpDir, "test", DefaultOptions())
	if err != nil {
		t.Fatalf("failed to create collection: %v", err)
	}
	defer coll.Close()

	metadata := map[string]any{
		"key1": "value1",
		"key2": 123,
	}

	item := coll.AddText("test").WithMetadata(metadata)

	if item.meta["key1"] != "value1" {
		t.Errorf("key1 = %v, want value1", item.meta["key1"])
	}
	if item.meta["key2"] != 123 {
		t.Errorf("key2 = %v, want 123", item.meta["key2"])
	}
}

func TestWithMetadataMerge(t *testing.T) {
	tmpDir := t.TempDir()
	coll, err := CreateCollection(tmpDir, "test", DefaultOptions())
	if err != nil {
		t.Fatalf("failed to create collection: %v", err)
	}
	defer coll.Close()

	item := coll.AddText("test").
		WithMeta("existing", "value").
		WithMetadata(map[string]any{"new": "value2"})

	if item.meta["existing"] != "value" {
		t.Error("existing metadata was lost")
	}
	if item.meta["new"] != "value2" {
		t.Error("new metadata not merged")
	}
}

func TestWithTags(t *testing.T) {
	tmpDir := t.TempDir()
	coll, err := CreateCollection(tmpDir, "test", DefaultOptions())
	if err != nil {
		t.Fatalf("failed to create collection: %v", err)
	}
	defer coll.Close()

	item := coll.AddText("test").WithTags("tag1", "tag2", "tag3")

	if len(item.tags) != 3 {
		t.Fatalf("expected 3 tags, got %d", len(item.tags))
	}
	if item.tags[0] != "tag1" || item.tags[1] != "tag2" || item.tags[2] != "tag3" {
		t.Errorf("tags = %v, want [tag1 tag2 tag3]", item.tags)
	}
}

func TestWithTag(t *testing.T) {
	tmpDir := t.TempDir()
	coll, err := CreateCollection(tmpDir, "test", DefaultOptions())
	if err != nil {
		t.Fatalf("failed to create collection: %v", err)
	}
	defer coll.Close()

	item := coll.AddText("test").
		WithTag("tag1").
		WithTag("tag2")

	if len(item.tags) != 2 {
		t.Fatalf("expected 2 tags, got %d", len(item.tags))
	}
	if item.tags[0] != "tag1" || item.tags[1] != "tag2" {
		t.Errorf("tags = %v, want [tag1 tag2]", item.tags)
	}
}

func TestWithVector(t *testing.T) {
	tmpDir := t.TempDir()
	coll, err := CreateCollection(tmpDir, "test", DefaultOptions())
	if err != nil {
		t.Fatalf("failed to create collection: %v", err)
	}
	defer coll.Close()

	vector := make([]float32, 768)
	for i := range vector {
		vector[i] = float32(i)
	}

	item := coll.AddText("test").WithVector(vector)

	if len(item.vector) != 768 {
		t.Fatalf("vector length = %d, want 768", len(item.vector))
	}
	if item.vector[0] != 0 || item.vector[767] != 767 {
		t.Error("vector values not set correctly")
	}
}

func TestWithFlags(t *testing.T) {
	tmpDir := t.TempDir()
	coll, err := CreateCollection(tmpDir, "test", DefaultOptions())
	if err != nil {
		t.Fatalf("failed to create collection: %v", err)
	}
	defer coll.Close()

	item := coll.AddText("test").WithFlags(0x42)

	if item.flags != 0x42 {
		t.Errorf("flags = %d, want 0x42", item.flags)
	}
}

func TestWithGroup(t *testing.T) {
	tmpDir := t.TempDir()
	coll, err := CreateCollection(tmpDir, "test", DefaultOptions())
	if err != nil {
		t.Fatalf("failed to create collection: %v", err)
	}
	defer coll.Close()

	item := coll.AddText("test").WithGroup(5, 10)

	if item.groupID != 5 {
		t.Errorf("groupID = %d, want 5", item.groupID)
	}
	if item.groupPlace != 10 {
		t.Errorf("groupPlace = %d, want 10", item.groupPlace)
	}
}

func TestWithNewGroup(t *testing.T) {
	tmpDir := t.TempDir()
	coll, err := CreateCollection(tmpDir, "test", DefaultOptions())
	if err != nil {
		t.Fatalf("failed to create collection: %v", err)
	}
	defer coll.Close()

	item := coll.AddText("test").WithNewGroup()

	if item.groupID != -1 {
		t.Errorf("groupID = %d, want -1", item.groupID)
	}
	if item.groupPlace != 0 {
		t.Errorf("groupPlace = %d, want 0", item.groupPlace)
	}
}

func TestChaining(t *testing.T) {
	tmpDir := t.TempDir()
	coll, err := CreateCollection(tmpDir, "test", DefaultOptions())
	if err != nil {
		t.Fatalf("failed to create collection: %v", err)
	}
	defer coll.Close()

	// Test that all methods return *Item for chaining
	item := coll.AddText("test").
		WithMeta("key", "value").
		WithTags("tag1").
		WithFlags(1).
		WithGroup(1, 0)

	if item.collection != coll {
		t.Error("chaining broke collection reference")
	}
}

func TestApplySuccess(t *testing.T) {
	tmpDir := t.TempDir()
	coll, err := CreateCollection(tmpDir, "test", DefaultOptions())
	if err != nil {
		t.Fatalf("failed to create collection: %v", err)
	}
	defer coll.Close()

	result, err := coll.AddText("hello world").
		WithMeta("author", "John").
		WithTags("test", "demo").
		Apply()

	if err != nil {
		t.Fatalf("Apply failed: %v", err)
	}

	if result.ID != 0 {
		t.Errorf("ID = %d, want 0", result.ID)
	}
	if string(result.Data) != "hello world" {
		t.Errorf("Data = %q, want 'hello world'", result.Data)
	}
	if result.DataDescriptor != uint8(Text) {
		t.Errorf("DataDescriptor = %d, want %d", result.DataDescriptor, Text)
	}
	if len(result.Tags) != 2 {
		t.Errorf("Tags length = %d, want 2", len(result.Tags))
	}
}

func TestApplyMetadataJSON(t *testing.T) {
	tmpDir := t.TempDir()
	coll, err := CreateCollection(tmpDir, "test", DefaultOptions())
	if err != nil {
		t.Fatalf("failed to create collection: %v", err)
	}
	defer coll.Close()

	now := time.Now()
	result, err := coll.AddText("test").
		WithMeta("author", "Jane").
		WithMeta("count", 123).
		WithMeta("timestamp", now.Unix()).
		Apply()

	if err != nil {
		t.Fatalf("Apply failed: %v", err)
	}

	// Deserialize and check metadata
	meta, err := result.GetMeta()
	if err != nil {
		t.Fatalf("GetMeta failed: %v", err)
	}

	if meta["author"] != "Jane" {
		t.Errorf("author = %v, want Jane", meta["author"])
	}
	// JSON numbers are float64
	if meta["count"] != float64(123) {
		t.Errorf("count = %v, want 123", meta["count"])
	}
}

func TestApplyNoMetadata(t *testing.T) {
	tmpDir := t.TempDir()
	coll, err := CreateCollection(tmpDir, "test", DefaultOptions())
	if err != nil {
		t.Fatalf("failed to create collection: %v", err)
	}
	defer coll.Close()

	result, err := coll.AddText("no metadata").Apply()

	if err != nil {
		t.Fatalf("Apply failed: %v", err)
	}

	if result.Meta != nil {
		t.Errorf("Meta should be nil, got %v", result.Meta)
	}
}

func TestApplyVectorSizeMismatch(t *testing.T) {
	tmpDir := t.TempDir()
	coll, err := CreateCollection(tmpDir, "test", DefaultOptions())
	if err != nil {
		t.Fatalf("failed to create collection: %v", err)
	}
	defer coll.Close()

	wrongSizeVector := make([]float32, 100) // Default is 768

	_, err = coll.AddText("test").
		WithVector(wrongSizeVector).
		Apply()

	if err == nil {
		t.Error("expected error for vector size mismatch")
	}
}

func TestApplyWithNewGroup(t *testing.T) {
	tmpDir := t.TempDir()
	coll, err := CreateCollection(tmpDir, "test", DefaultOptions())
	if err != nil {
		t.Fatalf("failed to create collection: %v", err)
	}
	defer coll.Close()

	result, err := coll.AddText("first in group").WithNewGroup().Apply()

	if err != nil {
		t.Fatalf("Apply failed: %v", err)
	}

	if result.GroupID <= 0 {
		t.Errorf("GroupID should be auto-assigned, got %d", result.GroupID)
	}
}

func TestGetMeta(t *testing.T) {
	tmpDir := t.TempDir()
	coll, err := CreateCollection(tmpDir, "test", DefaultOptions())
	if err != nil {
		t.Fatalf("failed to create collection: %v", err)
	}
	defer coll.Close()

	result, err := coll.AddText("test").
		WithMeta("key1", "value1").
		WithMeta("key2", 42).
		Apply()

	if err != nil {
		t.Fatalf("Apply failed: %v", err)
	}

	meta, err := result.GetMeta()
	if err != nil {
		t.Fatalf("GetMeta failed: %v", err)
	}

	if meta["key1"] != "value1" {
		t.Errorf("key1 = %v, want value1", meta["key1"])
	}
	if meta["key2"] != float64(42) { // JSON numbers are float64
		t.Errorf("key2 = %v, want 42", meta["key2"])
	}
}

func TestGetMetaEmpty(t *testing.T) {
	tmpDir := t.TempDir()
	coll, err := CreateCollection(tmpDir, "test", DefaultOptions())
	if err != nil {
		t.Fatalf("failed to create collection: %v", err)
	}
	defer coll.Close()

	result, err := coll.AddText("test").Apply()

	if err != nil {
		t.Fatalf("Apply failed: %v", err)
	}

	meta, err := result.GetMeta()
	if err != nil {
		t.Fatalf("GetMeta failed: %v", err)
	}

	if len(meta) != 0 {
		t.Errorf("expected empty map, got %v", meta)
	}
}

func TestGetMetaValue(t *testing.T) {
	tmpDir := t.TempDir()
	coll, err := CreateCollection(tmpDir, "test", DefaultOptions())
	if err != nil {
		t.Fatalf("failed to create collection: %v", err)
	}
	defer coll.Close()

	result, err := coll.AddText("test").
		WithMeta("author", "Alice").
		Apply()

	if err != nil {
		t.Fatalf("Apply failed: %v", err)
	}

	value, err := result.GetMetaValue("author")
	if err != nil {
		t.Fatalf("GetMetaValue failed: %v", err)
	}

	if value != "Alice" {
		t.Errorf("value = %v, want Alice", value)
	}
}

func TestGetMetaValueNotFound(t *testing.T) {
	tmpDir := t.TempDir()
	coll, err := CreateCollection(tmpDir, "test", DefaultOptions())
	if err != nil {
		t.Fatalf("failed to create collection: %v", err)
	}
	defer coll.Close()

	result, err := coll.AddText("test").
		WithMeta("key1", "value1").
		Apply()

	if err != nil {
		t.Fatalf("Apply failed: %v", err)
	}

	_, err = result.GetMetaValue("nonexistent")
	if err == nil {
		t.Error("expected error for nonexistent key")
	}
}

func TestComplexUsageExample(t *testing.T) {
	tmpDir := t.TempDir()
	coll, err := CreateCollection(tmpDir, "test", DefaultOptions())
	if err != nil {
		t.Fatalf("failed to create collection: %v", err)
	}
	defer coll.Close()

	// Create first item with new group
	item1, err := coll.AddText("First document").
		WithMeta("author", "John Doe").
		WithMeta("priority", 1).
		WithTags("important", "draft").
		WithNewGroup().
		Apply()

	if err != nil {
		t.Fatalf("failed to create item1: %v", err)
	}

	// Create second item in same group
	item2, err := coll.AddText("Second document").
		WithMeta("author", "Jane Doe").
		WithMeta("priority", 2).
		WithTags("important").
		WithGroup(item1.GroupID, 1).
		Apply()

	if err != nil {
		t.Fatalf("failed to create item2: %v", err)
	}

	// Verify items are in same group
	if item1.GroupID != item2.GroupID {
		t.Error("items should be in the same group")
	}

	// Verify metadata
	meta1, _ := item1.GetMeta()
	if meta1["author"] != "John Doe" {
		t.Errorf("item1 author incorrect")
	}

	meta2, _ := item2.GetMeta()
	if meta2["author"] != "Jane Doe" {
		t.Errorf("item2 author incorrect")
	}
}

// TestReadAndModify tests reading an item and creating a modified version
func TestReadAndModify(t *testing.T) {
	tmpDir := t.TempDir()
	coll, err := CreateCollection(tmpDir, "test", DefaultOptions())
	if err != nil {
		t.Fatalf("failed to create collection: %v", err)
	}
	defer coll.Close()

	// Create original item
	original, err := coll.AddText("original text").
		WithMeta("version", 1).
		WithTags("v1").
		Apply()

	if err != nil {
		t.Fatalf("failed to create original: %v", err)
	}

	// Read and get metadata
	originalMeta, err := original.GetMeta()
	if err != nil {
		t.Fatalf("failed to get meta: %v", err)
	}

	// Create modified version
	modified, err := coll.AddText("modified text").
		WithMeta("version", 2).
		WithMeta("original_version", originalMeta["version"]).
		WithTags("v2", "updated").
		Apply()

	if err != nil {
		t.Fatalf("failed to create modified: %v", err)
	}

	modifiedMeta, _ := modified.GetMeta()
	if modifiedMeta["version"] != float64(2) {
		t.Error("version should be 2")
	}
}

func cleanupTestDir(dir string) {
	os.RemoveAll(dir)
}

func createTestCollection(t *testing.T) (*Collection, string) {
	tmpDir := filepath.Join(os.TempDir(), "mindstore_test_"+t.Name())
	if err := os.MkdirAll(tmpDir, 0755); err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}

	coll, err := CreateCollection(tmpDir, "test", DefaultOptions())
	if err != nil {
		cleanupTestDir(tmpDir)
		t.Fatalf("failed to create collection: %v", err)
	}

	return coll, tmpDir
}
