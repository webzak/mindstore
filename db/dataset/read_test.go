package dataset

import (
	"testing"

	"github.com/webzak/mindstore/internal/testutil/assert"
)

func TestRead(t *testing.T) {
	tmpDir := t.TempDir()
	opts := DefaultOptions()
	opts.VectorSize = 3
	ds, err := Open(tmpDir, "test_read", opts)
	assert.NilError(t, err)
	defer ds.Close()

	// Create test item with all fields
	original := &Item{
		Data:           []byte("test data"),
		Meta:           []byte("test metadata"),
		DataDescriptor: 1,
		MetaDescriptor: 2,
		Flags:          3,
		Vector:         []float32{1.0, 2.0, 3.0},
		Tags:           []string{"tag1", "tag2"},
	}

	id, err := ds.Append(original)
	assert.NilError(t, err)

	tests := []struct {
		name      string
		opts      ReadOptions
		checkData bool
		checkMeta bool
		checkVec  bool
		checkTags bool
		checkGrp  bool
	}{
		{
			name:      "read with no options",
			opts:      0,
			checkData: false,
			checkMeta: false,
			checkVec:  false,
			checkTags: false,
			checkGrp:  false,
		},
		{
			name:      "read data only",
			opts:      ReadData,
			checkData: true,
			checkMeta: false,
			checkVec:  false,
			checkTags: false,
			checkGrp:  false,
		},
		{
			name:      "read metadata only",
			opts:      ReadMeta,
			checkData: false,
			checkMeta: true,
			checkVec:  false,
			checkTags: false,
			checkGrp:  false,
		},
		{
			name:      "read vector only",
			opts:      ReadVector,
			checkData: false,
			checkMeta: false,
			checkVec:  true,
			checkTags: false,
			checkGrp:  false,
		},
		{
			name:      "read tags only",
			opts:      ReadTags,
			checkData: false,
			checkMeta: false,
			checkVec:  false,
			checkTags: true,
			checkGrp:  false,
		},
		{
			name:      "read group only",
			opts:      ReadGroup,
			checkData: false,
			checkMeta: false,
			checkVec:  false,
			checkTags: false,
			checkGrp:  false, // No group in original item
		},
		{
			name:      "read all options",
			opts:      AllReadOptions(),
			checkData: true,
			checkMeta: true,
			checkVec:  true,
			checkTags: true,
			checkGrp:  false, // No group in original item
		},
		{
			name:      "read data and metadata",
			opts:      ReadData | ReadMeta,
			checkData: true,
			checkMeta: true,
			checkVec:  false,
			checkTags: false,
			checkGrp:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			item, err := ds.Read(id, tt.opts)
			assert.NilError(t, err)
			assert.NotNil(t, item, "expected non-nil item")

			// Always check index fields
			assert.Equal(t, id, item.ID)
			assert.Equal(t, original.DataDescriptor, item.DataDescriptor)
			assert.Equal(t, original.MetaDescriptor, item.MetaDescriptor)
			assert.Equal(t, original.Flags, item.Flags)

			// Check optional fields based on flags
			if tt.checkData {
				assert.DeepEqual(t, original.Data, item.Data)
			} else if item.Data != nil {
				t.Error("expected nil Data when not requested")
			}

			if tt.checkMeta {
				assert.DeepEqual(t, original.Meta, item.Meta)
			} else if item.Meta != nil {
				t.Error("expected nil Meta when not requested")
			}

			if tt.checkVec {
				assert.DeepEqual(t, original.Vector, item.Vector)
			} else if item.Vector != nil {
				t.Error("expected nil Vector when not requested")
			}

			if tt.checkTags {
				assert.DeepEqual(t, original.Tags, item.Tags)
			} else if item.Tags != nil {
				t.Error("expected nil Tags when not requested")
			}

			if tt.checkGrp {
				if original.GroupID > 0 {
					if item.GroupID == 0 {
						t.Error("expected GroupID > 0 when original has group")
					}
					assert.Equal(t, original.GroupID, item.GroupID)
					assert.Equal(t, original.GroupPlace, item.GroupPlace)
				} else if item.GroupID > 0 {
					t.Error("expected GroupID = 0 when original has no group")
				}
			} else if item.GroupID > 0 {
				t.Error("expected GroupID = 0 when not requested")
			}
		})
	}
}

func TestReadNonExistentID(t *testing.T) {
	tmpDir := t.TempDir()
	ds, err := Open(tmpDir, "test_read_nonexistent", DefaultOptions())
	assert.NilError(t, err)
	defer ds.Close()

	// Try to read ID that doesn't exist
	_, err = ds.Read(0, ReadData)
	assert.NotNilError(t, err)

	_, err = ds.Read(999, ReadData)
	assert.NotNilError(t, err)

	_, err = ds.Read(-1, ReadData)
	assert.NotNilError(t, err)
}

func TestReadAfterFlush(t *testing.T) {
	tmpDir := t.TempDir()
	ds, err := Open(tmpDir, "test_read_after_flush", DefaultOptions())
	assert.NilError(t, err)
	defer ds.Close()

	// Append items
	data := []byte("test data")
	item := &Item{
		Data:           data,
		DataDescriptor: 1,
	}
	id, err := ds.Append(item)
	assert.NilError(t, err)

	// Flush
	assert.NilError(t, ds.Flush())

	// Read after flush
	retrieved, err := ds.Read(id, ReadData)
	assert.NilError(t, err)
	assert.DeepEqual(t, data, retrieved.Data)
}

func TestReadMultipleItems(t *testing.T) {
	tmpDir := t.TempDir()
	ds, err := Open(tmpDir, "test_read_multiple", DefaultOptions())
	assert.NilError(t, err)
	defer ds.Close()

	// Append multiple items with different data
	numItems := 50
	expectedData := make([][]byte, numItems)

	for i := 0; i < numItems; i++ {
		data := []byte("test data " + string(rune('A'+i%26)))
		expectedData[i] = data
		item := &Item{
			Data:           data,
			DataDescriptor: uint8(i),
		}
		_, err := ds.Append(item)
		assert.NilError(t, err)
	}

	// Read all items and verify
	for i := 0; i < numItems; i++ {
		item, err := ds.Read(i, ReadData)
		assert.NilError(t, err)
		assert.DeepEqual(t, expectedData[i], item.Data)
		assert.Equal(t, uint8(i), item.DataDescriptor)
	}
}

func TestReadWithEmptyData(t *testing.T) {
	tmpDir := t.TempDir()
	ds, err := Open(tmpDir, "test_read_empty", DefaultOptions())
	assert.NilError(t, err)
	defer ds.Close()

	// Append item with empty data
	item := &Item{
		Data:           []byte{},
		DataDescriptor: 1,
	}
	id, err := ds.Append(item)
	assert.NilError(t, err)

	// Read it back
	retrieved, err := ds.Read(id, ReadData)
	assert.NilError(t, err)
	assert.DeepEqual(t, []byte{}, retrieved.Data)
}

func TestReadWithNoVector(t *testing.T) {
	tmpDir := t.TempDir()
	ds, err := Open(tmpDir, "test_read_no_vector", DefaultOptions())
	assert.NilError(t, err)
	defer ds.Close()

	// Append item without vector
	item := &Item{
		Data:           []byte("test"),
		DataDescriptor: 1,
	}
	id, err := ds.Append(item)
	assert.NilError(t, err)

	// Read with vector flag - should not error but vector should be nil/empty
	retrieved, err := ds.Read(id, ReadVector)
	assert.NilError(t, err)
	if retrieved.Vector != nil && len(retrieved.Vector) > 0 {
		t.Errorf("expected nil or empty vector, got %v", retrieved.Vector)
	}
}

func TestReadWithNoTags(t *testing.T) {
	tmpDir := t.TempDir()
	ds, err := Open(tmpDir, "test_read_no_tags", DefaultOptions())
	assert.NilError(t, err)
	defer ds.Close()

	// Append item without tags
	item := &Item{
		Data:           []byte("test"),
		DataDescriptor: 1,
	}
	id, err := ds.Append(item)
	assert.NilError(t, err)

	// Read with tags flag
	retrieved, err := ds.Read(id, ReadTags)
	assert.NilError(t, err)
	assert.DeepEqual(t, []string{}, retrieved.Tags)
}

func TestReadWithNoGroup(t *testing.T) {
	tmpDir := t.TempDir()
	ds, err := Open(tmpDir, "test_read_no_group", DefaultOptions())
	assert.NilError(t, err)
	defer ds.Close()

	// Append item without group
	item := &Item{
		Data:           []byte("test"),
		DataDescriptor: 1,
	}
	id, err := ds.Append(item)
	assert.NilError(t, err)

	// Read with group flag
	retrieved, err := ds.Read(id, ReadGroup)
	assert.NilError(t, err)
	if retrieved.GroupID != 0 {
		t.Errorf("expected GroupID = 0, got %d", retrieved.GroupID)
	}
}

func TestReadOptionsHas(t *testing.T) {
	tests := []struct {
		name     string
		opts     ReadOptions
		flag     ReadOptions
		expected bool
	}{
		{"has data", ReadData, ReadData, true},
		{"has meta", ReadMeta, ReadMeta, true},
		{"has vector", ReadVector, ReadVector, true},
		{"has tags", ReadTags, ReadTags, true},
		{"has group", ReadGroup, ReadGroup, true},
		{"does not have data", ReadMeta, ReadData, false},
		{"combined has data", ReadData | ReadMeta, ReadData, true},
		{"combined has meta", ReadData | ReadMeta, ReadMeta, true},
		{"combined does not have vector", ReadData | ReadMeta, ReadVector, false},
		{"all has everything", AllReadOptions(), ReadData, true},
		{"zero has nothing", ReadOptions(0), ReadData, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.opts.has(tt.flag)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestAllReadOptions(t *testing.T) {
	opts := AllReadOptions()

	// Verify all flags are set
	assert.Equal(t, true, opts.has(ReadData))
	assert.Equal(t, true, opts.has(ReadMeta))
	assert.Equal(t, true, opts.has(ReadVector))
	assert.Equal(t, true, opts.has(ReadTags))
	assert.Equal(t, true, opts.has(ReadGroup))
}
