package dataset

import (
	"testing"

	"github.com/webzak/mindstore/internal/testutil/assert"
)

func TestAppend(t *testing.T) {
	tmpDir := t.TempDir()

	tests := []struct {
		name       string
		item       *Item
		vectorSize int
	}{
		{
			name: "append data only",
			item: &Item{
				Data:           []byte("test data"),
				DataDescriptor: 1,
			},
		},
		{
			name: "append with metadata",
			item: &Item{
				Data:           []byte("test data"),
				Meta:           []byte("test metadata"),
				DataDescriptor: 1,
				MetaDescriptor: 2,
			},
		},
		{
			name: "append with vector",
			item: &Item{
				Data:           []byte("test data"),
				DataDescriptor: 1,
				Vector:         []float32{1.0, 2.0, 3.0, 4.0},
			},
			vectorSize: 4,
		},
		{
			name: "append with tags",
			item: &Item{
				Data:           []byte("test data"),
				DataDescriptor: 1,
				Tags:           []string{"tag1", "tag2", "tag3"},
			},
		},

		{
			name: "append complete item",
			item: &Item{
				Data:           []byte("complete data"),
				Meta:           []byte("complete metadata"),
				DataDescriptor: 3,
				MetaDescriptor: 4,
				Flags:          5,
				Vector:         []float32{1.0, 2.0, 3.0},
				Tags:           []string{"complete", "test"},
			},
			vectorSize: 3,
		},

		{
			name: "append empty data",
			item: &Item{
				Data:           []byte{},
				DataDescriptor: 1,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts := DefaultOptions()
			if tt.vectorSize > 0 {
				opts.VectorSize = tt.vectorSize
			}

			ds, err := Open(tmpDir, "test_append_"+tt.name, opts)
			assert.NilError(t, err)
			defer ds.Close()

			id, err := ds.Append(tt.item)
			assert.NilError(t, err)

			// Verify ID is valid
			if id < 0 {
				t.Errorf("expected valid ID, got %d", id)
			}

			// Verify item.ID was updated
			assert.Equal(t, id, tt.item.ID)

			// Verify count increased
			expectedCount := 1
			assert.Equal(t, expectedCount, ds.Count())
		})
	}
}

func TestAppendMultipleItems(t *testing.T) {
	tmpDir := t.TempDir()
	ds, err := Open(tmpDir, "test_append_multiple", DefaultOptions())
	assert.NilError(t, err)
	defer ds.Close()

	numItems := 100
	ids := make([]int, numItems)

	// Append multiple items
	for i := 0; i < numItems; i++ {
		item := &Item{
			Data:           []byte("test data"),
			DataDescriptor: uint8(i % 256),
		}
		id, err := ds.Append(item)
		assert.NilError(t, err)
		ids[i] = id
	}

	// Verify all IDs are unique and sequential
	for i, id := range ids {
		assert.Equal(t, i, id)
	}

	// Verify count
	assert.Equal(t, numItems, ds.Count())
}

func TestAppendWithVectorEmptySlice(t *testing.T) {
	tmpDir := t.TempDir()
	ds, err := Open(tmpDir, "test_append_empty_vector", DefaultOptions())
	assert.NilError(t, err)
	defer ds.Close()

	// Append with empty vector slice (should not error)
	item := &Item{
		Data:           []byte("test data"),
		DataDescriptor: 1,
		Vector:         []float32{},
	}
	id, err := ds.Append(item)
	assert.NilError(t, err)

	// Append with nil vector (should not error)
	item2 := &Item{
		Data:           []byte("test data 2"),
		DataDescriptor: 1,
		Vector:         nil,
	}
	id2, err := ds.Append(item2)
	assert.NilError(t, err)
	assert.Equal(t, id+1, id2)
}

func TestAppendWithEmptyTags(t *testing.T) {
	tmpDir := t.TempDir()
	ds, err := Open(tmpDir, "test_append_empty_tags", DefaultOptions())
	assert.NilError(t, err)
	defer ds.Close()

	// Append with empty tags slice
	item := &Item{
		Data:           []byte("test data"),
		DataDescriptor: 1,
		Tags:           []string{},
	}
	_, err = ds.Append(item)
	assert.NilError(t, err)

	// Append with nil tags
	item2 := &Item{
		Data:           []byte("test data 2"),
		DataDescriptor: 1,
		Tags:           nil,
	}
	_, err = ds.Append(item2)
	assert.NilError(t, err)
}

func TestAppendWithGroupNilOrZero(t *testing.T) {
	tmpDir := t.TempDir()
	ds, err := Open(tmpDir, "test_append_group", DefaultOptions())
	assert.NilError(t, err)
	defer ds.Close()

	// Append with no group (GroupID = 0)
	item := &Item{
		Data:           []byte("test data"),
		DataDescriptor: 1,
		GroupID:        0,
	}
	_, err = ds.Append(item)
	assert.NilError(t, err)

	// Append with group ID 0 (should not assign to group)
	item2 := &Item{
		Data:           []byte("test data 2"),
		DataDescriptor: 1,
		GroupID:        0,
		GroupPlace:     0,
	}
	_, err = ds.Append(item2)
	assert.NilError(t, err)
}

func TestAppendSequentialIDs(t *testing.T) {
	tmpDir := t.TempDir()
	ds, err := Open(tmpDir, "test_append_sequential", DefaultOptions())
	assert.NilError(t, err)
	defer ds.Close()

	// Append items and verify IDs are sequential starting from 0
	for i := 0; i < 10; i++ {
		item := &Item{
			Data:           []byte("test"),
			DataDescriptor: 1,
		}
		id, err := ds.Append(item)
		assert.NilError(t, err)
		assert.Equal(t, i, id)
	}
}

func TestAppendAfterFlush(t *testing.T) {
	tmpDir := t.TempDir()
	ds, err := Open(tmpDir, "test_append_after_flush", DefaultOptions())
	assert.NilError(t, err)
	defer ds.Close()

	// Append some items
	for i := 0; i < 5; i++ {
		item := &Item{Data: []byte("test")}
		_, err := ds.Append(item)
		assert.NilError(t, err)
	}

	// Flush
	assert.NilError(t, ds.Flush())

	// Append more items
	for i := 5; i < 10; i++ {
		item := &Item{Data: []byte("test")}
		id, err := ds.Append(item)
		assert.NilError(t, err)
		assert.Equal(t, i, id)
	}

	assert.Equal(t, 10, ds.Count())
}
