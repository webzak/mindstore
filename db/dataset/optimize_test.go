package dataset

import (
	"testing"

	"github.com/webzak/mindstore/internal/testutil/assert"
)

// TestOptimizeBasic tests basic optimization with deletions
func TestOptimizeBasic(t *testing.T) {
	tmpDir := t.TempDir()
	ds, err := Open(tmpDir, "test_optimize", DefaultOptions())
	assert.NilError(t, err)
	defer ds.Close()

	// Add 10 records
	items := make([]Item, 10)
	for i := 0; i < 10; i++ {
		items[i] = Item{
			Data:           []byte("test data " + string(rune('0'+i))),
			Meta:           []byte("test meta " + string(rune('0'+i))),
			DataDescriptor: 1,
			MetaDescriptor: 2,
		}
		result, err := ds.Append(items[i])
		assert.NilError(t, err)
		assert.Equal(t, i, result.ID)
	}

	// Delete records at indices 2, 5, 8
	assert.NilError(t, ds.Delete(2))
	assert.NilError(t, ds.Delete(5))
	assert.NilError(t, ds.Delete(8))

	// Verify count before optimization
	assert.Equal(t, 10, ds.Count())

	// Optimize
	assert.NilError(t, ds.Optimize())

	// Verify count after optimization (should be 7)
	assert.Equal(t, 7, ds.Count())

	// Verify remaining records have correct data
	expectedIndices := []int{0, 1, 3, 4, 6, 7, 9}
	for newIdx, oldIdx := range expectedIndices {
		item, err := ds.Read(newIdx, ReadData|ReadMeta)
		assert.NilError(t, err)
		if string(item.Data) != string(items[oldIdx].Data) {
			t.Errorf("data mismatch at index %d: got %s, want %s", newIdx, item.Data, items[oldIdx].Data)
		}
		if string(item.Meta) != string(items[oldIdx].Meta) {
			t.Errorf("meta mismatch at index %d: got %s, want %s", newIdx, item.Meta, items[oldIdx].Meta)
		}
	}
}

// TestOptimizeWithGaps tests optimization with gaps in storage
func TestOptimizeWithGaps(t *testing.T) {
	tmpDir := t.TempDir()
	ds, err := Open(tmpDir, "test_gaps", DefaultOptions())
	assert.NilError(t, err)
	defer ds.Close()

	// Add records with varying sizes to create potential gaps
	largeData := make([]byte, 1024)
	for i := range largeData {
		largeData[i] = byte(i % 256)
	}

	for i := 0; i < 20; i++ {
		var data []byte
		if i%2 == 0 {
			data = largeData
		} else {
			data = []byte("small")
		}
		_, err := ds.Append(Item{Data: data})
		assert.NilError(t, err)
	}

	// Delete several records
	for i := 0; i < 20; i += 3 {
		assert.NilError(t, ds.Delete(i))
	}

	// Flush to ensure file is written
	assert.NilError(t, ds.Flush())

	// Optimize
	assert.NilError(t, ds.Optimize())

	// Verify data integrity
	count := ds.Count()
	for i := 0; i < count; i++ {
		item, err := ds.Read(i, ReadData)
		assert.NilError(t, err)
		if len(item.Data) == 0 {
			t.Errorf("expected non-empty data at index %d", i)
		}
	}
}

// TestOptimizeWithTags tests optimization with tags cleanup
func TestOptimizeWithTags(t *testing.T) {
	tmpDir := t.TempDir()
	ds, err := Open(tmpDir, "test_tags", DefaultOptions())
	assert.NilError(t, err)
	defer ds.Close()

	// Add records with tags
	for i := 0; i < 5; i++ {
		result, err := ds.Append(Item{Data: []byte("test")})
		assert.NilError(t, err)
		assert.NilError(t, ds.AddTags(result.ID, "tag1", "tag2"))
	}

	// Delete records 1 and 3
	assert.NilError(t, ds.Delete(1))
	assert.NilError(t, ds.Delete(3))

	// Optimize - this will renumber IDs
	assert.NilError(t, ds.Optimize())

	// After optimization, IDs are renumbered from 0
	// We had 5 records, deleted 2, so now we have 3 records with IDs 0, 1, 2
	// Verify count
	assert.Equal(t, 3, ds.Count())

	// Verify tags still exist and are mapped to new IDs
	ids, err := ds.GetIDsByTag("tag1")
	assert.NilError(t, err)
	assert.Equal(t, 3, len(ids)) // Should have 3 records left with tags

	// Verify all remaining records have tags (they should be at new IDs 0, 1, 2)
	for i := 0; i < 3; i++ {
		tags, err := ds.GetTagsByID(i)
		assert.NilError(t, err)
		assert.Equal(t, 2, len(tags))
	}
}

// TestOptimizeWithGroups tests optimization with groups cleanup
func TestOptimizeWithGroups(t *testing.T) {
	tmpDir := t.TempDir()
	ds, err := Open(tmpDir, "test_groups", DefaultOptions())
	assert.NilError(t, err)
	defer ds.Close()

	// Add records - first one creates a new group
	result0, err := ds.Append(Item{
		Data:    []byte("test0"),
		GroupID: -1, // Create new group
	})
	assert.NilError(t, err)
	groupID := result0.GroupID

	// Add more members to the group
	_, err = ds.Append(Item{
		Data:       []byte("test1"),
		GroupID:    groupID,
		GroupPlace: 1,
	})
	assert.NilError(t, err)

	_, err = ds.Append(Item{
		Data:       []byte("test2"),
		GroupID:    groupID,
		GroupPlace: 2,
	})
	assert.NilError(t, err)

	// Delete record at index 1
	assert.NilError(t, ds.Delete(1))

	// Optimize
	assert.NilError(t, ds.Optimize())

	// After optimization, should have 2 records
	assert.Equal(t, 2, ds.Count())
}

// TestOptimizeEmpty tests optimization on empty dataset
func TestOptimizeEmpty(t *testing.T) {
	tmpDir := t.TempDir()
	ds, err := Open(tmpDir, "test_empty", DefaultOptions())
	assert.NilError(t, err)
	defer ds.Close()

	// Optimize empty dataset
	assert.NilError(t, ds.Optimize())

	// Verify count is still 0
	assert.Equal(t, 0, ds.Count())
}

// TestOptimizeNoMarkedRecords tests optimization with no deletions
func TestOptimizeNoMarkedRecords(t *testing.T) {
	tmpDir := t.TempDir()
	ds, err := Open(tmpDir, "test_no_marked", DefaultOptions())
	assert.NilError(t, err)
	defer ds.Close()

	// Add records
	for i := 0; i < 5; i++ {
		_, err := ds.Append(Item{Data: []byte("test")})
		assert.NilError(t, err)
	}

	// Optimize without any deletions
	assert.NilError(t, ds.Optimize())

	// Verify all data remains intact
	assert.Equal(t, 5, ds.Count())
	for i := 0; i < 5; i++ {
		item, err := ds.Read(i, ReadData)
		assert.NilError(t, err)
		if string(item.Data) != "test" {
			t.Errorf("expected data 'test', got '%s'", item.Data)
		}
	}
}

// TestOptimizePersistence tests that data is persisted before and after optimization
func TestOptimizePersistence(t *testing.T) {
	tmpDir := t.TempDir()
	ds, err := Open(tmpDir, "test_persist", DefaultOptions())
	assert.NilError(t, err)

	// Add records
	for i := 0; i < 5; i++ {
		_, err := ds.Append(Item{Data: []byte("test")})
		assert.NilError(t, err)
	}

	// Delete some records
	assert.NilError(t, ds.Delete(1))
	assert.NilError(t, ds.Delete(3))

	// Flush to ensure persisted before optimization
	assert.NilError(t, ds.Flush())
	assert.Equal(t, true, ds.IsPersisted())

	// Optimize
	assert.NilError(t, ds.Optimize())

	// Verify persisted after optimization
	assert.Equal(t, true, ds.IsPersisted())

	// Close dataset
	assert.NilError(t, ds.Close())

	// Reopen and verify data
	ds, err = Open(tmpDir, "test_persist", DefaultOptions())
	assert.NilError(t, err)
	defer ds.Close()

	assert.Equal(t, 3, ds.Count())
}

// TestOptimizeReopenAfter tests reopening dataset after optimization
func TestOptimizeReopenAfter(t *testing.T) {
	tmpDir := t.TempDir()

	// Create dataset and add data
	ds, err := Open(tmpDir, "test_reopen", DefaultOptions())
	assert.NilError(t, err)

	testData := []string{"data0", "data1", "data2", "data3", "data4"}
	for i := 0; i < 5; i++ {
		_, err := ds.Append(Item{Data: []byte(testData[i])})
		assert.NilError(t, err)
	}

	// Delete records
	assert.NilError(t, ds.Delete(1))
	assert.NilError(t, ds.Delete(3))

	// Optimize
	assert.NilError(t, ds.Optimize())

	expectedCount := 3
	assert.Equal(t, expectedCount, ds.Count())

	// Close dataset
	assert.NilError(t, ds.Close())

	// Reopen dataset
	ds, err = Open(tmpDir, "test_reopen", DefaultOptions())
	assert.NilError(t, err)
	defer ds.Close()

	// Verify count
	assert.Equal(t, expectedCount, ds.Count())

	// Verify data integrity (should have records 0, 2, 4 from original)
	expectedData := []string{"data0", "data2", "data4"}
	for i := 0; i < expectedCount; i++ {
		item, err := ds.Read(i, ReadData)
		assert.NilError(t, err)
		if string(item.Data) != expectedData[i] {
			t.Errorf("expected data '%s', got '%s'", expectedData[i], item.Data)
		}
	}
}

// TestOptimizeOnClosedDataset tests that Optimize returns error on closed dataset
func TestOptimizeOnClosedDataset(t *testing.T) {
	tmpDir := t.TempDir()
	ds, err := Open(tmpDir, "test_closed", DefaultOptions())
	assert.NilError(t, err)

	// Close dataset
	assert.NilError(t, ds.Close())

	// Try to optimize closed dataset
	err = ds.Optimize()
	assert.ErrorIs(t, ErrDatasetClosed, err)
}
