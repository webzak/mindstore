package dataset

import (
	"testing"

	"github.com/webzak/mindstore/internal/testutil/assert"
)

func TestDataIterator(t *testing.T) {
	tmpDir := t.TempDir()
	ds, err := Open(tmpDir, "test_data_iterator", DefaultOptions())
	assert.NilError(t, err)
	defer ds.Close()

	// Add test items
	testData := []struct {
		data       []byte
		descriptor uint8
	}{
		{[]byte("data1"), 1},
		{[]byte("data2"), 2},
		{[]byte("data3"), 3},
		{[]byte("data4"), 4},
		{[]byte("data5"), 5},
	}

	for _, td := range testData {
		item := Item{
			Data:           td.data,
			DataDescriptor: td.descriptor,
		}
		_, err := ds.Append(item)
		assert.NilError(t, err)
	}

	// Iterate and verify
	count := 0
	for idx, data := range ds.DataIterator() {
		if idx >= len(testData) {
			t.Fatalf("unexpected index %d", idx)
		}

		// First byte should be descriptor
		assert.Equal(t, testData[idx].descriptor, data[0])

		// Rest should be the actual data
		actualData := data[1:]
		assert.DeepEqual(t, testData[idx].data, actualData)

		count++
	}

	assert.Equal(t, len(testData), count)
}

func TestDataIteratorEmpty(t *testing.T) {
	tmpDir := t.TempDir()
	ds, err := Open(tmpDir, "test_data_iterator_empty", DefaultOptions())
	assert.NilError(t, err)
	defer ds.Close()

	// Iterate over empty dataset
	count := 0
	for range ds.DataIterator() {
		count++
	}

	assert.Equal(t, 0, count)
}

func TestDataIteratorEarlyBreak(t *testing.T) {
	tmpDir := t.TempDir()
	ds, err := Open(tmpDir, "test_data_iterator_break", DefaultOptions())
	assert.NilError(t, err)
	defer ds.Close()

	// Add items
	for i := 0; i < 10; i++ {
		item := Item{
			Data:           []byte("test"),
			DataDescriptor: 1,
		}
		_, err := ds.Append(item)
		assert.NilError(t, err)
	}

	// Iterate but break early
	count := 0
	for range ds.DataIterator() {
		count++
		if count == 5 {
			break
		}
	}

	assert.Equal(t, 5, count)
}

func TestMetaDataIterator(t *testing.T) {
	tmpDir := t.TempDir()
	ds, err := Open(tmpDir, "test_metadata_iterator", DefaultOptions())
	assert.NilError(t, err)
	defer ds.Close()

	// Add test items with metadata
	testMeta := []struct {
		data       []byte
		meta       []byte
		descriptor uint8
	}{
		{[]byte("data1"), []byte("meta1"), 10},
		{[]byte("data2"), []byte("meta2"), 20},
		{[]byte("data3"), []byte("meta3"), 30},
	}

	for _, tm := range testMeta {
		item := Item{
			Data:           tm.data,
			Meta:           tm.meta,
			DataDescriptor: 1,
			MetaDescriptor: tm.descriptor,
		}
		_, err := ds.Append(item)
		assert.NilError(t, err)
	}

	// Iterate and verify
	count := 0
	for idx, meta := range ds.MetaDataIterator() {
		if idx >= len(testMeta) {
			t.Fatalf("unexpected index %d", idx)
		}

		// First byte should be descriptor
		assert.Equal(t, testMeta[idx].descriptor, meta[0])

		// Rest should be the actual metadata
		actualMeta := meta[1:]
		assert.DeepEqual(t, testMeta[idx].meta, actualMeta)

		count++
	}

	assert.Equal(t, len(testMeta), count)
}

func TestMetaDataIteratorEmpty(t *testing.T) {
	tmpDir := t.TempDir()
	ds, err := Open(tmpDir, "test_metadata_iterator_empty", DefaultOptions())
	assert.NilError(t, err)
	defer ds.Close()

	// Iterate over empty dataset
	count := 0
	for range ds.MetaDataIterator() {
		count++
	}

	assert.Equal(t, 0, count)
}

func TestMetaDataIteratorSkipsItemsWithoutMeta(t *testing.T) {
	tmpDir := t.TempDir()
	ds, err := Open(tmpDir, "test_metadata_iterator_skip", DefaultOptions())
	assert.NilError(t, err)
	defer ds.Close()

	// Add items, some with metadata, some without
	item1 := Item{
		Data:           []byte("data1"),
		Meta:           []byte("meta1"),
		DataDescriptor: 1,
		MetaDescriptor: 1,
	}
	_, err = ds.Append(item1)
	assert.NilError(t, err)

	item2 := Item{
		Data:           []byte("data2"),
		DataDescriptor: 1,
		// No metadata
	}
	_, err = ds.Append(item2)
	assert.NilError(t, err)

	item3 := Item{
		Data:           []byte("data3"),
		Meta:           []byte("meta3"),
		DataDescriptor: 1,
		MetaDescriptor: 3,
	}
	_, err = ds.Append(item3)
	assert.NilError(t, err)

	// Should only iterate over items with metadata
	count := 0
	indices := []int{}
	for idx := range ds.MetaDataIterator() {
		indices = append(indices, idx)
		count++
	}

	// Should skip item at index 1
	assert.Equal(t, 2, count)
	assert.Equal(t, 0, indices[0])
	assert.Equal(t, 2, indices[1])
}

func TestVectorsIterator(t *testing.T) {
	tmpDir := t.TempDir()
	opts := DefaultOptions()
	opts.VectorSize = 3
	ds, err := Open(tmpDir, "test_vectors_iterator", opts)
	assert.NilError(t, err)
	defer ds.Close()

	// Add test items with vectors
	testVectors := [][]float32{
		{1.0, 2.0, 3.0},
		{4.0, 5.0, 6.0},
		{7.0, 8.0, 9.0},
	}

	for _, vec := range testVectors {
		item := Item{
			Data:           []byte("test"),
			DataDescriptor: 1,
			Vector:         vec,
		}
		_, err := ds.Append(item)
		assert.NilError(t, err)
	}

	// Iterate and verify
	count := 0
	for idx, vector := range ds.VectorsIterator() {
		if idx >= len(testVectors) {
			t.Fatalf("unexpected index %d", idx)
		}

		assert.DeepEqual(t, testVectors[idx], vector)
		count++
	}

	assert.Equal(t, len(testVectors), count)
}

func TestVectorsIteratorEmpty(t *testing.T) {
	tmpDir := t.TempDir()
	ds, err := Open(tmpDir, "test_vectors_iterator_empty", DefaultOptions())
	assert.NilError(t, err)
	defer ds.Close()

	// Iterate over empty dataset
	count := 0
	for range ds.VectorsIterator() {
		count++
	}

	assert.Equal(t, 0, count)
}

func TestVectorsIteratorEarlyBreak(t *testing.T) {
	tmpDir := t.TempDir()
	opts := DefaultOptions()
	opts.VectorSize = 3
	ds, err := Open(tmpDir, "test_vectors_iterator_break", opts)
	assert.NilError(t, err)
	defer ds.Close()

	// Add items with vectors
	for i := 0; i < 10; i++ {
		item := Item{
			Data:           []byte("test"),
			DataDescriptor: 1,
			Vector:         []float32{float32(i), float32(i + 1), float32(i + 2)},
		}
		_, err := ds.Append(item)
		assert.NilError(t, err)
	}

	// Iterate but break early
	count := 0
	for range ds.VectorsIterator() {
		count++
		if count == 3 {
			break
		}
	}

	assert.Equal(t, 3, count)
}

func TestIteratorsAfterFlush(t *testing.T) {
	tmpDir := t.TempDir()
	opts := DefaultOptions()
	opts.VectorSize = 3
	ds, err := Open(tmpDir, "test_iterators_flush", opts)
	assert.NilError(t, err)
	defer ds.Close()

	// Add items
	numItems := 5
	for i := 0; i < numItems; i++ {
		item := Item{
			Data:           []byte("data"),
			Meta:           []byte("meta"),
			DataDescriptor: 1,
			MetaDescriptor: 1,
			Vector:         []float32{1.0, 2.0, 3.0},
		}
		_, err := ds.Append(item)
		assert.NilError(t, err)
	}

	// Flush
	assert.NilError(t, ds.Flush())

	// Verify iterators still work after flush
	dataCount := 0
	for range ds.DataIterator() {
		dataCount++
	}
	assert.Equal(t, numItems, dataCount)

	metaCount := 0
	for range ds.MetaDataIterator() {
		metaCount++
	}
	assert.Equal(t, numItems, metaCount)

	vectorCount := 0
	for range ds.VectorsIterator() {
		vectorCount++
	}
	assert.Equal(t, numItems, vectorCount)
}

func TestIteratorsOnClosedDataset(t *testing.T) {
	tmpDir := t.TempDir()
	opts := DefaultOptions()
	opts.VectorSize = 3
	ds, err := Open(tmpDir, "test_iterators_closed", opts)
	assert.NilError(t, err)

	// Add an item
	item := Item{
		Data:           []byte("test"),
		Meta:           []byte("meta"),
		DataDescriptor: 1,
		MetaDescriptor: 1,
		Vector:         []float32{1.0, 2.0, 3.0},
	}
	_, err = ds.Append(item)
	assert.NilError(t, err)

	// Close dataset
	ds.Close()

	// Iterators should not panic on closed dataset, just return nothing
	count := 0
	for range ds.DataIterator() {
		count++
	}
	assert.Equal(t, 0, count)

	count = 0
	for range ds.MetaDataIterator() {
		count++
	}
	assert.Equal(t, 0, count)

	count = 0
	for range ds.VectorsIterator() {
		count++
	}
	assert.Equal(t, 0, count)
}

func TestDataIteratorWithEmptyData(t *testing.T) {
	tmpDir := t.TempDir()
	ds, err := Open(tmpDir, "test_data_iterator_empty_data", DefaultOptions())
	assert.NilError(t, err)
	defer ds.Close()

	// Add item with empty data
	item := Item{
		Data:           []byte{},
		DataDescriptor: 1,
	}
	_, err = ds.Append(item)
	assert.NilError(t, err)

	// Iterator should NOT yield items with empty data (as per DataIterator documentation)
	// Empty data results in offset=-1, size=0, which is skipped
	count := 0
	for range ds.DataIterator() {
		count++
	}
	assert.Equal(t, 0, count)
}

func TestMultipleIteratorsConcurrently(t *testing.T) {
	tmpDir := t.TempDir()
	opts := DefaultOptions()
	opts.VectorSize = 3
	ds, err := Open(tmpDir, "test_multiple_iterators", opts)
	assert.NilError(t, err)
	defer ds.Close()

	// Add items
	numItems := 5
	for i := 0; i < numItems; i++ {
		item := Item{
			Data:           []byte("data"),
			Meta:           []byte("meta"),
			DataDescriptor: 1,
			MetaDescriptor: 1,
			Vector:         []float32{1.0, 2.0, 3.0},
		}
		_, err := ds.Append(item)
		assert.NilError(t, err)
	}

	// Note: Due to mutex locking, iterators can't truly run concurrently
	// But we can verify that running them sequentially works
	dataCount := 0
	for range ds.DataIterator() {
		dataCount++
	}

	metaCount := 0
	for range ds.MetaDataIterator() {
		metaCount++
	}

	vectorCount := 0
	for range ds.VectorsIterator() {
		vectorCount++
	}

	assert.Equal(t, numItems, dataCount)
	assert.Equal(t, numItems, metaCount)
	assert.Equal(t, numItems, vectorCount)
}

func TestVectorsIteratorSparseVectors(t *testing.T) {
	tmpDir := t.TempDir()
	opts := DefaultOptions()
	opts.VectorSize = 3
	ds, err := Open(tmpDir, "test_vectors_iterator_sparse", opts)
	assert.NilError(t, err)
	defer ds.Close()

	// Add items where only some have vectors
	// This tests that VectorsIterator returns index IDs, not vector positions
	testData := []struct {
		data       []byte
		vector     []float32
		hasVector  bool
		expectedID int
	}{
		{[]byte("item0"), []float32{1.0, 2.0, 3.0}, true, 0},  // Index 0, Vector pos 0
		{[]byte("item1"), nil, false, -1},                     // Index 1, No vector
		{[]byte("item2"), []float32{4.0, 5.0, 6.0}, true, 2},  // Index 2, Vector pos 1
		{[]byte("item3"), nil, false, -1},                     // Index 3, No vector
		{[]byte("item4"), []float32{7.0, 8.0, 9.0}, true, 4},  // Index 4, Vector pos 2
		{[]byte("item5"), []float32{10.0, 11.0, 12.0}, true, 5}, // Index 5, Vector pos 3
	}

	for _, td := range testData {
		item := Item{
			Data:           td.data,
			DataDescriptor: 1,
			Vector:         td.vector,
		}
		_, err := ds.Append(item)
		assert.NilError(t, err)
	}

	// Collect vectors and their indices
	type result struct {
		idx    int
		vector []float32
	}
	var results []result

	for idx, vector := range ds.VectorsIterator() {
		results = append(results, result{idx: idx, vector: vector})
	}

	// Should only get 4 vectors (items with hasVector=true)
	assert.Equal(t, 4, len(results))

	// Verify that we get INDEX IDs, not vector positions
	// The iterator should return indices 0, 2, 4, 5 (not 0, 1, 2, 3)
	expectedIndices := []int{0, 2, 4, 5}
	expectedVectors := [][]float32{
		{1.0, 2.0, 3.0},
		{4.0, 5.0, 6.0},
		{7.0, 8.0, 9.0},
		{10.0, 11.0, 12.0},
	}

	for i, res := range results {
		assert.Equal(t, expectedIndices[i], res.idx)
		assert.DeepEqual(t, expectedVectors[i], res.vector)
	}
}

func TestVectorsIteratorAfterOptimize(t *testing.T) {
	tmpDir := t.TempDir()
	opts := DefaultOptions()
	opts.VectorSize = 3
	ds, err := Open(tmpDir, "test_vectors_iterator_optimize", opts)
	assert.NilError(t, err)
	defer ds.Close()

	// Add items with vectors
	vectors := [][]float32{
		{1.0, 2.0, 3.0},
		{4.0, 5.0, 6.0},
		{7.0, 8.0, 9.0},
		{10.0, 11.0, 12.0},
	}

	for _, vec := range vectors {
		item := Item{
			Data:           []byte("test"),
			DataDescriptor: 1,
			Vector:         vec,
		}
		_, err := ds.Append(item)
		assert.NilError(t, err)
	}

	// Delete items at indices 1 and 2
	err = ds.Delete(1)
	assert.NilError(t, err)
	err = ds.Delete(2)
	assert.NilError(t, err)

	// Optimize - this should compact the dataset
	err = ds.Optimize()
	assert.NilError(t, err)

	// After optimization, only 2 items remain (original indices 0 and 3)
	// They should now be at indices 0 and 1
	type result struct {
		idx    int
		vector []float32
	}
	var results []result

	for idx, vector := range ds.VectorsIterator() {
		results = append(results, result{idx: idx, vector: vector})
	}

	assert.Equal(t, 2, len(results))

	// After optimization, indices should be 0 and 1 (compacted)
	assert.Equal(t, 0, results[0].idx)
	assert.Equal(t, 1, results[1].idx)

	// Vectors should be from original items 0 and 3
	assert.DeepEqual(t, vectors[0], results[0].vector)
	assert.DeepEqual(t, vectors[3], results[1].vector)
}
