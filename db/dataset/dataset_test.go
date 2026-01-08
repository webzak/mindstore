package dataset

import (
	"path/filepath"
	"testing"

	"github.com/webzak/mindstore/internal/testutil/assert"
)

func tempDataset(t *testing.T) *Dataset {
	path := filepath.Join(t.TempDir(), "test.ds")
	ds, err := NewDataset(path, 0, nil, 10)
	assert.NilError(t, err)
	return ds
}

func TestNewDatasetAndClose(t *testing.T) {
	ds := tempDataset(t)
	err := ds.Close()
	assert.NilError(t, err)
}

func TestAppendAndRead(t *testing.T) {
	ds := tempDataset(t)
	defer ds.Close()

	id1, err := ds.Append(NewByteUnit([]byte("one"), 0), nil, nil)
	assert.NilError(t, err)

	id2, err := ds.Append(NewByteUnit([]byte("two"), 0), nil, nil)
	assert.NilError(t, err)

	id3, err := ds.Append(NewByteUnit([]byte("three"), 0), nil, nil)
	assert.NilError(t, err)

	// read in different order
	c3, err := ds.Read(id3)
	assert.NilError(t, err)
	assert.DeepEqual(t, []byte("three"), c3.Data.Blob())

	c1, err := ds.Read(id1)
	assert.NilError(t, err)
	assert.DeepEqual(t, []byte("one"), c1.Data.Blob())

	c2, err := ds.Read(id2)
	assert.NilError(t, err)
	assert.DeepEqual(t, []byte("two"), c2.Data.Blob())
}

func TestUpdate(t *testing.T) {
	ds := tempDataset(t)
	defer ds.Close()

	id, err := ds.Append(NewByteUnit([]byte("original"), 0), nil, nil)
	assert.NilError(t, err)

	err = ds.Update(id, NewByteUnit([]byte("updated"), 0), nil, nil)
	assert.NilError(t, err)

	c, err := ds.Read(id)
	assert.NilError(t, err)
	assert.DeepEqual(t, []byte("updated"), c.Data.Blob())
}

func TestDelete(t *testing.T) {
	ds := tempDataset(t)
	defer ds.Close()

	id, err := ds.Append(NewByteUnit([]byte("to delete"), 0), nil, nil)
	assert.NilError(t, err)

	ok := ds.Delete(id)
	assert.Equal(t, true, ok)

	_, err = ds.Read(id)
	assert.NotNilError(t, err)
}

func TestList(t *testing.T) {
	ds := tempDataset(t)
	defer ds.Close()

	ds.Append(NewByteUnit([]byte("a"), 0), nil, nil)
	ds.Append(NewByteUnit([]byte("b"), 0), nil, nil)
	ds.Append(NewByteUnit([]byte("c"), 0), nil, nil)

	count := 0
	for _, err := range ds.List().Iter() {
		assert.NilError(t, err)
		count++
	}
	assert.Equal(t, 3, count)
}

func TestOptimize(t *testing.T) {
	ds := tempDataset(t)
	defer ds.Close()

	id1, _ := ds.Append(NewByteUnit([]byte("one"), 0), nil, nil)
	id2, _ := ds.Append(NewByteUnit([]byte("two"), 0), nil, nil)
	id3, _ := ds.Append(NewByteUnit([]byte("three"), 0), nil, nil)

	ds.Delete(id2)

	err := ds.Optimize()
	assert.NilError(t, err)

	// Verify IDs are preserved and data is correct
	c1, err := ds.Read(id1)
	assert.NilError(t, err)
	assert.DeepEqual(t, []byte("one"), c1.Data.Blob())

	c3, err := ds.Read(id3)
	assert.NilError(t, err)
	assert.DeepEqual(t, []byte("three"), c3.Data.Blob())

	// Verify deleted record stays deleted
	_, err = ds.Read(id2)
	assert.NotNilError(t, err)
}
