package dataset

import (
	"path/filepath"
	"testing"

	"github.com/webzak/mindstore/internal/testutil/assert"
)

func tempDataset(t *testing.T) *Dataset {
	path := filepath.Join(t.TempDir(), "test.ds")
	ds, err := NewDataset(path, nil, 10)
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

	id1, err := ds.Append(ChunkData{Data: []byte("one")})
	assert.NilError(t, err)

	id2, err := ds.Append(ChunkData{Data: []byte("two")})
	assert.NilError(t, err)

	id3, err := ds.Append(ChunkData{Data: []byte("three")})
	assert.NilError(t, err)

	// read in different order
	c3, err := ds.Read(id3)
	assert.NilError(t, err)
	assert.DeepEqual(t, []byte("three"), c3.Data)

	c1, err := ds.Read(id1)
	assert.NilError(t, err)
	assert.DeepEqual(t, []byte("one"), c1.Data)

	c2, err := ds.Read(id2)
	assert.NilError(t, err)
	assert.DeepEqual(t, []byte("two"), c2.Data)
}

func TestUpdate(t *testing.T) {
	ds := tempDataset(t)
	defer ds.Close()

	id, err := ds.Append(ChunkData{Data: []byte("original")})
	assert.NilError(t, err)

	err = ds.Update(id, ChunkData{Data: []byte("updated")})
	assert.NilError(t, err)

	c, err := ds.Read(id)
	assert.NilError(t, err)
	assert.DeepEqual(t, []byte("updated"), c.Data)
}

func TestDelete(t *testing.T) {
	ds := tempDataset(t)
	defer ds.Close()

	id, err := ds.Append(ChunkData{Data: []byte("to delete")})
	assert.NilError(t, err)

	ok := ds.Delete(id)
	assert.Equal(t, true, ok)

	_, err = ds.Read(id)
	assert.NotNilError(t, err)
}

func TestList(t *testing.T) {
	ds := tempDataset(t)
	defer ds.Close()

	ds.Append(ChunkData{Data: []byte("a")})
	ds.Append(ChunkData{Data: []byte("b")})
	ds.Append(ChunkData{Data: []byte("c")})

	count := 0
	for _, err := range ds.List().Iter() {
		assert.NilError(t, err)
		count++
	}
	assert.Equal(t, 3, count)
}
