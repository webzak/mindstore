package dataset

import (
	"testing"

	"github.com/webzak/mindstore/internal/index"
	"github.com/webzak/mindstore/internal/testutil/assert"
)

func TestSetData_InPlaceReplacement(t *testing.T) {
	tmpDir := t.TempDir()
	ds, err := Open(tmpDir, "test_setdata_inplace", DefaultOptions())
	assert.NilError(t, err)
	defer ds.Close()

	// Append initial item with some data
	item := &Item{
		Data:           []byte("original data here"),
		DataDescriptor: 1,
	}
	id, err := ds.Append(item)
	assert.NilError(t, err)

	// Replace with smaller data (should fit in same space)
	newData := []byte("new data")
	newDescriptor := uint8(2)
	err = ds.SetData(id, newData, newDescriptor)
	assert.NilError(t, err)

	// Read back and verify
	readItem, err := ds.Read(id, ReadData)
	assert.NilError(t, err)
	assert.DeepEqual(t, newData, readItem.Data)
	assert.Equal(t, newDescriptor, readItem.DataDescriptor)
}

func TestSetData_AppendScenario(t *testing.T) {
	tmpDir := t.TempDir()
	ds, err := Open(tmpDir, "test_setdata_append", DefaultOptions())
	assert.NilError(t, err)
	defer ds.Close()

	// Append initial item with small data
	item := &Item{
		Data:           []byte("small"),
		DataDescriptor: 1,
	}
	id, err := ds.Append(item)
	assert.NilError(t, err)

	// Replace with much larger data (should append)
	newData := []byte("this is much larger data that won't fit in the original space")
	newDescriptor := uint8(3)
	err = ds.SetData(id, newData, newDescriptor)
	assert.NilError(t, err)

	// Read back and verify
	readItem, err := ds.Read(id, ReadData)
	assert.NilError(t, err)
	assert.DeepEqual(t, newData, readItem.Data)
	assert.Equal(t, newDescriptor, readItem.DataDescriptor)
}

func TestSetData_EmptyData(t *testing.T) {
	tmpDir := t.TempDir()
	ds, err := Open(tmpDir, "test_setdata_empty", DefaultOptions())
	assert.NilError(t, err)
	defer ds.Close()

	// Append initial item
	item := &Item{
		Data:           []byte("some data"),
		DataDescriptor: 1,
	}
	id, err := ds.Append(item)
	assert.NilError(t, err)

	// Set to empty data (nil)
	newDescriptor := uint8(0)
	err = ds.SetData(id, nil, newDescriptor)
	assert.NilError(t, err)

	// Verify index entry has sentinel values
	row, err := ds.index.Get(id)
	assert.NilError(t, err)
	assert.Equal(t, int64(-1), row.Offset)
	assert.Equal(t, int64(0), row.Size)
	assert.Equal(t, newDescriptor, row.DataDescriptor)

	// Read back and verify empty data
	readItem, err := ds.Read(id, ReadData)
	assert.NilError(t, err)
	assert.DeepEqual(t, []byte{}, readItem.Data)
	assert.Equal(t, newDescriptor, readItem.DataDescriptor)
}

func TestSetData_EmptyDataZeroLengthArray(t *testing.T) {
	tmpDir := t.TempDir()
	ds, err := Open(tmpDir, "test_setdata_empty_array", DefaultOptions())
	assert.NilError(t, err)
	defer ds.Close()

	// Append initial item
	item := &Item{
		Data:           []byte("some data"),
		DataDescriptor: 1,
	}
	id, err := ds.Append(item)
	assert.NilError(t, err)

	// Set to empty data (zero-length array)
	newDescriptor := uint8(5)
	err = ds.SetData(id, []byte{}, newDescriptor)
	assert.NilError(t, err)

	// Verify index entry has sentinel values
	row, err := ds.index.Get(id)
	assert.NilError(t, err)
	assert.Equal(t, int64(-1), row.Offset)
	assert.Equal(t, int64(0), row.Size)
	assert.Equal(t, newDescriptor, row.DataDescriptor)
}

func TestSetData_InvalidIndex(t *testing.T) {
	tmpDir := t.TempDir()
	ds, err := Open(tmpDir, "test_setdata_invalid", DefaultOptions())
	assert.NilError(t, err)
	defer ds.Close()

	// Try to set data on non-existent index
	err = ds.SetData(999, []byte("data"), 1)
	assert.ErrorIs(t, index.ErrIndexOutOfRange, err)
}

func TestSetData_ClosedDataset(t *testing.T) {
	tmpDir := t.TempDir()
	ds, err := Open(tmpDir, "test_setdata_closed", DefaultOptions())
	assert.NilError(t, err)

	// Append an item first
	item := &Item{Data: []byte("data")}
	id, err := ds.Append(item)
	assert.NilError(t, err)

	// Close dataset
	ds.Close()

	// Try to set data on closed dataset
	err = ds.SetData(id, []byte("new data"), 1)
	assert.ErrorIs(t, ErrDatasetClosed, err)
}

func TestSetData_DescriptorUpdates(t *testing.T) {
	tmpDir := t.TempDir()
	ds, err := Open(tmpDir, "test_setdata_descriptor", DefaultOptions())
	assert.NilError(t, err)
	defer ds.Close()

	// Append initial item
	item := &Item{
		Data:           []byte("test data"),
		DataDescriptor: 10,
	}
	id, err := ds.Append(item)
	assert.NilError(t, err)

	// Update data with different descriptor
	newDescriptor := uint8(20)
	err = ds.SetData(id, []byte("test data"), newDescriptor)
	assert.NilError(t, err)

	// Verify descriptor changed
	readItem, err := ds.Read(id, ReadData)
	assert.NilError(t, err)
	assert.Equal(t, newDescriptor, readItem.DataDescriptor)
}

func TestSetData_Persistence(t *testing.T) {
	tmpDir := t.TempDir()
	opts := DefaultOptions()
	// Use immediate persistence for predictable behavior
	opts.MaxDataAppendBufferSize = 0
	opts.MaxIndexAppendBufferSize = 0

	ds, err := Open(tmpDir, "test_setdata_persist", opts)
	assert.NilError(t, err)

	// Append initial item
	item := &Item{
		Data:           []byte("original"),
		DataDescriptor: 1,
	}
	id, err := ds.Append(item)
	assert.NilError(t, err)

	// Update data
	newData := []byte("updated")
	err = ds.SetData(id, newData, 2)
	assert.NilError(t, err)

	// Flush to ensure persistence
	err = ds.Flush()
	assert.NilError(t, err)

	// Close and reopen
	ds.Close()

	ds, err = Open(tmpDir, "test_setdata_persist", opts)
	assert.NilError(t, err)
	defer ds.Close()

	// Read and verify data persisted
	readItem, err := ds.Read(id, ReadData)
	assert.NilError(t, err)
	assert.DeepEqual(t, newData, readItem.Data)
	assert.Equal(t, uint8(2), readItem.DataDescriptor)
}

func TestSetData_PreservesOtherFields(t *testing.T) {
	tmpDir := t.TempDir()
	ds, err := Open(tmpDir, "test_setdata_preserve", DefaultOptions())
	assert.NilError(t, err)
	defer ds.Close()

	// Append item with metadata and flags
	item := &Item{
		Data:           []byte("data"),
		DataDescriptor: 1,
		Meta:           []byte("metadata"),
		MetaDescriptor: 2,
		Flags:          index.MarkedForRemoval,
	}
	id, err := ds.Append(item)
	assert.NilError(t, err)

	// Update data only
	err = ds.SetData(id, []byte("new data"), 5)
	assert.NilError(t, err)

	// Verify metadata and flags are preserved
	row, err := ds.index.Get(id)
	assert.NilError(t, err)
	assert.Equal(t, uint8(2), row.MetaDataDescriptor)
	assert.Equal(t, uint8(index.MarkedForRemoval), row.Flags)

	// Verify metadata can still be read
	readItem, err := ds.Read(id, ReadData|ReadMeta)
	assert.NilError(t, err)
	assert.DeepEqual(t, []byte("metadata"), readItem.Meta)
	assert.Equal(t, uint8(2), readItem.MetaDescriptor)
	assert.Equal(t, uint8(index.MarkedForRemoval), readItem.Flags)
}

func TestSetData_ReadModifyWrite(t *testing.T) {
	tmpDir := t.TempDir()
	ds, err := Open(tmpDir, "test_setdata_rmw", DefaultOptions())
	assert.NilError(t, err)
	defer ds.Close()

	// Append initial item
	item := &Item{
		Data:           []byte("first"),
		DataDescriptor: 1,
	}
	id, err := ds.Append(item)
	assert.NilError(t, err)

	// Read-modify-write cycle
	for i := 0; i < 5; i++ {
		// Read current data
		readItem, err := ds.Read(id, ReadData)
		assert.NilError(t, err)

		// Modify data
		newData := append(readItem.Data, []byte("-updated")...)
		newDescriptor := readItem.DataDescriptor + 1

		// Write back
		err = ds.SetData(id, newData, newDescriptor)
		assert.NilError(t, err)
	}

	// Verify final state
	finalItem, err := ds.Read(id, ReadData)
	assert.NilError(t, err)
	assert.DeepEqual(t, []byte("first-updated-updated-updated-updated-updated"), finalItem.Data)
	assert.Equal(t, uint8(6), finalItem.DataDescriptor)
}

func TestSetData_FromEmptyToData(t *testing.T) {
	tmpDir := t.TempDir()
	ds, err := Open(tmpDir, "test_setdata_empty_to_data", DefaultOptions())
	assert.NilError(t, err)
	defer ds.Close()

	// Append item with no data
	item := &Item{
		Data:           nil,
		DataDescriptor: 0,
	}
	id, err := ds.Append(item)
	assert.NilError(t, err)

	// Verify initial state is empty
	row, err := ds.index.Get(id)
	assert.NilError(t, err)
	assert.Equal(t, int64(-1), row.Offset)
	assert.Equal(t, int64(0), row.Size)

	// Set actual data
	newData := []byte("now has data")
	err = ds.SetData(id, newData, 7)
	assert.NilError(t, err)

	// Verify data was set
	readItem, err := ds.Read(id, ReadData)
	assert.NilError(t, err)
	assert.DeepEqual(t, newData, readItem.Data)
	assert.Equal(t, uint8(7), readItem.DataDescriptor)
}

func TestSetMetaData_InPlaceReplacement(t *testing.T) {
	tmpDir := t.TempDir()
	ds, err := Open(tmpDir, "test_setmetadata_inplace", DefaultOptions())
	assert.NilError(t, err)
	defer ds.Close()

	// Append initial item with some metadata
	item := &Item{
		Data:           []byte("data"),
		Meta:           []byte("original meta here"),
		DataDescriptor: 1,
		MetaDescriptor: 1,
	}
	id, err := ds.Append(item)
	assert.NilError(t, err)

	// Replace with smaller metadata (should fit in same space)
	newMeta := []byte("new meta")
	newDescriptor := uint8(2)
	err = ds.SetMetaData(id, newMeta, newDescriptor)
	assert.NilError(t, err)

	// Read back and verify
	readItem, err := ds.Read(id, ReadMeta)
	assert.NilError(t, err)
	assert.DeepEqual(t, newMeta, readItem.Meta)
	assert.Equal(t, newDescriptor, readItem.MetaDescriptor)
}

func TestSetMetaData_AppendScenario(t *testing.T) {
	tmpDir := t.TempDir()
	ds, err := Open(tmpDir, "test_setmetadata_append", DefaultOptions())
	assert.NilError(t, err)
	defer ds.Close()

	// Append initial item with small metadata
	item := &Item{
		Data:           []byte("data"),
		Meta:           []byte("small"),
		DataDescriptor: 1,
		MetaDescriptor: 1,
	}
	id, err := ds.Append(item)
	assert.NilError(t, err)

	// Replace with much larger metadata (should append)
	newMeta := []byte("this is much larger metadata that won't fit in the original space")
	newDescriptor := uint8(3)
	err = ds.SetMetaData(id, newMeta, newDescriptor)
	assert.NilError(t, err)

	// Read back and verify
	readItem, err := ds.Read(id, ReadMeta)
	assert.NilError(t, err)
	assert.DeepEqual(t, newMeta, readItem.Meta)
	assert.Equal(t, newDescriptor, readItem.MetaDescriptor)
}

func TestSetMetaData_EmptyData(t *testing.T) {
	tmpDir := t.TempDir()
	ds, err := Open(tmpDir, "test_setmetadata_empty", DefaultOptions())
	assert.NilError(t, err)
	defer ds.Close()

	// Append initial item
	item := &Item{
		Data:           []byte("data"),
		Meta:           []byte("some metadata"),
		DataDescriptor: 1,
		MetaDescriptor: 1,
	}
	id, err := ds.Append(item)
	assert.NilError(t, err)

	// Set to empty metadata (nil)
	newDescriptor := uint8(0)
	err = ds.SetMetaData(id, nil, newDescriptor)
	assert.NilError(t, err)

	// Verify index entry has sentinel values
	row, err := ds.index.Get(id)
	assert.NilError(t, err)
	assert.Equal(t, int64(-1), row.MetaOffset)
	assert.Equal(t, int64(0), row.MetaSize)
	assert.Equal(t, newDescriptor, row.MetaDataDescriptor)

	// Read back and verify empty metadata
	readItem, err := ds.Read(id, ReadMeta)
	assert.NilError(t, err)
	assert.DeepEqual(t, []byte{}, readItem.Meta)
	assert.Equal(t, newDescriptor, readItem.MetaDescriptor)
}

func TestSetMetaData_EmptyDataZeroLengthArray(t *testing.T) {
	tmpDir := t.TempDir()
	ds, err := Open(tmpDir, "test_setmetadata_empty_array", DefaultOptions())
	assert.NilError(t, err)
	defer ds.Close()

	// Append initial item
	item := &Item{
		Data:           []byte("data"),
		Meta:           []byte("some metadata"),
		DataDescriptor: 1,
		MetaDescriptor: 1,
	}
	id, err := ds.Append(item)
	assert.NilError(t, err)

	// Set to empty metadata (zero-length array)
	newDescriptor := uint8(5)
	err = ds.SetMetaData(id, []byte{}, newDescriptor)
	assert.NilError(t, err)

	// Verify index entry has sentinel values
	row, err := ds.index.Get(id)
	assert.NilError(t, err)
	assert.Equal(t, int64(-1), row.MetaOffset)
	assert.Equal(t, int64(0), row.MetaSize)
	assert.Equal(t, newDescriptor, row.MetaDataDescriptor)
}

func TestSetMetaData_InvalidIndex(t *testing.T) {
	tmpDir := t.TempDir()
	ds, err := Open(tmpDir, "test_setmetadata_invalid", DefaultOptions())
	assert.NilError(t, err)
	defer ds.Close()

	// Try to set metadata on non-existent index
	err = ds.SetMetaData(999, []byte("metadata"), 1)
	assert.ErrorIs(t, index.ErrIndexOutOfRange, err)
}

func TestSetMetaData_ClosedDataset(t *testing.T) {
	tmpDir := t.TempDir()
	ds, err := Open(tmpDir, "test_setmetadata_closed", DefaultOptions())
	assert.NilError(t, err)

	// Append an item first
	item := &Item{
		Data:           []byte("data"),
		Meta:           []byte("metadata"),
		DataDescriptor: 1,
		MetaDescriptor: 1,
	}
	id, err := ds.Append(item)
	assert.NilError(t, err)

	// Close dataset
	ds.Close()

	// Try to set metadata on closed dataset
	err = ds.SetMetaData(id, []byte("new metadata"), 1)
	assert.ErrorIs(t, ErrDatasetClosed, err)
}

func TestSetMetaData_DescriptorUpdates(t *testing.T) {
	tmpDir := t.TempDir()
	ds, err := Open(tmpDir, "test_setmetadata_descriptor", DefaultOptions())
	assert.NilError(t, err)
	defer ds.Close()

	// Append initial item
	item := &Item{
		Data:           []byte("data"),
		Meta:           []byte("test metadata"),
		DataDescriptor: 1,
		MetaDescriptor: 10,
	}
	id, err := ds.Append(item)
	assert.NilError(t, err)

	// Update metadata with different descriptor
	newDescriptor := uint8(20)
	err = ds.SetMetaData(id, []byte("test metadata"), newDescriptor)
	assert.NilError(t, err)

	// Verify descriptor changed
	readItem, err := ds.Read(id, ReadMeta)
	assert.NilError(t, err)
	assert.Equal(t, newDescriptor, readItem.MetaDescriptor)
}

func TestSetMetaData_Persistence(t *testing.T) {
	tmpDir := t.TempDir()
	opts := DefaultOptions()
	// Use immediate persistence for predictable behavior
	opts.MaxDataAppendBufferSize = 0
	opts.MaxMetaDataAppendBufferSize = 0
	opts.MaxIndexAppendBufferSize = 0

	ds, err := Open(tmpDir, "test_setmetadata_persist", opts)
	assert.NilError(t, err)

	// Append initial item
	item := &Item{
		Data:           []byte("data"),
		Meta:           []byte("original"),
		DataDescriptor: 1,
		MetaDescriptor: 1,
	}
	id, err := ds.Append(item)
	assert.NilError(t, err)

	// Update metadata
	newMeta := []byte("updated")
	err = ds.SetMetaData(id, newMeta, 2)
	assert.NilError(t, err)

	// Flush to ensure persistence
	err = ds.Flush()
	assert.NilError(t, err)

	// Close and reopen
	ds.Close()

	ds, err = Open(tmpDir, "test_setmetadata_persist", opts)
	assert.NilError(t, err)
	defer ds.Close()

	// Read and verify metadata persisted
	readItem, err := ds.Read(id, ReadMeta)
	assert.NilError(t, err)
	assert.DeepEqual(t, newMeta, readItem.Meta)
	assert.Equal(t, uint8(2), readItem.MetaDescriptor)
}

func TestSetMetaData_PreservesOtherFields(t *testing.T) {
	tmpDir := t.TempDir()
	ds, err := Open(tmpDir, "test_setmetadata_preserve", DefaultOptions())
	assert.NilError(t, err)
	defer ds.Close()

	// Append item with data and flags
	item := &Item{
		Data:           []byte("data"),
		DataDescriptor: 1,
		Meta:           []byte("metadata"),
		MetaDescriptor: 2,
		Flags:          index.MarkedForRemoval,
	}
	id, err := ds.Append(item)
	assert.NilError(t, err)

	// Update metadata only
	err = ds.SetMetaData(id, []byte("new metadata"), 5)
	assert.NilError(t, err)

	// Verify data and flags are preserved
	row, err := ds.index.Get(id)
	assert.NilError(t, err)
	assert.Equal(t, uint8(1), row.DataDescriptor)
	assert.Equal(t, uint8(index.MarkedForRemoval), row.Flags)

	// Verify data can still be read
	readItem, err := ds.Read(id, ReadData|ReadMeta)
	assert.NilError(t, err)
	assert.DeepEqual(t, []byte("data"), readItem.Data)
	assert.Equal(t, uint8(1), readItem.DataDescriptor)
	assert.Equal(t, uint8(index.MarkedForRemoval), readItem.Flags)
}

func TestSetMetaData_ReadModifyWrite(t *testing.T) {
	tmpDir := t.TempDir()
	ds, err := Open(tmpDir, "test_setmetadata_rmw", DefaultOptions())
	assert.NilError(t, err)
	defer ds.Close()

	// Append initial item
	item := &Item{
		Data:           []byte("data"),
		Meta:           []byte("first"),
		DataDescriptor: 1,
		MetaDescriptor: 1,
	}
	id, err := ds.Append(item)
	assert.NilError(t, err)

	// Read-modify-write cycle
	for i := 0; i < 5; i++ {
		// Read current metadata
		readItem, err := ds.Read(id, ReadMeta)
		assert.NilError(t, err)

		// Modify metadata
		newMeta := append(readItem.Meta, []byte("-updated")...)
		newDescriptor := readItem.MetaDescriptor + 1

		// Write back
		err = ds.SetMetaData(id, newMeta, newDescriptor)
		assert.NilError(t, err)
	}

	// Verify final state
	finalItem, err := ds.Read(id, ReadMeta)
	assert.NilError(t, err)
	assert.DeepEqual(t, []byte("first-updated-updated-updated-updated-updated"), finalItem.Meta)
	assert.Equal(t, uint8(6), finalItem.MetaDescriptor)
}

func TestSetMetaData_FromEmptyToData(t *testing.T) {
	tmpDir := t.TempDir()
	ds, err := Open(tmpDir, "test_setmetadata_empty_to_data", DefaultOptions())
	assert.NilError(t, err)
	defer ds.Close()

	// Append item with no metadata
	item := &Item{
		Data:           []byte("data"),
		Meta:           nil,
		DataDescriptor: 1,
		MetaDescriptor: 0,
	}
	id, err := ds.Append(item)
	assert.NilError(t, err)

	// Verify initial state is empty
	row, err := ds.index.Get(id)
	assert.NilError(t, err)
	assert.Equal(t, int64(-1), row.MetaOffset)
	assert.Equal(t, int64(0), row.MetaSize)

	// Set actual metadata
	newMeta := []byte("now has metadata")
	err = ds.SetMetaData(id, newMeta, 7)
	assert.NilError(t, err)

	// Verify metadata was set
	readItem, err := ds.Read(id, ReadMeta)
	assert.NilError(t, err)
	assert.DeepEqual(t, newMeta, readItem.Meta)
	assert.Equal(t, uint8(7), readItem.MetaDescriptor)
}
