package data

import (
	"path/filepath"
	"testing"

	"github.com/webzak/mindstore/internal/testutil/assert"
)

func TestNew(t *testing.T) {
	t.Run("creates new data storage successfully", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "test.data")

		d, err := New(path, Options{MaxAppendBufferSize: 1024})
		assert.NilError(t, err)
		assert.NotNil(t, d, "data should not be nil")
		assert.Equal(t, int64(0), d.persistedSize)
		assert.Equal(t, int64(1024), d.maxAppendBufferSize)
		assert.Equal(t, 0, len(d.appendBuffer))
		assert.Equal(t, 0, len(d.bufferOffsets))
		assert.Equal(t, true, d.IsPersisted())

		err = d.Close()
		assert.NilError(t, err)
	})

	t.Run("opens existing data storage", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "test.data")

		// Create initial storage and write data
		d1, err := New(path, Options{MaxAppendBufferSize: 0})
		assert.NilError(t, err)

		data := []byte("existing data")
		offset, length, err := d1.Append(data)
		assert.NilError(t, err)
		assert.Equal(t, int64(0), offset)
		assert.Equal(t, int64(len(data)), length)

		err = d1.Close()
		assert.NilError(t, err)

		// Reopen and verify persisted size
		d2, err := New(path, Options{MaxAppendBufferSize: 1024})
		assert.NilError(t, err)
		assert.Equal(t, int64(len(data)), d2.persistedSize)

		err = d2.Close()
		assert.NilError(t, err)
	})

	t.Run("handles zero buffer size", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "test.data")

		d, err := New(path, Options{MaxAppendBufferSize: 0})
		assert.NilError(t, err)
		assert.Equal(t, int64(0), d.maxAppendBufferSize)

		err = d.Close()
		assert.NilError(t, err)
	})
}

func TestAppend(t *testing.T) {
	t.Run("appends data to buffer", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "test.data")

		d, err := New(path, Options{MaxAppendBufferSize: 1024})
		assert.NilError(t, err)
		defer d.Close()

		data := []byte("test data")
		offset, length, err := d.Append(data)
		assert.NilError(t, err)
		assert.Equal(t, int64(0), offset)
		assert.Equal(t, int64(len(data)), length)
		assert.Equal(t, len(data), len(d.appendBuffer))
		assert.Equal(t, 1, len(d.bufferOffsets))
		assert.Equal(t, false, d.IsPersisted())
	})

	t.Run("appends multiple records to buffer", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "test.data")

		d, err := New(path, Options{MaxAppendBufferSize: 1024})
		assert.NilError(t, err)
		defer d.Close()

		data1 := []byte("first")
		offset1, length1, err := d.Append(data1)
		assert.NilError(t, err)
		assert.Equal(t, int64(0), offset1)
		assert.Equal(t, int64(5), length1)

		data2 := []byte("second")
		offset2, length2, err := d.Append(data2)
		assert.NilError(t, err)
		assert.Equal(t, int64(5), offset2)
		assert.Equal(t, int64(6), length2)

		assert.Equal(t, 11, len(d.appendBuffer))
		assert.Equal(t, 2, len(d.bufferOffsets))
	})

	t.Run("handles empty data with sentinel offset", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "test.data")

		d, err := New(path, Options{MaxAppendBufferSize: 1024})
		assert.NilError(t, err)
		defer d.Close()

		offset, length, err := d.Append([]byte{})
		assert.NilError(t, err)
		assert.Equal(t, int64(-1), offset)
		assert.Equal(t, int64(0), length)
		assert.Equal(t, 0, len(d.appendBuffer))
	})

	t.Run("handles nil data with sentinel offset", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "test.data")

		d, err := New(path, Options{MaxAppendBufferSize: 1024})
		assert.NilError(t, err)
		defer d.Close()

		offset, length, err := d.Append(nil)
		assert.NilError(t, err)
		assert.Equal(t, int64(-1), offset)
		assert.Equal(t, int64(0), length)
	})

	t.Run("flushes buffer when max size exceeded", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "test.data")

		d, err := New(path, Options{MaxAppendBufferSize: 10})
		assert.NilError(t, err)
		defer d.Close()

		// First append fits in buffer
		data1 := []byte("12345")
		offset1, _, err := d.Append(data1)
		assert.NilError(t, err)
		assert.Equal(t, int64(0), offset1)
		assert.Equal(t, 5, len(d.appendBuffer))

		// Second append exceeds buffer, should trigger flush
		data2 := []byte("67890ABC")
		offset2, length2, err := d.Append(data2)
		assert.NilError(t, err)
		assert.Equal(t, int64(5), offset2)
		assert.Equal(t, int64(8), length2)

		// Buffer should now contain only the second record
		assert.Equal(t, 8, len(d.appendBuffer))
		assert.Equal(t, int64(5), d.persistedSize)
	})

	t.Run("writes directly when buffer size is zero", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "test.data")

		d, err := New(path, Options{MaxAppendBufferSize: 0})
		assert.NilError(t, err)
		defer d.Close()

		data := []byte("immediate write")
		offset, length, err := d.Append(data)
		assert.NilError(t, err)
		assert.Equal(t, int64(0), offset)
		assert.Equal(t, int64(len(data)), length)
		assert.Equal(t, 0, len(d.appendBuffer))
		assert.Equal(t, int64(len(data)), d.persistedSize)
		assert.Equal(t, true, d.IsPersisted())
	})

	t.Run("writes large record directly when larger than buffer", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "test.data")

		d, err := New(path, Options{MaxAppendBufferSize: 10})
		assert.NilError(t, err)
		defer d.Close()

		// Add small record to buffer first
		small := []byte("tiny")
		offset1, _, err := d.Append(small)
		assert.NilError(t, err)
		assert.Equal(t, int64(0), offset1)

		// Large record exceeds buffer size, should flush and write directly
		large := []byte("this is a very large record that exceeds buffer size")
		offset2, length2, err := d.Append(large)
		assert.NilError(t, err)
		assert.Equal(t, int64(4), offset2)
		assert.Equal(t, int64(len(large)), length2)

		// Buffer should be empty after direct write
		assert.Equal(t, 0, len(d.appendBuffer))
		assert.Equal(t, int64(4+len(large)), d.persistedSize)
		assert.Equal(t, true, d.IsPersisted())
	})
}

func TestFlush(t *testing.T) {
	t.Run("flushes buffered data to storage", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "test.data")

		d, err := New(path, Options{MaxAppendBufferSize: 1024})
		assert.NilError(t, err)
		defer d.Close()

		data := []byte("buffered data")
		_, _, err = d.Append(data)
		assert.NilError(t, err)
		assert.Equal(t, false, d.IsPersisted())

		err = d.Flush()
		assert.NilError(t, err)
		assert.Equal(t, 0, len(d.appendBuffer))
		assert.Equal(t, 0, len(d.bufferOffsets))
		assert.Equal(t, int64(len(data)), d.persistedSize)
		assert.Equal(t, true, d.IsPersisted())
	})

	t.Run("flush with empty buffer does nothing", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "test.data")

		d, err := New(path, Options{MaxAppendBufferSize: 1024})
		assert.NilError(t, err)
		defer d.Close()

		err = d.Flush()
		assert.NilError(t, err)
		assert.Equal(t, int64(0), d.persistedSize)
	})

	t.Run("multiple flushes", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "test.data")

		d, err := New(path, Options{MaxAppendBufferSize: 1024})
		assert.NilError(t, err)
		defer d.Close()

		// First batch
		d.Append([]byte("first"))
		err = d.Flush()
		assert.NilError(t, err)
		assert.Equal(t, int64(5), d.persistedSize)

		// Second batch
		d.Append([]byte("second"))
		err = d.Flush()
		assert.NilError(t, err)
		assert.Equal(t, int64(11), d.persistedSize)
	})
}

func TestRead(t *testing.T) {
	t.Run("reads persisted data", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "test.data")

		d, err := New(path, Options{MaxAppendBufferSize: 0})
		assert.NilError(t, err)
		defer d.Close()

		data := []byte("persistent data")
		offset, length, err := d.Append(data)
		assert.NilError(t, err)

		read, err := d.Read(offset, length)
		assert.NilError(t, err)
		assert.DeepEqual(t, data, read)
	})

	t.Run("reads buffered data", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "test.data")

		d, err := New(path, Options{MaxAppendBufferSize: 1024})
		assert.NilError(t, err)
		defer d.Close()

		data := []byte("buffered data")
		offset, length, err := d.Append(data)
		assert.NilError(t, err)

		read, err := d.Read(offset, length)
		assert.NilError(t, err)
		assert.DeepEqual(t, data, read)
	})

	t.Run("reads multiple records from buffer", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "test.data")

		d, err := New(path, Options{MaxAppendBufferSize: 1024})
		assert.NilError(t, err)
		defer d.Close()

		data1 := []byte("first")
		offset1, length1, err := d.Append(data1)
		assert.NilError(t, err)

		data2 := []byte("second")
		offset2, length2, err := d.Append(data2)
		assert.NilError(t, err)

		read1, err := d.Read(offset1, length1)
		assert.NilError(t, err)
		assert.DeepEqual(t, data1, read1)

		read2, err := d.Read(offset2, length2)
		assert.NilError(t, err)
		assert.DeepEqual(t, data2, read2)
	})

	t.Run("reads data spanning persisted and buffered", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "test.data")

		d, err := New(path, Options{MaxAppendBufferSize: 1024})
		assert.NilError(t, err)
		defer d.Close()

		// Write and flush first record
		data1 := []byte("persisted")
		offset1, length1, err := d.Append(data1)
		assert.NilError(t, err)
		err = d.Flush()
		assert.NilError(t, err)

		// Write second record to buffer
		data2 := []byte("buffered")
		offset2, length2, err := d.Append(data2)
		assert.NilError(t, err)

		// Read both
		read1, err := d.Read(offset1, length1)
		assert.NilError(t, err)
		assert.DeepEqual(t, data1, read1)

		read2, err := d.Read(offset2, length2)
		assert.NilError(t, err)
		assert.DeepEqual(t, data2, read2)
	})

	t.Run("handles sentinel offset for empty data", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "test.data")

		d, err := New(path, Options{MaxAppendBufferSize: 1024})
		assert.NilError(t, err)
		defer d.Close()

		read, err := d.Read(-1, 0)
		assert.NilError(t, err)
		assert.DeepEqual(t, []byte{}, read)
	})

	t.Run("returns error when reading beyond buffer", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "test.data")

		d, err := New(path, Options{MaxAppendBufferSize: 1024})
		assert.NilError(t, err)
		defer d.Close()

		data := []byte("short")
		offset, _, err := d.Append(data)
		assert.NilError(t, err)

		// Try to read more data than exists
		_, err = d.Read(offset, 100)
		assert.NotNilError(t, err)
	})

	t.Run("buffer isolation - read returns copy", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "test.data")

		d, err := New(path, Options{MaxAppendBufferSize: 1024})
		assert.NilError(t, err)
		defer d.Close()

		data := []byte("original")
		offset, length, err := d.Append(data)
		assert.NilError(t, err)

		read, err := d.Read(offset, length)
		assert.NilError(t, err)

		// Mutate the returned slice
		read[0] = 'X'

		// Read again and verify original data unchanged
		read2, err := d.Read(offset, length)
		assert.NilError(t, err)
		assert.DeepEqual(t, data, read2)
	})

	t.Run("reuses reader file descriptor", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "test.data")

		d, err := New(path, Options{MaxAppendBufferSize: 0})
		assert.NilError(t, err)
		defer d.Close()

		// Write multiple records
		data1 := []byte("first")
		offset1, length1, err := d.Append(data1)
		assert.NilError(t, err)

		data2 := []byte("second")
		offset2, length2, err := d.Append(data2)
		assert.NilError(t, err)

		// First read should initialize readerFD
		assert.Equal(t, true, d.readerFD == nil)
		_, err = d.Read(offset1, length1)
		assert.NilError(t, err)
		assert.Equal(t, false, d.readerFD == nil)

		// Second read should reuse same FD
		readerFD := d.readerFD
		_, err = d.Read(offset2, length2)
		assert.NilError(t, err)
		assert.Equal(t, true, d.readerFD == readerFD)
	})
}

func TestIsPersisted(t *testing.T) {
	t.Run("returns true when buffer is empty", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "test.data")

		d, err := New(path, Options{MaxAppendBufferSize: 1024})
		assert.NilError(t, err)
		defer d.Close()

		assert.Equal(t, true, d.IsPersisted())
	})

	t.Run("returns false when buffer has data", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "test.data")

		d, err := New(path, Options{MaxAppendBufferSize: 1024})
		assert.NilError(t, err)
		defer d.Close()

		d.Append([]byte("buffered"))
		assert.Equal(t, false, d.IsPersisted())
	})

	t.Run("returns true after flush", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "test.data")

		d, err := New(path, Options{MaxAppendBufferSize: 1024})
		assert.NilError(t, err)
		defer d.Close()

		d.Append([]byte("buffered"))
		assert.Equal(t, false, d.IsPersisted())

		d.Flush()
		assert.Equal(t, true, d.IsPersisted())
	})
}

func TestClose(t *testing.T) {
	t.Run("closes successfully", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "test.data")

		d, err := New(path, Options{MaxAppendBufferSize: 1024})
		assert.NilError(t, err)

		err = d.Close()
		assert.NilError(t, err)
	})

	t.Run("flushes buffered data on close", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "test.data")

		d, err := New(path, Options{MaxAppendBufferSize: 1024})
		assert.NilError(t, err)

		data := []byte("unflushed")
		d.Append(data)
		assert.Equal(t, false, d.IsPersisted())

		err = d.Close()
		assert.NilError(t, err)

		// Reopen and verify data was persisted
		d2, err := New(path, Options{MaxAppendBufferSize: 1024})
		assert.NilError(t, err)
		defer d2.Close()

		assert.Equal(t, int64(len(data)), d2.persistedSize)
	})

	t.Run("closes reader file descriptor", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "test.data")

		d, err := New(path, Options{MaxAppendBufferSize: 0})
		assert.NilError(t, err)

		// Write and read to open readerFD
		data := []byte("test")
		offset, length, err := d.Append(data)
		assert.NilError(t, err)

		_, err = d.Read(offset, length)
		assert.NilError(t, err)
		assert.Equal(t, false, d.readerFD == nil)

		err = d.Close()
		assert.NilError(t, err)
		assert.Equal(t, true, d.readerFD == nil)
	})

	t.Run("handles close with no reader", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "test.data")

		d, err := New(path, Options{MaxAppendBufferSize: 1024})
		assert.NilError(t, err)

		// Close without ever opening reader
		err = d.Close()
		assert.NilError(t, err)
	})
}

func TestReplace(t *testing.T) {
	t.Run("returns error for invalid offset - zero", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "test.data")

		d, err := New(path, Options{MaxAppendBufferSize: 1024})
		assert.NilError(t, err)
		defer d.Close()

		// Append some data
		d.Append([]byte("original data"))

		// Try to replace at offset 0
		err = d.Replace([]byte("new"), 0)
		assert.NotNilError(t, err)
	})

	t.Run("returns error for invalid offset - negative", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "test.data")

		d, err := New(path, Options{MaxAppendBufferSize: 1024})
		assert.NilError(t, err)
		defer d.Close()

		// Append some data
		d.Append([]byte("original data"))

		// Try to replace at negative offset
		err = d.Replace([]byte("new"), -5)
		assert.NotNilError(t, err)
	})

	t.Run("returns error for offset beyond total size", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "test.data")

		d, err := New(path, Options{MaxAppendBufferSize: 1024})
		assert.NilError(t, err)
		defer d.Close()

		// Append some data
		d.Append([]byte("original data"))

		// Try to replace beyond the data
		err = d.Replace([]byte("new"), 1000)
		assert.NotNilError(t, err)
	})

	t.Run("replaces data in append buffer - fits completely", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "test.data")

		d, err := New(path, Options{MaxAppendBufferSize: 1024})
		assert.NilError(t, err)
		defer d.Close()

		// Append data to buffer
		data := []byte("0123456789")
		offset, _, err := d.Append(data)
		assert.NilError(t, err)
		assert.Equal(t, int64(0), offset)

		// Replace part of the buffered data
		replacement := []byte("ABCD")
		err = d.Replace(replacement, 3)
		assert.NilError(t, err)

		// Verify the data was replaced in buffer
		expected := []byte("012ABCD789")
		assert.DeepEqual(t, expected, d.appendBuffer)

		// Verify reading returns the replaced data
		read, err := d.Read(0, 10)
		assert.NilError(t, err)
		assert.DeepEqual(t, expected, read)
	})

	t.Run("replaces data in append buffer - near start of buffer", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "test.data")

		d, err := New(path, Options{MaxAppendBufferSize: 1024})
		assert.NilError(t, err)
		defer d.Close()

		// Append data to buffer
		data := []byte("original")
		offset, _, err := d.Append(data)
		assert.NilError(t, err)

		// Replace near the beginning of buffered data (offset must be > 0)
		replacement := []byte("NEW")
		err = d.Replace(replacement, offset+1)
		assert.NilError(t, err)

		// Verify the replacement
		expected := []byte("oNEWinal")
		read, err := d.Read(0, 8)
		assert.NilError(t, err)
		assert.DeepEqual(t, expected, read)
	})

	t.Run("replaces data in append buffer - at end of buffer", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "test.data")

		d, err := New(path, Options{MaxAppendBufferSize: 1024})
		assert.NilError(t, err)
		defer d.Close()

		// Append data to buffer
		data := []byte("original")
		d.Append(data)

		// Replace at the end of buffered data
		replacement := []byte("END")
		err = d.Replace(replacement, 5)
		assert.NilError(t, err)

		// Verify the replacement
		expected := []byte("origiEND")
		read, err := d.Read(0, 8)
		assert.NilError(t, err)
		assert.DeepEqual(t, expected, read)
	})

	t.Run("flushes and replaces in storage when replacement doesn't fit in buffer", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "test.data")

		d, err := New(path, Options{MaxAppendBufferSize: 1024})
		assert.NilError(t, err)
		defer d.Close()

		// Append data to buffer
		data := []byte("0123456789")
		d.Append(data)

		// Try to replace data that extends beyond buffer
		replacement := []byte("ABCDEFGHIJK")
		err = d.Replace(replacement, 5)
		assert.NilError(t, err)

		// Buffer should be flushed
		assert.Equal(t, 0, len(d.appendBuffer))
		assert.Equal(t, true, d.IsPersisted())

		// Verify the data was replaced in storage
		read, err := d.Read(0, 16)
		assert.NilError(t, err)
		expected := []byte("01234ABCDEFGHIJK")
		assert.DeepEqual(t, expected, read)
	})

	t.Run("replaces data in persisted storage", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "test.data")

		d, err := New(path, Options{MaxAppendBufferSize: 1024})
		assert.NilError(t, err)
		defer d.Close()

		// Append and flush data
		data := []byte("original persisted data")
		offset, _, err := d.Append(data)
		assert.NilError(t, err)
		err = d.Flush()
		assert.NilError(t, err)

		// Replace part of the persisted data
		// "original persisted data" -> "original REPLACED data"
		// Replace 8 bytes starting at position 9 ("persiste" -> "REPLACED")
		replacement := []byte("REPLACED")
		err = d.Replace(replacement, offset+9)
		assert.NilError(t, err)

		// Verify the replacement
		// "original " (9 bytes) + "REPLACED" (8 bytes) + "d data" (6 bytes) = 23 bytes
		read, err := d.Read(0, int64(len(data)))
		assert.NilError(t, err)
		expected := []byte("original REPLACEDd data")
		assert.DeepEqual(t, expected, read)
	})

	t.Run("replaces data spanning persisted and buffer boundary", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "test.data")

		d, err := New(path, Options{MaxAppendBufferSize: 1024})
		assert.NilError(t, err)
		defer d.Close()

		// Write and flush first part
		data1 := []byte("persisted_")
		d.Append(data1)
		err = d.Flush()
		assert.NilError(t, err)

		// Write second part to buffer
		data2 := []byte("buffered")
		d.Append(data2)

		// Try to replace across the boundary - should flush and replace in storage
		// Total: "persisted_buffered" (18 bytes)
		// Replace 5 bytes at position 8: "d_buf" -> "XXXXX"
		replacement := []byte("XXXXX")
		err = d.Replace(replacement, 8)
		assert.NilError(t, err)

		// Should have flushed
		assert.Equal(t, 0, len(d.appendBuffer))

		// Verify the replacement
		// "persiste" (8) + "XXXXX" (5) + "fered" (5) = 18 bytes
		read, err := d.Read(0, 18)
		assert.NilError(t, err)
		expected := []byte("persisteXXXXXfered")
		assert.DeepEqual(t, expected, read)
	})

	t.Run("replaces most of buffered data", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "test.data")

		d, err := New(path, Options{MaxAppendBufferSize: 1024})
		assert.NilError(t, err)
		defer d.Close()

		// Append data to buffer
		data := []byte("replace_me")
		offset, _, err := d.Append(data)
		assert.NilError(t, err)

		// Replace most of the buffer (offset must be > 0, so start at offset+1)
		replacement := []byte("ompletely")
		err = d.Replace(replacement, offset+1)
		assert.NilError(t, err)

		// Verify
		read, err := d.Read(0, 10)
		assert.NilError(t, err)
		expected := []byte("rompletely")
		assert.DeepEqual(t, expected, read)
	})

	t.Run("multiple replacements in buffer", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "test.data")

		d, err := New(path, Options{MaxAppendBufferSize: 1024})
		assert.NilError(t, err)
		defer d.Close()

		// Append data to buffer
		data := []byte("0123456789")
		d.Append(data)

		// First replacement
		err = d.Replace([]byte("AA"), 2)
		assert.NilError(t, err)

		// Second replacement
		err = d.Replace([]byte("BB"), 6)
		assert.NilError(t, err)

		// Verify both replacements
		expected := []byte("01AA45BB89")
		read, err := d.Read(0, 10)
		assert.NilError(t, err)
		assert.DeepEqual(t, expected, read)
	})

	t.Run("replace after multiple appends in buffer", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "test.data")

		d, err := New(path, Options{MaxAppendBufferSize: 1024})
		assert.NilError(t, err)
		defer d.Close()

		// Multiple appends
		d.Append([]byte("first"))
		d.Append([]byte("second"))
		d.Append([]byte("third"))

		// Replace in the middle record
		replacement := []byte("XXX")
		err = d.Replace(replacement, 5)
		assert.NilError(t, err)

		// Verify
		read, err := d.Read(0, 16)
		assert.NilError(t, err)
		expected := []byte("firstXXXondthird")
		assert.DeepEqual(t, expected, read)
	})

	t.Run("replace with zero-length data", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "test.data")

		d, err := New(path, Options{MaxAppendBufferSize: 1024})
		assert.NilError(t, err)
		defer d.Close()

		// Append data
		data := []byte("original")
		d.Append(data)

		// Replace with zero-length data (should work, just copies nothing)
		err = d.Replace([]byte{}, 5)
		assert.NilError(t, err)

		// Data should remain unchanged
		read, err := d.Read(0, 8)
		assert.NilError(t, err)
		assert.DeepEqual(t, data, read)
	})

	t.Run("replace persisted data after reopening", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "test.data")

		// First session - write and close
		{
			d, err := New(path, Options{MaxAppendBufferSize: 100})
			assert.NilError(t, err)

			d.Append([]byte("original data"))
			err = d.Close()
			assert.NilError(t, err)
		}

		// Second session - reopen and replace
		{
			d, err := New(path, Options{MaxAppendBufferSize: 100})
			assert.NilError(t, err)
			defer d.Close()

			// Replace part of the persisted data
			// "original data" (13 bytes) -> replace 8 bytes at position 9
			// "original " (9) + "REPLACED" (8) would go past end, so we can only replace 4 bytes
			err = d.Replace([]byte("REPL"), 9)
			assert.NilError(t, err)

			// Verify
			read, err := d.Read(0, 13)
			assert.NilError(t, err)
			expected := []byte("original REPL")
			assert.DeepEqual(t, expected, read)
		}
	})
}

func TestIntegration(t *testing.T) {
	t.Run("complex workflow with mixed operations", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "test.data")

		d, err := New(path, Options{MaxAppendBufferSize: 50})
		assert.NilError(t, err)
		defer d.Close()

		// Append multiple records
		offsets := make([]int64, 0)
		lengths := make([]int64, 0)
		dataItems := [][]byte{
			[]byte("record 1"),
			[]byte("record 2"),
			[]byte("record 3"),
			[]byte("record 4 is longer"),
			[]byte(""),
			[]byte("record 6"),
		}

		for _, data := range dataItems {
			offset, length, err := d.Append(data)
			assert.NilError(t, err)
			offsets = append(offsets, offset)
			lengths = append(lengths, length)
		}

		// Manually flush
		err = d.Flush()
		assert.NilError(t, err)

		// Append more after flush
		data7 := []byte("record 7 after flush")
		offset7, length7, err := d.Append(data7)
		assert.NilError(t, err)

		// Read all records
		for i, data := range dataItems {
			if len(data) == 0 {
				assert.Equal(t, int64(-1), offsets[i])
				continue
			}
			read, err := d.Read(offsets[i], lengths[i])
			assert.NilError(t, err)
			assert.DeepEqual(t, data, read)
		}

		// Read the post-flush record
		read7, err := d.Read(offset7, length7)
		assert.NilError(t, err)
		assert.DeepEqual(t, data7, read7)
	})

	t.Run("persistence across reopens", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "test.data")

		data1 := []byte("persistent record 1")
		data2 := []byte("persistent record 2")
		var offset1, offset2, length1, length2 int64

		// First session
		{
			d, err := New(path, Options{MaxAppendBufferSize: 100})
			assert.NilError(t, err)

			offset1, length1, err = d.Append(data1)
			assert.NilError(t, err)

			offset2, length2, err = d.Append(data2)
			assert.NilError(t, err)

			err = d.Close()
			assert.NilError(t, err)
		}

		// Second session
		{
			d, err := New(path, Options{MaxAppendBufferSize: 100})
			assert.NilError(t, err)
			defer d.Close()

			// Read data from previous session
			read1, err := d.Read(offset1, length1)
			assert.NilError(t, err)
			assert.DeepEqual(t, data1, read1)

			read2, err := d.Read(offset2, length2)
			assert.NilError(t, err)
			assert.DeepEqual(t, data2, read2)

			// Append new data
			data3 := []byte("new session data")
			offset3, length3, err := d.Append(data3)
			assert.NilError(t, err)

			read3, err := d.Read(offset3, length3)
			assert.NilError(t, err)
			assert.DeepEqual(t, data3, read3)
		}
	})

	t.Run("large data handling", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "test.data")

		d, err := New(path, Options{MaxAppendBufferSize: 1024})
		assert.NilError(t, err)
		defer d.Close()

		// Create large data (10KB)
		largeData := make([]byte, 10*1024)
		for i := range largeData {
			largeData[i] = byte(i % 256)
		}

		offset, length, err := d.Append(largeData)
		assert.NilError(t, err)
		assert.Equal(t, int64(len(largeData)), length)

		// Should be written directly and persisted
		assert.Equal(t, true, d.IsPersisted())

		read, err := d.Read(offset, length)
		assert.NilError(t, err)
		assert.DeepEqual(t, largeData, read)
	})
}
