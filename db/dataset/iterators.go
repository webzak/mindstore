package dataset

import "iter"

// DataIterator returns an iterator over dataset records that yields (index, data) pairs.
// This uses Go 1.23's iter.Seq2 for idiomatic range-over-function iteration.
// It internally uses index.Iterator() to get data positions and data.Read to retrieve actual data.
// Only records with actual data set (offset >= 0 and size > 0) are yielded; empty records are skipped.
// The data includes the DataDescriptor as the first byte, followed by the actual data.
//
// Example usage:
//
//	for idx, data := range ds.DataIterator() {
//	    descriptor := data[0]
//	    actualData := data[1:]
//	    // use idx, descriptor and actualData
//	}
func (ds *Dataset) DataIterator() iter.Seq2[int, []byte] {
	return func(yield func(int, []byte) bool) {
		ds.mu.Lock()
		defer ds.mu.Unlock()

		if ds.closed {
			return
		}

		for idx, row := range ds.index.Iterator() {
			// Skip records with no data set (offset -1 or size 0)
			if row.Offset < 0 || row.Size <= 0 {
				continue
			}

			// Read actual data using offset and size from index row
			rawData, err := ds.data.Read(row.Offset, row.Size)
			if err != nil {
				// On error, stop iteration
				return
			}

			// Prepend descriptor as first byte
			data := make([]byte, len(rawData)+1)
			data[0] = row.DataDescriptor
			copy(data[1:], rawData)

			// Yield the index and data
			if !yield(idx, data) {
				return
			}
		}
	}
}

// MetaDataIterator returns an iterator over dataset metadata records that yields (index, metadata) pairs.
// This uses Go 1.23's iter.Seq2 for idiomatic range-over-function iteration.
// It internally uses index.Iterator() to get metadata positions and meta.Read to retrieve actual metadata.
// Only records with actual metadata set (metaOffset >= 0 and metaSize > 0) are yielded; empty records are skipped.
// The metadata includes the MetaDataDescriptor as the first byte, followed by the actual metadata.
//
// Example usage:
//
//	for idx, meta := range ds.MetaDataIterator() {
//	    descriptor := meta[0]
//	    actualMeta := meta[1:]
//	    // use idx, descriptor and actualMeta
//	}
func (ds *Dataset) MetaDataIterator() iter.Seq2[int, []byte] {
	return func(yield func(int, []byte) bool) {
		ds.mu.Lock()
		defer ds.mu.Unlock()

		if ds.closed {
			return
		}

		for idx, row := range ds.index.Iterator() {
			// Skip records with no metadata set (offset -1 or size 0)
			if row.MetaOffset < 0 || row.MetaSize <= 0 {
				continue
			}

			// Read actual metadata using offset and size from index row
			rawMeta, err := ds.meta.Read(row.MetaOffset, row.MetaSize)
			if err != nil {
				// On error, stop iteration
				return
			}

			// Prepend descriptor as first byte
			meta := make([]byte, len(rawMeta)+1)
			meta[0] = row.MetaDataDescriptor
			copy(meta[1:], rawMeta)

			// Yield the index and metadata
			if !yield(idx, meta) {
				return
			}
		}
	}
}

// VectorsIterator returns an iterator over dataset vectors that yields (index, vector) pairs.
// This uses Go 1.23's iter.Seq2 for idiomatic range-over-function iteration.
// It internally reuses vectors.Iterator() to iterate over all vectors.
//
// Example usage:
//
//	for idx, vector := range ds.VectorsIterator() {
//	    // use idx and vector
//	}
func (ds *Dataset) VectorsIterator() iter.Seq2[int, []float32] {
	return func(yield func(int, []float32) bool) {
		ds.mu.Lock()
		defer ds.mu.Unlock()

		if ds.closed {
			return
		}

		for idx, vector := range ds.vectors.Iterator() {
			if !yield(idx, vector) {
				return
			}
		}
	}
}
