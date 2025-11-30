package dataset

import (
	"testing"

	"github.com/webzak/mindstore/internal/testutil/assert"
)

func TestDefaultOptions(t *testing.T) {
	opts := DefaultOptions()

	assert.Equal(t, DefaultMaxDataAppendBufferSize, opts.MaxDataAppendBufferSize)
	assert.Equal(t, DefaultMaxMetaDataAppendBufferSize, opts.MaxMetaDataAppendBufferSize)
	assert.Equal(t, DefaultMaxIndexAppendBufferSize, opts.MaxIndexAppendBufferSize)
	assert.Equal(t, DefaultVectorSize, opts.VectorSize)
	assert.Equal(t, DefaultMaxVectorBufferSize, opts.MaxVectorBufferSize)
	assert.Equal(t, DefaultMaxVectorAppendBufferSize, opts.MaxVectorAppendBufferSize)
}

func TestDefaultConstants(t *testing.T) {
	// Verify default constants have expected values
	assert.Equal(t, 2<<16, DefaultMaxDataAppendBufferSize)
	assert.Equal(t, 2<<14, DefaultMaxMetaDataAppendBufferSize)
	assert.Equal(t, 64, DefaultMaxIndexAppendBufferSize)
	assert.Equal(t, 768, DefaultVectorSize)
	assert.Equal(t, 64, DefaultMaxVectorBufferSize)
	assert.Equal(t, 64, DefaultMaxVectorAppendBufferSize)
}

func TestCustomOptions(t *testing.T) {
	tmpDir := t.TempDir()

	customOpts := Options{
		MaxDataAppendBufferSize:     1024,
		MaxMetaDataAppendBufferSize: 512,
		MaxIndexAppendBufferSize:    16,
		VectorSize:                  384,
		MaxVectorBufferSize:         32,
		MaxVectorAppendBufferSize:   32,
	}

	// Open dataset with custom options
	ds, err := Open(tmpDir, "test_custom_opts", customOpts)
	assert.NilError(t, err)
	defer ds.Close()

	// Verify dataset opens successfully with custom options
	assert.NotNil(t, ds, "dataset should be created with custom options")
}

func TestOptionsWithZeroValues(t *testing.T) {
	tmpDir := t.TempDir()

	zeroOpts := Options{
		MaxDataAppendBufferSize:     0,
		MaxMetaDataAppendBufferSize: 0,
		MaxIndexAppendBufferSize:    0,
		VectorSize:                  0,
		MaxVectorBufferSize:         0,
		MaxVectorAppendBufferSize:   0,
	}

	// Open dataset with zero options - underlying components should handle this
	ds, err := Open(tmpDir, "test_zero_opts", zeroOpts)
	assert.NilError(t, err)
	defer ds.Close()

	assert.NotNil(t, ds, "dataset should be created even with zero options")
}

func TestOptionsWithLargeValues(t *testing.T) {
	tmpDir := t.TempDir()

	largeOpts := Options{
		MaxDataAppendBufferSize:     1 << 20, // 1MB
		MaxMetaDataAppendBufferSize: 1 << 18, // 256KB
		MaxIndexAppendBufferSize:    1024,
		VectorSize:                  2048,
		MaxVectorBufferSize:         1024,
		MaxVectorAppendBufferSize:   512,
	}

	ds, err := Open(tmpDir, "test_large_opts", largeOpts)
	assert.NilError(t, err)
	defer ds.Close()

	assert.NotNil(t, ds, "dataset should be created with large options")

	// Verify it can still append data
	item := &Item{
		Data:           []byte("test data"),
		DataDescriptor: 1,
	}
	_, err = ds.Append(item)
	assert.NilError(t, err)
}

func TestOptionsVectorSize(t *testing.T) {
	tmpDir := t.TempDir()

	tests := []struct {
		name       string
		vectorSize int
		testVector []float32
	}{
		{
			name:       "small vector size",
			vectorSize: 3,
			testVector: []float32{1.0, 2.0, 3.0},
		},
		{
			name:       "default vector size",
			vectorSize: 768,
			testVector: make([]float32, 768),
		},
		{
			name:       "large vector size",
			vectorSize: 1536,
			testVector: make([]float32, 1536),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts := DefaultOptions()
			opts.VectorSize = tt.vectorSize

			ds, err := Open(tmpDir, "test_vector_size_"+tt.name, opts)
			assert.NilError(t, err)
			defer ds.Close()

			// Initialize test vector
			for i := range tt.testVector {
				tt.testVector[i] = float32(i)
			}

			// Append item with vector
			item := &Item{
				Data:           []byte("test"),
				DataDescriptor: 1,
				Vector:         tt.testVector,
			}
			id, err := ds.Append(item)
			assert.NilError(t, err)

			// Read back and verify
			retrieved, err := ds.Read(id, ReadVector)
			assert.NilError(t, err)
			assert.DeepEqual(t, tt.testVector, retrieved.Vector)
		})
	}
}

func TestOptionsBufferSizes(t *testing.T) {
	tmpDir := t.TempDir()

	// Test with small buffer sizes to force more frequent flushes
	smallBufferOpts := Options{
		MaxDataAppendBufferSize:     128,
		MaxMetaDataAppendBufferSize: 64,
		MaxIndexAppendBufferSize:    2,
		VectorSize:                  3,
		MaxVectorBufferSize:         2,
		MaxVectorAppendBufferSize:   2,
	}

	ds, err := Open(tmpDir, "test_small_buffers", smallBufferOpts)
	assert.NilError(t, err)
	defer ds.Close()

	// Append multiple items to trigger buffer flushes
	for i := 0; i < 10; i++ {
		item := &Item{
			Data:           []byte("test data"),
			Meta:           []byte("test meta"),
			DataDescriptor: 1,
			MetaDescriptor: 1,
			Vector:         []float32{1.0, 2.0, 3.0},
		}
		_, err := ds.Append(item)
		assert.NilError(t, err)
	}

	// Verify all items are accessible
	assert.Equal(t, 10, ds.Count())
}

func TestOptionsIndependence(t *testing.T) {
	tmpDir := t.TempDir()

	// Create two datasets with different options
	opts1 := Options{
		MaxDataAppendBufferSize:     1024,
		MaxMetaDataAppendBufferSize: 512,
		MaxIndexAppendBufferSize:    32,
		VectorSize:                  384,
		MaxVectorBufferSize:         32,
		MaxVectorAppendBufferSize:   32,
	}

	ds1, err := Open(tmpDir, "test_opts_1", opts1)
	assert.NilError(t, err)
	defer ds1.Close()

	opts2 := Options{
		MaxDataAppendBufferSize:     2048,
		MaxMetaDataAppendBufferSize: 1024,
		MaxIndexAppendBufferSize:    64,
		VectorSize:                  768,
		MaxVectorBufferSize:         64,
		MaxVectorAppendBufferSize:   64,
	}

	ds2, err := Open(tmpDir, "test_opts_2", opts2)
	assert.NilError(t, err)
	defer ds2.Close()

	// Both datasets should work independently
	item1 := &Item{Data: []byte("data1"), DataDescriptor: 1}
	_, err = ds1.Append(item1)
	assert.NilError(t, err)

	item2 := &Item{Data: []byte("data2"), DataDescriptor: 2}
	_, err = ds2.Append(item2)
	assert.NilError(t, err)

	assert.Equal(t, 1, ds1.Count())
	assert.Equal(t, 1, ds2.Count())
}
