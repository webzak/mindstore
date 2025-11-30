package conv

import (
	"testing"

	"github.com/webzak/mindstore/internal/testutil/assert"
)

func TestVectorConvert(t *testing.T) {
	fs := []float32{0.11, 0.22, 0.33}
	bs := Float32SliceToByte(fs)
	rs := BytesToFloat32Slice(bs)

	// Verify length
	assert.Equal(t, 3, len(rs))

	// Verify values are correct
	assert.DeepEqual(t, fs, rs)

	// Verify that slices share memory (unsafe conversion)
	for i := range fs {
		faddr := &fs[i]
		baddr := &rs[i]
		if faddr != baddr {
			t.Fatalf("Value address with index %d %x is not equal expected %x", faddr, rs[i], baddr)
		}
	}
}

func TestBytesToFloat32SliceSafe(t *testing.T) {
	fs := []float32{1.5, 2.5, 3.5, 4.5}
	bs := Float32SliceToByte(fs)
	rs := BytesToFloat32SliceSafe(bs)

	// Verify length
	assert.Equal(t, 4, len(rs))

	// Verify values are correct
	assert.DeepEqual(t, fs, rs)

	// Verify that the result slice is independent (doesn't share memory)
	// by checking that addresses are different
	for i := range fs {
		faddr := &fs[i]
		raddr := &rs[i]
		if faddr == raddr {
			t.Fatalf("Value address with index %d should be different (safe copy), but both are %p", i, faddr)
		}
	}

	// Additional check: modifying the byte slice shouldn't affect the result
	originalValue := rs[0]
	for i := range bs[:Float32Size] {
		bs[i] = 0
	}
	assert.Equal(t, originalValue, rs[0])
}

func TestInt64Convert(t *testing.T) {
	v := int64(4394823094832)
	bs := make([]byte, Int64Size)
	Int64ToBytes(v, bs)
	rs := BytesToInt64(bs)
	assert.Equal(t, v, rs)
}

func TestInt32Convert(t *testing.T) {
	v := int32(433094832)
	bs := make([]byte, Int32Size)
	Int32ToBytes(v, bs)
	rs := BytesToInt32(bs)
	assert.Equal(t, v, rs)
}
