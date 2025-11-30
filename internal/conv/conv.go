package conv

import (
	"encoding/binary"
	"unsafe"
)

const Int64Size = int(unsafe.Sizeof(int64(0)))
const Int32Size = int(unsafe.Sizeof(int32(0)))
const Float32Size = int(unsafe.Sizeof(float32(0)))

// Float32SliceToByte returns byte representation of float32 slice
func Float32SliceToByte(in []float32) []byte {
	if len(in) == 0 {
		return []byte{}
	}
	return unsafe.Slice((*byte)(unsafe.Pointer(&in[0])), len(in)*Float32Size)
}

// BytesToFloat32Slice converts byte slice to float32 slice
// WARNING: The returned slice shares memory with the input byte slice.
// Ensure the byte slice remains alive as long as the float32 slice is in use.
func BytesToFloat32Slice(bytes []byte) []float32 {
	return unsafe.Slice((*float32)(unsafe.Pointer(&bytes[0])), len(bytes)/Float32Size)
}

// BytesToFloat32SliceSafe converts byte slice to float32 slice with a copy
// This creates an independent copy, ensuring no GC issues with the source byte slice
func BytesToFloat32SliceSafe(bytes []byte) []float32 {
	// First do the unsafe conversion
	temp := unsafe.Slice((*float32)(unsafe.Pointer(&bytes[0])), len(bytes)/Float32Size)
	// Then make a copy to ensure independence from the source byte slice
	result := make([]float32, len(temp))
	copy(result, temp)
	return result
}

// Int64ToBytes converts int to byte slice
func Int64ToBytes(value int64, dst []byte) {
	if len(dst) < Int64Size {
		panic("destination size does is less than integer")
	}
	binary.BigEndian.PutUint64(dst, uint64(value))
}

// BytesToInt64 converts byte slice to int64
func BytesToInt64(bytes []byte) int64 {
	v := binary.BigEndian.Uint64(bytes)
	return int64(v)
}

// Int32ToBytes converts int to byte slice
func Int32ToBytes(value int32, dst []byte) {
	if len(dst) < Int32Size {
		panic("destination size does is less than integer")
	}
	binary.BigEndian.PutUint32(dst, uint32(value))
}

// BytesToInt32 converts byte slice to int64
func BytesToInt32(bytes []byte) int32 {
	v := binary.BigEndian.Uint32(bytes)
	return int32(v)
}
