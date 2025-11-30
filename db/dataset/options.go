package mindstore

const (
	DefaultMaxDataAppendBufferSize     = 2 << 16
	DefaultMaxMetaDataAppendBufferSize = 2 << 14
	DefaultMaxIndexAppendBufferSize    = 64
	DefaultVectorSize                  = 768
	DefaultMaxVectorBufferSize         = 64
	DefaultMaxVectorAppendBufferSize   = 64
)

// Options defines the configuration for a collection
type Options struct {
	// MaxDataAppendBufferSize max buffer for appending data
	MaxDataAppendBufferSize int
	// MaxMetaDataAppendBufferSize max buffer for appending metadata
	MaxMetaDataAppendBufferSize int
	// MaxVectorAppendBufferSize max buffer for index to be unsynced
	MaxIndexAppendBufferSize int
	// VectorSize is the size of the float32 vector
	VectorSize int
	// MaxVectorBufferSize is the maximum amount of vectors in memory buffer
	MaxVectorBufferSize int
	// MaxAppendBufferSize is the maximum amount of appended vectors which triggers flush
	MaxVectorAppendBufferSize int
}

// DefaulOptions return default options
func DefaultOptions() Options {
	return Options{
		MaxDataAppendBufferSize:     DefaultMaxDataAppendBufferSize,
		MaxMetaDataAppendBufferSize: DefaultMaxMetaDataAppendBufferSize,
		MaxIndexAppendBufferSize:    DefaultMaxIndexAppendBufferSize,
		VectorSize:                  DefaultVectorSize,
		MaxVectorBufferSize:         DefaultMaxVectorBufferSize,
		MaxVectorAppendBufferSize:   DefaultMaxVectorAppendBufferSize,
	}
}
