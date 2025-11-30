package mindstore

// ReadOptions specifies which components of an Item to read using bitmask flags.
// If ReadOptions is 0, only the core Data field will be populated.
type ReadOptions uint8

const (
	// ReadData indicates whether to read data
	ReadData ReadOptions = 1 << iota
	// ReadMeta indicates whether to read metadata
	ReadMeta
	// ReadVector indicates whether to read vector data
	ReadVector
	// ReadTags indicates whether to read tags
	ReadTags
	// ReadGroup indicates whether to read group information
	ReadGroup
)

// Has checks if a specific option is set
func (r ReadOptions) Has(flag ReadOptions) bool {
	return r&flag != 0
}

// AllReadOptions returns ReadOptions with all fields set to true
func AllReadOptions() ReadOptions {
	return ReadData | ReadMeta | ReadVector | ReadTags | ReadGroup
}
