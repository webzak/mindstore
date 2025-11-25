package db

// import "errors"

// var (
// 	// ErrMetadataNotEnabled is returned when trying to access metadata on a collection with metadata disabled
// 	ErrMetadataNotEnabled = errors.New("metadata not enabled for this collection")
// 	// ErrMetadataKeyNotFound is returned when a metadata key doesn't exist
// 	ErrMetadataKeyNotFound = errors.New("metadata key not found")
// 	// ErrMetadataTypeMismatch is returned when a metadata value has a different type than requested
// 	ErrMetadataTypeMismatch = errors.New("metadata value type mismatch")
// )

// // MetadataAccessor provides a fluent API for accessing and modifying metadata for a specific record ID.
// type MetadataAccessor struct {
// 	collection *Collection
// 	id         int
// }

// // Metadata returns a MetadataAccessor for the given record ID.
// // This provides a fluent API for working with metadata.
// //
// // Example:
// //
// //	collection.Metadata(1).SetString("name", "John").SetInt("age", 30)
// //	name, ok := collection.Metadata(1).GetString("name")
// func (c *Collection) Metadata(id int) *MetadataAccessor {
// 	return &MetadataAccessor{
// 		collection: c,
// 		id:         id,
// 	}
// }

// // GetString retrieves a string value from metadata.
// // Returns the value and true if found and is a string, otherwise returns empty string and false.
// func (ma *MetadataAccessor) GetString(key string) (string, bool) {
// 	if ma.collection.meta == nil {
// 		return "", false
// 	}

// 	meta := ma.collection.meta.Get(ma.id)
// 	if meta == nil {
// 		return "", false
// 	}

// 	val, ok := meta[key].(string)
// 	return val, ok
// }

// // GetInt retrieves an int value from metadata.
// // Returns the value and true if found and is an int, otherwise returns 0 and false.
// func (ma *MetadataAccessor) GetInt(key string) (int, bool) {
// 	if ma.collection.meta == nil {
// 		return 0, false
// 	}

// 	meta := ma.collection.meta.Get(ma.id)
// 	if meta == nil {
// 		return 0, false
// 	}

// 	val, ok := meta[key].(int)
// 	return val, ok
// }

// // GetBool retrieves a bool value from metadata.
// // Returns the value and true if found and is a bool, otherwise returns false and false.
// func (ma *MetadataAccessor) GetBool(key string) (bool, bool) {
// 	if ma.collection.meta == nil {
// 		return false, false
// 	}

// 	meta := ma.collection.meta.Get(ma.id)
// 	if meta == nil {
// 		return false, false
// 	}

// 	val, ok := meta[key].(bool)
// 	return val, ok
// }

// // GetBytes retrieves a []byte value from metadata.
// // Returns the value and true if found and is a []byte, otherwise returns nil and false.
// func (ma *MetadataAccessor) GetBytes(key string) ([]byte, bool) {
// 	if ma.collection.meta == nil {
// 		return nil, false
// 	}

// 	meta := ma.collection.meta.Get(ma.id)
// 	if meta == nil {
// 		return nil, false
// 	}

// 	val, ok := meta[key].([]byte)
// 	return val, ok
// }

// // SetString sets a string value in metadata.
// // Returns the MetadataAccessor for method chaining.
// func (ma *MetadataAccessor) SetString(key string, value string) *MetadataAccessor {
// 	if ma.collection.meta != nil {
// 		ma.collection.meta.SetKey(ma.id, key, value)
// 	}
// 	return ma
// }

// // SetInt sets an int value in metadata.
// // Returns the MetadataAccessor for method chaining.
// func (ma *MetadataAccessor) SetInt(key string, value int) *MetadataAccessor {
// 	if ma.collection.meta != nil {
// 		ma.collection.meta.SetKey(ma.id, key, value)
// 	}
// 	return ma
// }

// // SetBool sets a bool value in metadata.
// // Returns the MetadataAccessor for method chaining.
// func (ma *MetadataAccessor) SetBool(key string, value bool) *MetadataAccessor {
// 	if ma.collection.meta != nil {
// 		ma.collection.meta.SetKey(ma.id, key, value)
// 	}
// 	return ma
// }

// // SetBytes sets a []byte value in metadata.
// // Returns the MetadataAccessor for method chaining.
// func (ma *MetadataAccessor) SetBytes(key string, value []byte) *MetadataAccessor {
// 	if ma.collection.meta != nil {
// 		ma.collection.meta.SetKey(ma.id, key, value)
// 	}
// 	return ma
// }

// // Set merges the provided metadata map with existing metadata.
// // It updates existing keys and adds new ones without removing existing keys.
// // Returns the MetadataAccessor for method chaining.
// func (ma *MetadataAccessor) Set(data map[string]any) *MetadataAccessor {
// 	if ma.collection.meta != nil {
// 		ma.collection.meta.Set(ma.id, data)
// 	}
// 	return ma
// }

// // Replace replaces all metadata with the provided map.
// // This will remove any existing metadata and replace it with the provided data.
// // Returns the MetadataAccessor for method chaining.
// func (ma *MetadataAccessor) Replace(data map[string]any) *MetadataAccessor {
// 	if ma.collection.meta != nil {
// 		ma.collection.meta.Replace(ma.id, data)
// 	}
// 	return ma
// }

// // Get retrieves all metadata for the record as a map.
// // Returns nil if metadata is not enabled or no metadata exists for this ID.
// func (ma *MetadataAccessor) Get() map[string]any {
// 	if ma.collection.meta == nil {
// 		return nil
// 	}
// 	return ma.collection.meta.Get(ma.id)
// }

// // DeleteKey removes a specific key from metadata.
// // Returns the MetadataAccessor for method chaining.
// func (ma *MetadataAccessor) DeleteKey(key string) *MetadataAccessor {
// 	if ma.collection.meta != nil {
// 		ma.collection.meta.DeleteKey(ma.id, key)
// 	}
// 	return ma
// }

// // Delete removes all metadata for the record.
// // Returns the MetadataAccessor for method chaining.
// func (ma *MetadataAccessor) Delete() *MetadataAccessor {
// 	if ma.collection.meta != nil {
// 		ma.collection.meta.Delete(ma.id)
// 	}
// 	return ma
// }

// // Exists checks if metadata exists for this record ID.
// func (ma *MetadataAccessor) Exists() bool {
// 	if ma.collection.meta == nil {
// 		return false
// 	}
// 	meta := ma.collection.meta.Get(ma.id)
// 	return len(meta) > 0
// }

// // HasKey checks if a specific key exists in metadata for this record.
// func (ma *MetadataAccessor) HasKey(key string) bool {
// 	if ma.collection.meta == nil {
// 		return false
// 	}
// 	meta := ma.collection.meta.Get(ma.id)
// 	if meta == nil {
// 		return false
// 	}
// 	_, ok := meta[key]
// 	return ok
// }
