package db

import "strings"

// AddTag adds one or more tags to the specified record ID
func (c *Dataset) AddTags(id int, tags ...string) error {
	if id < 0 || id >= c.index.Count() {
		return ErrInvalidRecordID
	}
	for _, tag := range tags {
		c.tags.Add(id, tag)
	}
	return nil
}

// RemoveTag removes one or more tags from the specified record ID
func (c *Dataset) RemoveTags(id int, tags ...string) error {
	if id < 0 || id >= c.index.Count() {
		return ErrInvalidRecordID
	}
	for _, tag := range tags {
		c.tags.Remove(id, tag)
	}
	return nil
}

// GetIDsByTag returns all record IDs that have the specified tag
// The tag search is case-insensitive
func (c *Dataset) GetIDsByTag(tag string) ([]int, error) {
	tag = strings.ToLower(tag)
	ids, err := c.tags.GetIDs(tag)
	if err != nil {
		return nil, err
	}
	if ids == nil {
		return []int{}, nil
	}
	return ids, nil
}

// GetTagsByID returns all tags associated with the specified record ID
func (c *Dataset) GetTagsByID(id int) ([]string, error) {
	if id < 0 || id >= c.index.Count() {
		return nil, ErrInvalidRecordID
	}
	tags, err := c.tags.GetTags(id)
	if err != nil {
		return nil, err
	}
	if tags == nil {
		return []string{}, nil
	}
	return tags, nil
}
