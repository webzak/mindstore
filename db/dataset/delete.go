package dataset

import "github.com/webzak/mindstore/internal/index"

// Delete marks a record for removal by setting the MarkedForRemoval flag
func (c *Dataset) Delete(idx int) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return ErrDatasetClosed
	}

	return c.index.SetFlags(idx, index.MarkedForRemoval)
}
