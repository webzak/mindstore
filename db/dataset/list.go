package dataset

import "iter"

// ChunkFilter is a predicate function for filtering chunks in a pipeline.
// Returns (true, nil) to include, (false, nil) to skip, (_, error) to stop with error.
type ChunkFilter func(c *Chunk) (bool, error)

// ByIDs returns a filter that matches chunks with any of the given IDs.
func ByIDs(ids ...uint32) ChunkFilter {
	set := make(map[uint32]struct{}, len(ids))
	for _, id := range ids {
		set[id] = struct{}{}
	}
	return func(c *Chunk) (bool, error) {
		_, ok := set[c.ID]
		return ok, nil
	}
}

// ListBuilder constructs a list pipeline with filters and load stages.
type ListBuilder struct {
	ds     *Dataset
	stages []listStage
}

type stageKind uint8

const (
	stageFilter stageKind = iota
	stageLoad
)

type listStage struct {
	kind   stageKind
	filter ChunkFilter // used when kind == stageFilter
	fields []Field     // used when kind == stageLoad
}

// List returns a new ListBuilder for iterating over chunks.
func (d *Dataset) List() *ListBuilder {
	return &ListBuilder{
		ds:     d,
		stages: nil,
	}
}

// Filter adds a filter stage to the pipeline.
func (b *ListBuilder) Filter(f ChunkFilter) *ListBuilder {
	b.stages = append(b.stages, listStage{
		kind:   stageFilter,
		filter: f,
	})
	return b
}

// Load adds a load stage that reads specified fields from disk.
func (b *ListBuilder) Load(fields ...Field) *ListBuilder {
	b.stages = append(b.stages, listStage{
		kind:   stageLoad,
		fields: fields,
	})
	return b
}

// Iter returns an iterator that executes the pipeline.
// The iterator holds the dataset lock for its entire duration.
// Errors from filters or I/O are yielded and stop iteration.
func (b *ListBuilder) Iter() iter.Seq2[*Chunk, error] {
	return func(yield func(*Chunk, error) bool) {
		b.ds.Lock()
		defer b.ds.Unlock()

		for _, idx := range b.ds.index {
			// Skip deleted records
			if idx.isDeleted() {
				continue
			}

			// Create chunk with index metadata
			chunk := &Chunk{
				ID:   idx.ID,
				Date: idx.Date,
				ChunkData: ChunkData{
					Flags:      idx.Flags,
					DataDesc:   idx.DataDesc,
					MetaDesc:   idx.MetaDesc,
					VectorDesc: idx.VectorDesc,
				},
			}

			// Execute pipeline stages in order
			skip := false
			for _, stage := range b.stages {
				switch stage.kind {
				case stageFilter:
					ok, err := stage.filter(chunk)
					if err != nil {
						yield(nil, err)
						return
					}
					if !ok {
						skip = true
						break
					}

				case stageLoad:
					if err := b.ds.readChunkFields(chunk, &idx, stage.fields); err != nil {
						yield(nil, err)
						return
					}
				}

				if skip {
					break
				}
			}

			if skip {
				continue
			}

			if !yield(chunk, nil) {
				return
			}
		}
	}
}
