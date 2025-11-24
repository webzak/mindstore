package math

import (
	"fmt"
	"math"
	"sort"

	"github.com/webzak/mindstore/internal/types"
)

// Distance represents distance calculation result
type Distance struct {
	ID       int     // index number
	Value    float32 // vector distance value
	Position int     // data position
}

// CosineSimRanking calculates cosine similarity over vectors list
// The results can be limited by limit value, 0 means return all
// The results are ordered by sort order
func CosineSimRanking(rows [][]float32, vector []float32, sortOrder types.SortType, limit int) ([]Distance, error) {
	lenRows := len(rows)
	lenVector := len(vector)
	res := make([]Distance, lenRows)
	for i, row := range rows {
		if len(row) != lenVector {
			return nil, fmt.Errorf("vector size mismatch: expected: %d, actual: %d", lenVector, len(row))
		}
		res[i] = Distance{
			ID:       i,
			Value:    CosineSim(row, vector),
			Position: i,
		}
	}
	if sortOrder == types.SortAsc {
		sort.Slice(res, func(i, j int) bool {
			return res[i].Value < res[j].Value
		})
	} else {
		sort.Slice(res, func(i, j int) bool {
			return res[i].Value > res[j].Value
		})
	}
	if limit > 0 && len(res) > limit {
		return res[:limit], nil
	}
	return res, nil

}

// CosineSim calculates cosine similarity between two vectors.
// Cosine similarity is the dot product of the vectors divided by the product of their magnitudes.
// Returns a value between -1 and 1, where 1 means identical direction, 0 means orthogonal,
// and -1 means opposite direction. Returns 0 if either vector is a zero vector.
// Note: assumes the sizes of a and b are equal (verified by caller).
func CosineSim(a []float32, b []float32) float32 {
	var sa, sb, sab float32 = 0.0, 0.0, 0.0
	for i, va := range a {
		vb := b[i]
		sab += va * vb
		sa += va * va
		sb += vb * vb
	}
	// Compute sqrt(sa * sb) instead of sqrt(sa) * sqrt(sb) for better efficiency
	sasb := float32(math.Sqrt(float64(sa * sb)))
	if sasb == 0 {
		return 0
	}
	return sab / sasb
}
