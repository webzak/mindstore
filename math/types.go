package math

// SortOrder
type SortOrder int

const (
	SortAsc SortOrder = iota
	SortDesc
)

// Distance represents distance calculation result
type Distance struct {
	ID    int     // index number
	Value float32 // vector distance value
}

// RankingFunc is a function type that calculates similarity/distance rankings
// for a set of vectors against a query vector
type RankingFunc func(rows [][]float32, vector []float32, sortOrder SortOrder, limit int) ([]Distance, error)

// VectorSearchMethod represents the search/similarity algorithm to use
type VectorSearchMethod int

const (
	// CosineSimMethod uses cosine similarity for search
	CosineSimMethod VectorSearchMethod = iota
)

// SortOrderToString converts a SortOrder to its string representation
func SortOrderToString(st SortOrder) string {
	switch st {
	case SortAsc:
		return "asc"
	case SortDesc:
		return "desc"
	default:
		panic("unknown SortType")
	}
}

// StringToSortOrder converts a string to its SortOrder representation
func StringToSortOrder(s string) (SortOrder, bool) {
	switch s {
	case "asc":
		return SortAsc, true
	case "desc":
		return SortDesc, true
	default:
		return 0, false
	}
}

// VectorSearchMethodToString converts a VectorSearchMethod to its string representation
func VectorSearchMethodToString(vsm VectorSearchMethod) string {
	switch vsm {
	case CosineSimMethod:
		return "cosine"
	default:
		panic("unknown VectorSearchMethod")
	}
}

// StringToVectorSearchMethod converts a string to its VectorSearchMethod representation
func StringToVectorSearchMethod(s string) (VectorSearchMethod, bool) {
	switch s {
	case "cosine":
		return CosineSimMethod, true
	default:
		return 0, false
	}
}
