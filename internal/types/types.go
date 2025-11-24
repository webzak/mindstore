package types

// SortType
type SortType int

const (
	SortAsc SortType = iota
	SortDesc
)

// VectorSearchMethod represents the search/similarity algorithm to use
type VectorSearchMethod int

const (
	// CosineSimMethod uses cosine similarity for search
	CosineSimMethod VectorSearchMethod = iota
)

// VectorSearchOptions represents options for vector search
type VectorSearchOptions struct {
	Method VectorSearchMethod
	Limit  int
	Sort   SortType
}

// SortTypeToString converts a SortType to its string representation
func SortTypeToString(st SortType) string {
	switch st {
	case SortAsc:
		return "asc"
	case SortDesc:
		return "desc"
	default:
		panic("unknown SortType")
	}
}

// StringToSortType converts a string to its SortType representation
func StringToSortType(s string) (SortType, bool) {
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
