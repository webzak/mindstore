package data

// DataType represents the type of data
type DataType int8

const (
	Text DataType = iota
	Image
	Audio
	Video
)

// DataTypeToString converts a DataType to its string representation
func DataTypeToString(dt DataType) string {
	switch dt {
	case Text:
		return "text"
	case Image:
		return "image"
	case Audio:
		return "audio"
	case Video:
		return "video"
	default:
		panic("unknown DataType")
	}
}

// StringToDataType converts a string to its DataType representation
func StringToDataType(s string) (DataType, bool) {
	switch s {
	case "text":
		return Text, true
	case "image":
		return Image, true
	case "audio":
		return Audio, true
	case "video":
		return Video, true
	default:
		return 0, false
	}
}
