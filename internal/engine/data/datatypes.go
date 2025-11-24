package data

// Type represents the type of data
type Type uint8

const (
	Text Type = iota
	Image
	Audio
	Video
)

// TypeToString converts a DataType to its string representation
func TypeToString(dt Type) string {
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

// StringToType converts a string to its DataType representation
func StringToType(s string) (Type, bool) {
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
