package visual

import (
	"fmt"
	"reflect"
	"strings"
)

// TextShort creates a preview string for a given text chunk.
// It flattens multiline text into a single line. If the flattened text
// is longer than 30 characters, it shows the first 20, an ellipsis "...",
// and the last 10 characters. Otherwise, it shows the full flattened text.
// The preview is prefixed with the total length of the original flattened text,
// e.g., "[533] This is the story about something ... the end."
func TextShort(text string) string {
	flattenedText := strings.ReplaceAll(text, "\n", " ")
	flattenedText = strings.ReplaceAll(flattenedText, "\r", " ")
	length := len(flattenedText)

	if length > 30 {
		return fmt.Sprintf("[%d] %s...%s", length, flattenedText[:20], flattenedText[length-10:])
	}
	return fmt.Sprintf("[%d] %s", length, flattenedText)
}

// VectorShort creates a string representation of a []float32 slice.
// If the slice length is greater than 3, it shows the first two elements and the last element.
// Otherwise, it shows all elements. All float values are rounded to 3 decimal places.
// The format is [length][element1, element2 .... lastElement].
func VectorShort(s []float32) string {
	var sb strings.Builder
	length := len(s)

	sb.WriteString(fmt.Sprintf("[%d]", length))
	sb.WriteString("[")

	if length == 0 {
		// No elements to display
	} else if length <= 3 {
		for i, val := range s {
			sb.WriteString(fmt.Sprintf("%.3f", val))
			if i < length-1 {
				sb.WriteString(", ")
			}
		}
	} else { // length > 3
		sb.WriteString(fmt.Sprintf("%.3f", s[0]))
		sb.WriteString(", ")
		sb.WriteString(fmt.Sprintf("%.3f", s[1]))
		sb.WriteString(" .... ")
		sb.WriteString(fmt.Sprintf("%.3f", s[length-1]))
	}

	sb.WriteString("]")
	return sb.String()
}

// TagsShort creates a string representation of a []string slice.
// If the slice length is greater than 3, it shows the first two elements and the last element.
// Otherwise, it shows all elements. The format is [length][element1, element2 .... lastElement].
func TagsShort(tags []string) string {
	return strings.Join(tags, ", ")
}

// MetadataShort creates a string representation of a map[string]any.
// It shows string, int, float, or bool values directly, and the Go type for others.
// The format is key1="foo", key2=234, key3=3.333, key4=false, key5=<go type here>.
func MetadataShort(metadata map[string]any) string {
	var sb strings.Builder
	first := true
	for k, v := range metadata {
		if !first {
			sb.WriteString(", ")
		}
		sb.WriteString(k)
		sb.WriteString("=")

		switch val := v.(type) {
		case string:
			sb.WriteString(fmt.Sprintf("%q", val))
		case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
			sb.WriteString(fmt.Sprintf("%v", val))
		case float32:
			sb.WriteString(fmt.Sprintf("%.3f", val))
		case float64:
			sb.WriteString(fmt.Sprintf("%.3f", val))
		case bool:
			sb.WriteString(fmt.Sprintf("%t", val))
		default:
			sb.WriteString(fmt.Sprintf("<%s>", reflect.TypeOf(v).String()))
		}
		first = false
	}
	return sb.String()
}
