package visual

import (
	"testing"
)

func TestTextShort(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "empty string",
			input:    "",
			expected: "[0] ",
		},
		{
			name:     "short text",
			input:    "Hello World",
			expected: "[11] Hello World",
		},
		{
			name:     "text at boundary (30 chars)",
			input:    "123456789012345678901234567890",
			expected: "[30] 123456789012345678901234567890",
		},
		{
			name:     "text just over boundary (31 chars)",
			input:    "1234567890123456789012345678901",
			expected: "[31] 12345678901234567890...2345678901",
		},
		{
			name:     "long text",
			input:    "This is a very long text that should be truncated to show only the first 20 characters and the last 10 characters with ellipsis in between.",
			expected: "[139] This is a very long ...n between.",
		},
		{
			name:     "multiline text",
			input:    "Line 1\nLine 2\nLine 3",
			expected: "[20] Line 1 Line 2 Line 3",
		},
		{
			name:     "text with carriage returns",
			input:    "Line 1\r\nLine 2\r\nLine 3",
			expected: "[22] Line 1  Line 2  Line 3",
		},
		{
			name:     "long multiline text",
			input:    "This is the first line\nThis is the second line\nThis is the third line and it's quite long indeed",
			expected: "[96] This is the first li...ong indeed",
		},
		{
			name:     "text with only newlines",
			input:    "\n\n\n",
			expected: "[3]    ",
		},
		{
			name:     "single character",
			input:    "A",
			expected: "[1] A",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := TextShort(tt.input)
			if result != tt.expected {
				t.Errorf("TextShort(%q) = %q; want %q", tt.input, result, tt.expected)
			}
		})
	}
}

func TestVectorShort(t *testing.T) {
	tests := []struct {
		name     string
		input    []float32
		expected string
	}{
		{
			name:     "empty vector",
			input:    []float32{},
			expected: "[0][]",
		},
		{
			name:     "single element",
			input:    []float32{1.234567},
			expected: "[1][1.235]",
		},
		{
			name:     "two elements",
			input:    []float32{1.234567, 2.345678},
			expected: "[2][1.235, 2.346]",
		},
		{
			name:     "three elements",
			input:    []float32{1.234567, 2.345678, 3.456789},
			expected: "[3][1.235, 2.346, 3.457]",
		},
		{
			name:     "four elements",
			input:    []float32{1.234567, 2.345678, 3.456789, 4.567890},
			expected: "[4][1.235, 2.346 .... 4.568]",
		},
		{
			name:     "many elements",
			input:    []float32{0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0},
			expected: "[10][0.100, 0.200 .... 1.000]",
		},
		{
			name:     "negative values",
			input:    []float32{-1.234, -2.345, -3.456, -4.567},
			expected: "[4][-1.234, -2.345 .... -4.567]",
		},
		{
			name:     "mixed positive and negative",
			input:    []float32{-1.5, 2.5, -3.5, 4.5},
			expected: "[4][-1.500, 2.500 .... 4.500]",
		},
		{
			name:     "zero values",
			input:    []float32{0.0, 0.0, 0.0, 0.0},
			expected: "[4][0.000, 0.000 .... 0.000]",
		},
		{
			name:     "very small values",
			input:    []float32{0.0001, 0.0002, 0.0003, 0.0004},
			expected: "[4][0.000, 0.000 .... 0.000]",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := VectorShort(tt.input)
			if result != tt.expected {
				t.Errorf("VectorShort(%v) = %q; want %q", tt.input, result, tt.expected)
			}
		})
	}
}

func TestTagsShort(t *testing.T) {
	tests := []struct {
		name     string
		input    []string
		expected string
	}{
		{
			name:     "empty tags",
			input:    []string{},
			expected: "",
		},
		{
			name:     "single tag",
			input:    []string{"tag1"},
			expected: "tag1",
		},
		{
			name:     "two tags",
			input:    []string{"tag1", "tag2"},
			expected: "tag1, tag2",
		},
		{
			name:     "three tags",
			input:    []string{"tag1", "tag2", "tag3"},
			expected: "tag1, tag2, tag3",
		},
		{
			name:     "many tags",
			input:    []string{"golang", "testing", "unit-test", "preview", "package"},
			expected: "golang, testing, unit-test, preview, package",
		},
		{
			name:     "tags with spaces",
			input:    []string{"tag with spaces", "another tag", "third"},
			expected: "tag with spaces, another tag, third",
		},
		{
			name:     "tags with special characters",
			input:    []string{"tag@1", "tag#2", "tag$3"},
			expected: "tag@1, tag#2, tag$3",
		},
		{
			name:     "empty string tags",
			input:    []string{"", "tag1", ""},
			expected: ", tag1, ",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := TagsShort(tt.input)
			if result != tt.expected {
				t.Errorf("TagsShort(%v) = %q; want %q", tt.input, result, tt.expected)
			}
		})
	}
}

func TestMetadataShort(t *testing.T) {
	tests := []struct {
		name     string
		input    map[string]any
		validate func(result string) bool
		desc     string
	}{
		{
			name:  "empty metadata",
			input: map[string]any{},
			validate: func(result string) bool {
				return result == ""
			},
			desc: "should return empty string",
		},
		{
			name: "string value",
			input: map[string]any{
				"name": "test",
			},
			validate: func(result string) bool {
				return result == `name="test"`
			},
			desc: "should format string with quotes",
		},
		{
			name: "int value",
			input: map[string]any{
				"count": 42,
			},
			validate: func(result string) bool {
				return result == "count=42"
			},
			desc: "should format int without quotes",
		},
		{
			name: "float32 value",
			input: map[string]any{
				"score": float32(3.14159),
			},
			validate: func(result string) bool {
				return result == "score=3.142"
			},
			desc: "should format float32 with 3 decimals",
		},
		{
			name: "float64 value",
			input: map[string]any{
				"score": 3.14159,
			},
			validate: func(result string) bool {
				return result == "score=3.142"
			},
			desc: "should format float64 with 3 decimals",
		},
		{
			name: "bool value true",
			input: map[string]any{
				"active": true,
			},
			validate: func(result string) bool {
				return result == "active=true"
			},
			desc: "should format bool true",
		},
		{
			name: "bool value false",
			input: map[string]any{
				"active": false,
			},
			validate: func(result string) bool {
				return result == "active=false"
			},
			desc: "should format bool false",
		},
		{
			name: "complex type",
			input: map[string]any{
				"data": []string{"a", "b"},
			},
			validate: func(result string) bool {
				return result == "data=<[]string>"
			},
			desc: "should show type for complex types",
		},
		{
			name: "all integer types",
			input: map[string]any{
				"int":    int(1),
				"int8":   int8(2),
				"int16":  int16(3),
				"int32":  int32(4),
				"int64":  int64(5),
				"uint":   uint(6),
				"uint8":  uint8(7),
				"uint16": uint16(8),
				"uint32": uint32(9),
				"uint64": uint64(10),
			},
			validate: func(result string) bool {
				// Since map iteration order is not guaranteed, check that all key-value pairs are present
				expectedPairs := []string{
					"int=1", "int8=2", "int16=3", "int32=4", "int64=5",
					"uint=6", "uint8=7", "uint16=8", "uint32=9", "uint64=10",
				}
				for _, pair := range expectedPairs {
					found := false
					for i := 0; i < len(result); i++ {
						if i+len(pair) <= len(result) && result[i:i+len(pair)] == pair {
							found = true
							break
						}
					}
					if !found {
						return false
					}
				}
				return true
			},
			desc: "should handle all integer types",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := MetadataShort(tt.input)
			if !tt.validate(result) {
				t.Errorf("MetadataShort(%v) = %q; %s", tt.input, result, tt.desc)
			}
		})
	}
}

// Benchmark tests
func BenchmarkTextShort(b *testing.B) {
	longText := "This is a very long text that should be truncated to show only the first 20 characters and the last 10 characters with ellipsis in between."
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		TextShort(longText)
	}
}

func BenchmarkVectorShort(b *testing.B) {
	vector := make([]float32, 768)
	for i := range vector {
		vector[i] = float32(i) * 0.001
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		VectorShort(vector)
	}
}

func BenchmarkTagsShort(b *testing.B) {
	tags := []string{"tag1", "tag2", "tag3", "tag4", "tag5"}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		TagsShort(tags)
	}
}

func BenchmarkMetadataShort(b *testing.B) {
	metadata := map[string]any{
		"name":   "test",
		"count":  42,
		"score":  3.14159,
		"active": true,
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		MetadataShort(metadata)
	}
}
