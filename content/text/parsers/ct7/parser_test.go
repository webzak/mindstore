package ct7

import (
	"io"
	"strings"
	"testing"
)

func TestParser_Next(t *testing.T) {
	tests := []struct {
		name      string
		input     string
		config    Config
		want      []Chunk
		wantErr   bool
		errString string
	}{
		{
			name: "Single chunk",
			input: `### Title 1
Some text here.`,
			config: Config{},
			want: []Chunk{
				{Title: "Title 1", Text: "### Title 1\nSome text here."},
			},
		},
		{
			name: "Multiple chunks",
			input: `### Title 1
Text 1
--------------------------------
### Title 2
Text 2`,
			config: Config{},
			want: []Chunk{
				{Title: "Title 1", Text: "### Title 1\nText 1"},
				{Title: "Title 2", Text: "### Title 2\nText 2"},
			},
		},
		{
			name: "Variable delimiters",
			input: `Chunk 1
-----
Chunk 2
--------------------
Chunk 3`,
			config: Config{},
			want: []Chunk{
				{Title: "", Text: "Chunk 1"},
				{Title: "", Text: "Chunk 2"},
				{Title: "", Text: "Chunk 3"},
			},
		},
		{
			name:   "Whitespace handling",
			input:  "\n   ###   Spaced Title   \n   \n   Some text with spaces   \n   \n   ----------------\n   \n   Next chunk\n   ",
			config: Config{},
			want: []Chunk{
				{Title: "Spaced Title", Text: "###   Spaced Title   \n   \n   Some text with spaces"},
				{Title: "", Text: "Next chunk"},
			},
		},
		{
			name: "Min size skip",
			input: `Small
-----
Big enough chunk`,
			config: Config{MinChunkSize: 10, OnChunkSizeError: OnErrorSkip},
			want: []Chunk{
				{Title: "", Text: "Big enough chunk"},
			},
		},
		{
			name:      "Min size abort",
			input:     `Small`,
			config:    Config{MinChunkSize: 10, OnChunkSizeError: OnErrorAbort},
			wantErr:   true,
			errString: "chunk size",
		},
		{
			name: "Max size skip",
			input: `Small
-----
Too big chunk here`,
			config: Config{MaxChunkSize: 10, OnChunkSizeError: OnErrorSkip},
			want: []Chunk{
				{Title: "", Text: "Small"},
			},
		},
		{
			name:      "Max size abort",
			input:     `Too big chunk here`,
			config:    Config{MaxChunkSize: 10, OnChunkSizeError: OnErrorAbort},
			wantErr:   true,
			errString: "chunk size",
		},
		{
			name:   "Empty input",
			input:  ``,
			config: Config{},
			want:   nil,
		},
		{
			name: "Only delimiters",
			input: `----------------
------------------`,
			config: Config{},
			want:   nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := NewParser(strings.NewReader(tt.input), tt.config)
			var got []Chunk
			for {
				chunk, err := p.Next()
				if err == io.EOF {
					break
				}
				if err != nil {
					if !tt.wantErr {
						t.Errorf("Unexpected error: %v", err)
					} else if !strings.Contains(err.Error(), tt.errString) {
						t.Errorf("Error %q does not contain %q", err.Error(), tt.errString)
					}
					return
				}
				if tt.wantErr {
					t.Errorf("Expected error but got none")
					return
				}
				got = append(got, *chunk)
			}

			if len(got) != len(tt.want) {
				t.Errorf("Got %d chunks, want %d", len(got), len(tt.want))
				return
			}

			for i := range got {
				if got[i].Title != tt.want[i].Title {
					t.Errorf("Chunk %d Title: got %q, want %q", i, got[i].Title, tt.want[i].Title)
				}
				if got[i].Text != tt.want[i].Text {
					t.Errorf("Chunk %d Text: got %q, want %q", i, got[i].Text, tt.want[i].Text)
				}
			}
		})
	}
}
