package ct7

import (
	"bufio"
	"fmt"
	"io"
	"strings"
)

// Chunk represents a parsed text chunk.
type Chunk struct {
	Title string
	Text  string
}

// OnErrorStrategy defines how to handle chunk size errors.
type OnErrorStrategy string

const (
	// OnErrorSkip skips the chunk if it violates size constraints.
	OnErrorSkip OnErrorStrategy = "skip"
	// OnErrorAbort returns an error if the chunk violates size constraints.
	OnErrorAbort OnErrorStrategy = "abort"
)

// Config holds the configuration for the parser.
type Config struct {
	MinChunkSize     int
	MaxChunkSize     int
	OnChunkSizeError OnErrorStrategy
}

// Parser parses text chunks from an io.Reader.
type Parser struct {
	scanner *bufio.Scanner
	config  Config
	atEOF   bool
}

// NewParser creates a new Parser with the given configuration.
func NewParser(r io.Reader, cfg Config) *Parser {
	return &Parser{
		scanner: bufio.NewScanner(r),
		config:  cfg,
	}
}

// Next returns the next Chunk from the input.
// It returns io.EOF when there are no more chunks.
func (p *Parser) Next() (*Chunk, error) {
	if p.atEOF {
		return nil, io.EOF
	}

	var sb strings.Builder
	foundContent := false

	for p.scanner.Scan() {
		line := p.scanner.Text()

		// Check for delimiter
		if isDelimiter(line) {
			if foundContent {
				// End of current chunk
				return p.processChunk(sb.String())
			}
			// Skip leading or consecutive delimiters
			continue
		}

		sb.WriteString(line)
		sb.WriteByte('\n')
		foundContent = true
	}

	if err := p.scanner.Err(); err != nil {
		return nil, err
	}

	p.atEOF = true
	if foundContent {
		return p.processChunk(sb.String())
	}

	return nil, io.EOF
}

func isDelimiter(line string) bool {
	// Check if line consists of at least 5 dashes
	trimmed := strings.TrimSpace(line)
	if len(trimmed) < 5 {
		return false
	}
	for _, r := range trimmed {
		if r != '-' {
			return false
		}
	}
	return true
}

func (p *Parser) processChunk(raw string) (*Chunk, error) {
	// Trim surrounding whitespace from the raw text
	text := strings.TrimSpace(raw)
	if len(text) == 0 {
		// Recursively call Next to skip empty chunks if any
		return p.Next()
	}

	// Extract Title
	title := ""
	lines := strings.Split(text, "\n")
	for _, line := range lines {
		trimmedLine := strings.TrimSpace(line)
		if strings.HasPrefix(trimmedLine, "###") {
			title = strings.TrimSpace(strings.TrimPrefix(trimmedLine, "###"))
			break
		}
	}

	// Validate size
	size := len(title) + len(text)
	if p.config.MinChunkSize > 0 && size < p.config.MinChunkSize {
		if p.config.OnChunkSizeError == OnErrorAbort {
			return nil, fmt.Errorf("chunk size %d is less than min %d", size, p.config.MinChunkSize)
		}
		// Skip this chunk
		return p.Next()
	}
	if p.config.MaxChunkSize > 0 && size > p.config.MaxChunkSize {
		if p.config.OnChunkSizeError == OnErrorAbort {
			return nil, fmt.Errorf("chunk size %d is greater than max %d", size, p.config.MaxChunkSize)
		}
		// Skip this chunk
		return p.Next()
	}

	return &Chunk{
		Title: title,
		Text:  text,
	}, nil
}
