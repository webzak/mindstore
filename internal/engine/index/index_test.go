package index

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/webzak/mindstore/internal/testutil"
)

func TestNew(t *testing.T) {
	dir := testutil.MakeTempDir(t)
	defer os.RemoveAll(dir)

	path := filepath.Join(dir, "test.idx")
	idx, err := New(path, nil)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if idx == nil {
		t.Fatal("New() returned nil index")
	}
	if idx.Count() != 0 {
		t.Errorf("New() count = %d, want 0", idx.Count())
	}
}

func TestNewWithOptions(t *testing.T) {
	dir := testutil.MakeTempDir(t)
	defer os.RemoveAll(dir)

	path := filepath.Join(dir, "test.idx")
	opt := &IndexOptions{
		MaxAppendBufferSize: 10,
	}
	idx, err := New(path, opt)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if idx.maxAppendBufferSize != 10 {
		t.Errorf("New() maxAppendBufferSize = %d, want 10", idx.maxAppendBufferSize)
	}
}

func TestAppend(t *testing.T) {
	dir := testutil.MakeTempDir(t)
	defer os.RemoveAll(dir)

	path := filepath.Join(dir, "test.idx")
	idx, err := New(path, nil)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	row := Row{
		Offset:             100,
		Size:               200,
		MetaOffset:         300,
		MetaSize:           400,
		DataDescriptor:     1,
		MetaDataDescriptor: 2,
		Flags:              0,
	}

	id, err := idx.Append(row)
	if err != nil {
		t.Fatalf("Append() error = %v", err)
	}
	if id != 0 {
		t.Errorf("Append() id = %d, want 0", id)
	}
	if idx.Count() != 1 {
		t.Errorf("Count() = %d, want 1", idx.Count())
	}
}

func TestGet(t *testing.T) {
	dir := testutil.MakeTempDir(t)
	defer os.RemoveAll(dir)

	path := filepath.Join(dir, "test.idx")
	idx, err := New(path, nil)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	row := Row{
		Offset:             100,
		Size:               200,
		MetaOffset:         300,
		MetaSize:           400,
		DataDescriptor:     1,
		MetaDataDescriptor: 2,
		Flags:              0,
	}

	idx.Append(row)

	got, err := idx.Get(0)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if got.Offset != row.Offset || got.Size != row.Size {
		t.Errorf("Get() = %+v, want %+v", got, row)
	}
}

func TestGetOutOfRange(t *testing.T) {
	dir := testutil.MakeTempDir(t)
	defer os.RemoveAll(dir)

	path := filepath.Join(dir, "test.idx")
	idx, err := New(path, nil)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	_, err = idx.Get(0)
	if err != ErrIndexOutOfRange {
		t.Errorf("Get() error = %v, want %v", err, ErrIndexOutOfRange)
	}
}

func TestFlush(t *testing.T) {
	dir := testutil.MakeTempDir(t)
	defer os.RemoveAll(dir)

	path := filepath.Join(dir, "test.idx")
	idx, err := New(path, nil)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	row := Row{Offset: 100, Size: 200}
	idx.Append(row)

	if idx.IsPersisted() {
		t.Error("IsPersisted() = true before flush, want false")
	}

	err = idx.Flush()
	if err != nil {
		t.Fatalf("Flush() error = %v", err)
	}

	if !idx.IsPersisted() {
		t.Error("IsPersisted() = false after flush, want true")
	}
}

func TestPersistence(t *testing.T) {
	dir := testutil.MakeTempDir(t)
	defer os.RemoveAll(dir)

	path := filepath.Join(dir, "test.idx")

	// Create index and add rows
	idx, err := New(path, nil)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	row1 := Row{Offset: 100, Size: 200, DataDescriptor: 1}
	row2 := Row{Offset: 300, Size: 400, DataDescriptor: 2}

	idx.Append(row1)
	idx.Append(row2)
	idx.Flush()

	// Reload index from storage
	idx2, err := New(path, nil)
	if err != nil {
		t.Fatalf("New() reload error = %v", err)
	}

	if idx2.Count() != 2 {
		t.Errorf("Count() after reload = %d, want 2", idx2.Count())
	}

	got, _ := idx2.Get(0)
	if got.Offset != row1.Offset || got.Size != row1.Size {
		t.Errorf("Get(0) after reload = %+v, want %+v", got, row1)
	}
}

func TestReplace(t *testing.T) {
	dir := testutil.MakeTempDir(t)
	defer os.RemoveAll(dir)

	path := filepath.Join(dir, "test.idx")
	idx, err := New(path, nil)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	row := Row{Offset: 100, Size: 200}
	idx.Append(row)
	idx.Flush()

	newRow := Row{Offset: 500, Size: 600}
	err = idx.Replace(0, newRow)
	if err != nil {
		t.Fatalf("Replace() error = %v", err)
	}

	got, _ := idx.Get(0)
	if got.Offset != newRow.Offset || got.Size != newRow.Size {
		t.Errorf("Get() after Replace() = %+v, want %+v", got, newRow)
	}
}

func TestSetFlags(t *testing.T) {
	dir := testutil.MakeTempDir(t)
	defer os.RemoveAll(dir)

	path := filepath.Join(dir, "test.idx")
	idx, err := New(path, nil)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	row := Row{Offset: 100, Size: 200, Flags: 0}
	idx.Append(row)
	idx.Flush()

	err = idx.SetFlags(0, MarkedForRemoval)
	if err != nil {
		t.Fatalf("SetFlags() error = %v", err)
	}

	got, _ := idx.Get(0)
	if got.Flags&MarkedForRemoval == 0 {
		t.Error("SetFlags() did not set MarkedForRemoval flag")
	}
}

func TestResetFlags(t *testing.T) {
	dir := testutil.MakeTempDir(t)
	defer os.RemoveAll(dir)

	path := filepath.Join(dir, "test.idx")
	idx, err := New(path, nil)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	row := Row{Offset: 100, Size: 200, Flags: MarkedForRemoval}
	idx.Append(row)
	idx.Flush()

	err = idx.ResetFlags(0, MarkedForRemoval)
	if err != nil {
		t.Fatalf("ResetFlags() error = %v", err)
	}

	got, _ := idx.Get(0)
	if got.Flags&MarkedForRemoval != 0 {
		t.Error("ResetFlags() did not clear MarkedForRemoval flag")
	}
}

func TestDestroy(t *testing.T) {
	dir := testutil.MakeTempDir(t)
	defer os.RemoveAll(dir)

	path := filepath.Join(dir, "test.idx")
	idx, err := New(path, nil)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	row := Row{Offset: 100, Size: 200}
	idx.Append(row)
	idx.Flush()

	err = idx.Destroy()
	if err != nil {
		t.Fatalf("Destroy() error = %v", err)
	}

	if idx.Count() != 0 {
		t.Errorf("Count() after Destroy() = %d, want 0", idx.Count())
	}
}

func TestOptimise(t *testing.T) {
	dir := testutil.MakeTempDir(t)
	defer os.RemoveAll(dir)

	path := filepath.Join(dir, "test.idx")
	idx, err := New(path, nil)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	// Add multiple rows
	idx.Append(Row{Offset: 100, Size: 200})
	idx.Append(Row{Offset: 300, Size: 400})
	idx.Append(Row{Offset: 500, Size: 600})
	idx.Flush()

	// Mark middle row for removal
	idx.SetFlags(1, MarkedForRemoval)

	// Optimise
	err = idx.Optimise()
	if err != nil {
		t.Fatalf("Optimise() error = %v", err)
	}

	if idx.Count() != 2 {
		t.Errorf("Count() after Optimise() = %d, want 2", idx.Count())
	}

	// Verify remaining rows
	row0, _ := idx.Get(0)
	if row0.Offset != 100 {
		t.Errorf("Get(0) after Optimise() offset = %d, want 100", row0.Offset)
	}

	row1, _ := idx.Get(1)
	if row1.Offset != 500 {
		t.Errorf("Get(1) after Optimise() offset = %d, want 500", row1.Offset)
	}
}

func TestIterator(t *testing.T) {
	dir := testutil.MakeTempDir(t)
	defer os.RemoveAll(dir)

	path := filepath.Join(dir, "test.idx")
	idx, err := New(path, nil)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	// Add multiple rows
	idx.Append(Row{Offset: 100, Size: 200})
	idx.Append(Row{Offset: 300, Size: 400})
	idx.Append(Row{Offset: 500, Size: 600})

	count := 0
	for pos, row := range idx.Iterator() {
		if row == nil {
			t.Fatal("Iterator returned nil row")
		}
		if pos != count {
			t.Errorf("Iterator position = %d, want %d", pos, count)
		}
		count++
	}

	if count != 3 {
		t.Errorf("Iterator counted %d rows, want 3", count)
	}
}

func TestIteratorEmpty(t *testing.T) {
	dir := testutil.MakeTempDir(t)
	defer os.RemoveAll(dir)

	path := filepath.Join(dir, "test.idx")
	idx, err := New(path, nil)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	count := 0
	for range idx.Iterator() {
		count++
	}
	if count != 0 {
		t.Errorf("Iterator on empty index iterated %d times, want 0", count)
	}
}
