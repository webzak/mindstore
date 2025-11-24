package tags

import (
	"os"
	"reflect"
	"sort"
	"testing"

	"github.com/webzak/mindstore/internal/testutil"
)

func TestTags_BasicOperations(t *testing.T) {
	tmpFile := testutil.MakeTempFile(t, "tags_test_*.bin")
	defer os.Remove(tmpFile)

	tags, err := New(tmpFile)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	// Test Add and GetIDs
	tags.Add(1, "tag1")
	tags.Add(1, "tag2")
	tags.Add(2, "tag1")

	ids := tags.GetIDs("tag1")
	sort.Ints(ids)
	if !reflect.DeepEqual(ids, []int{1, 2}) {
		t.Errorf("GetIDs('tag1') = %v, want %v", ids, []int{1, 2})
	}

	ids = tags.GetIDs("tag2")
	if !reflect.DeepEqual(ids, []int{1}) {
		t.Errorf("GetIDs('tag2') = %v, want %v", ids, []int{1})
	}

	// Test GetTags
	gotTags := tags.GetTags(1)
	sort.Strings(gotTags)
	if !reflect.DeepEqual(gotTags, []string{"tag1", "tag2"}) {
		t.Errorf("GetTags(1) = %v, want %v", gotTags, []string{"tag1", "tag2"})
	}

	gotTags = tags.GetTags(2)
	if !reflect.DeepEqual(gotTags, []string{"tag1"}) {
		t.Errorf("GetTags(2) = %v, want %v", gotTags, []string{"tag1"})
	}
}

func TestTags_CaseInsensitivity(t *testing.T) {
	tmpFile := testutil.MakeTempFile(t, "tags_test_*.bin")
	defer os.Remove(tmpFile)

	tags, err := New(tmpFile)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	tags.Add(1, "Tag1")
	ids := tags.GetIDs("tag1")
	if !reflect.DeepEqual(ids, []int{1}) {
		t.Errorf("GetIDs('tag1') = %v, want %v", ids, []int{1})
	}

	ids = tags.GetIDs("TAG1")
	if !reflect.DeepEqual(ids, []int{1}) {
		t.Errorf("GetIDs('TAG1') = %v, want %v", ids, []int{1})
	}
}

func TestTags_Remove(t *testing.T) {
	tmpFile := testutil.MakeTempFile(t, "tags_test_*.bin")
	defer os.Remove(tmpFile)

	tags, err := New(tmpFile)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	tags.Add(1, "tag1")
	tags.Add(1, "tag2")
	tags.Add(2, "tag1")

	tags.Remove(1, "tag1")

	ids := tags.GetIDs("tag1")
	if !reflect.DeepEqual(ids, []int{2}) {
		t.Errorf("GetIDs('tag1') after remove = %v, want %v", ids, []int{2})
	}

	gotTags := tags.GetTags(1)
	if !reflect.DeepEqual(gotTags, []string{"tag2"}) {
		t.Errorf("GetTags(1) after remove = %v, want %v", gotTags, []string{"tag2"})
	}
}

func TestTags_Persistence(t *testing.T) {
	tmpFile := testutil.MakeTempFile(t, "tags_test_*.bin")
	defer os.Remove(tmpFile)

	tags, err := New(tmpFile)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	tags.Add(1, "tag1")
	tags.Add(2, "tag2")

	if err := tags.Flush(); err != nil {
		t.Fatalf("Save() error = %v", err)
	}

	// Create new instance and load
	tags2, err := New(tmpFile)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	if err := tags2.Load(); err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	ids := tags2.GetIDs("tag1")
	if !reflect.DeepEqual(ids, []int{1}) {
		t.Errorf("GetIDs('tag1') loaded = %v, want %v", ids, []int{1})
	}

	ids = tags2.GetIDs("tag2")
	if !reflect.DeepEqual(ids, []int{2}) {
		t.Errorf("GetIDs('tag2') loaded = %v, want %v", ids, []int{2})
	}
}
