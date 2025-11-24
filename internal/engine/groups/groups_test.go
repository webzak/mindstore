package groups

import (
	"os"
	"reflect"
	"testing"

	"github.com/webzak/mindstore/internal/testutil"
)

func TestGroups_BasicOperations(t *testing.T) {
	tmpFile := testutil.MakeTempFile(t, "groups_test_*.bin")
	defer os.Remove(tmpFile)

	g, err := New(tmpFile)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	// Create groups
	g1 := g.CreateGroup()
	if g1 != 1 {
		t.Errorf("CreateGroup() = %d, want 1", g1)
	}
	g2 := g.CreateGroup()
	if g2 != 2 {
		t.Errorf("CreateGroup() = %d, want 2", g2)
	}

	// Assign members
	if err := g.Assign(g1, 10, 1); err != nil {
		t.Errorf("Assign(g1, 10, 1) error = %v", err)
	}
	if err := g.Assign(g1, 20, 0); err != nil {
		t.Errorf("Assign(g1, 20, 0) error = %v", err)
	}
	if err := g.Assign(g2, 30, 0); err != nil {
		t.Errorf("Assign(g2, 30, 0) error = %v", err)
	}

	// Test GetGroup
	if got, ok := g.GetGroup(10); !ok || got != g1 {
		t.Errorf("GetGroup(10) = %d, %v; want %d, true", got, ok, g1)
	}
	if got, ok := g.GetGroup(30); !ok || got != g2 {
		t.Errorf("GetGroup(30) = %d, %v; want %d, true", got, ok, g2)
	}
	if _, ok := g.GetGroup(99); ok {
		t.Errorf("GetGroup(99) should return false")
	}

	// Test GetMembers (should be sorted by place)
	members1 := g.GetMembers(g1)
	if !reflect.DeepEqual(members1, []int{20, 10}) { // 20 is place 0, 10 is place 1
		t.Errorf("GetMembers(g1) = %v, want %v", members1, []int{20, 10})
	}

	members2 := g.GetMembers(g2)
	if !reflect.DeepEqual(members2, []int{30}) {
		t.Errorf("GetMembers(g2) = %v, want %v", members2, []int{30})
	}
}

func TestGroups_AssignErrors(t *testing.T) {
	tmpFile := testutil.MakeTempFile(t, "groups_test_*.bin")
	defer os.Remove(tmpFile)

	g, err := New(tmpFile)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	g1 := g.CreateGroup()

	// Invalid group ID
	if err := g.Assign(999, 10, 0); err == nil {
		t.Error("Assign(999, ...) should fail")
	}

	// Assign same index to same group (update) - should work
	if err := g.Assign(g1, 10, 0); err != nil {
		t.Errorf("Assign(g1, 10, 0) error = %v", err)
	}
	if err := g.Assign(g1, 10, 1); err != nil {
		t.Errorf("Assign(g1, 10, 1) (update) error = %v", err)
	}

	// Assign same index to different group - should fail
	g2 := g.CreateGroup()
	if err := g.Assign(g2, 10, 0); err == nil {
		t.Error("Assign(g2, 10, 0) should fail (already in g1)")
	}
}

func TestGroups_Persistence(t *testing.T) {
	tmpFile := testutil.MakeTempFile(t, "groups_test_*.bin")
	defer os.Remove(tmpFile)

	g, err := New(tmpFile)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	g1 := g.CreateGroup()
	g.Assign(g1, 10, 1)
	g.Assign(g1, 20, 0)

	if err := g.Save(); err != nil {
		t.Fatalf("Save() error = %v", err)
	}

	// Reload
	g2, err := New(tmpFile)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if err := g2.Load(); err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	// Verify state
	members := g2.GetMembers(g1)
	if !reflect.DeepEqual(members, []int{20, 10}) {
		t.Errorf("GetMembers(g1) loaded = %v, want %v", members, []int{20, 10})
	}

	// Verify next group ID is correct
	g3 := g2.CreateGroup()
	if g3 != g1+1 {
		t.Errorf("CreateGroup() after load = %d, want %d", g3, g1+1)
	}
}
