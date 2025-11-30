package assert

import (
	"errors"
	"reflect"
	"testing"
)

// NilError is one liner for checking nil error in tests
func NilError(t *testing.T, err error) {
	if err != nil {
		t.Fatal(err)
	}
}

// NotNilError is one liner for checking nil error in tests
func NotNilError(t *testing.T, err error) {
	if err == nil {
		t.Fatalf("error expected to be not nil")
	}
}

// NotNil is one liner for checking that value is not nil
func NotNil(t *testing.T, value any, message string) {
	if value == nil {
		t.Fatal(message)
	}
}

// Equal is one liner for checking value equality in tests
func Equal[T comparable](t *testing.T, expected, actual T) {
	if actual != expected {
		t.Fatalf("expected %v, got %v", expected, actual)
	}
}

// DeepEqual is one liner for checking deep equality in tests (for slices, maps, etc.)
func DeepEqual(t *testing.T, expected, actual any) {
	if !reflect.DeepEqual(expected, actual) {
		t.Fatalf("expected %v, got %v", expected, actual)
	}
}

// ErrorIs is one liner for checking error equality in tests using errors.Is
func ErrorIs(t *testing.T, expected, actual error) {
	if !errors.Is(actual, expected) {
		t.Fatalf("expected error %v, got %v", expected, actual)
	}
}
