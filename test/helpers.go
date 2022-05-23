package test

import (
	"reflect"
	"testing"
)

// AssertNil is a test helper that ensures err is nil.
func AssertNil(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

// AssertEqual is a test helper that ensures
// expected is deeply equal to got.
func AssertEqual(t *testing.T, expected any, got any) {
	t.Helper()
	if !reflect.DeepEqual(expected, got) {
		t.Fatalf("expected %#v, got %#v", expected, got)
	}
}

// AssertNotEqual is a test helper that ensures
// expected is not deeply equal to got.
func AssertNotEqual(t *testing.T, a any, b any) {
	t.Helper()
	if reflect.DeepEqual(a, b) {
		t.Fatalf("%#v should not be equal to %#v", a, b)
	}
}
