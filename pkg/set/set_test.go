package set

import (
	"testing"
)

func TestSetBasicOperations(t *testing.T) {
	s := NewSet[string]()
	s.Add("a")
	if !s.Has("a") {
		t.Fatalf("expected set to have value 'a'")
	}

	s.Remove("a")
	if s.Has("a") {
		t.Fatalf("expected 'a' to be removed")
	}

	s.Add("b")
	v, ok := s.Pop()
	if !ok || v == "" {
		t.Fatalf("expected Pop to return an element")
	}

	s.Add("x")
	s.Add("y")
	sl := s.ToSlice()
	if len(sl) != 2 {
		t.Fatalf("expected slice length 2, got %d", len(sl))
	}

	s.Clear()
	if len(s.ToSlice()) != 0 {
		t.Fatalf("expected set to be empty after Clear")
	}
}
