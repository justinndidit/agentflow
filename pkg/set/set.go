// Package set defines ny own custom implementation of the Set data structure
package set

import "sync"

// Set struct represents the struct but in the actual sense
// the internal map (data) is the actual data storage
// using the struct gives us flexibility and safety
type Set[T comparable] struct {
	Data map[T]struct{}
	mu   sync.RWMutex
}

func NewSet[T comparable]() *Set[T] {
	return &Set[T]{
		Data: make(map[T]struct{}),
	}
}
func (s *Set[T]) Add(v T) {
	s.Data[v] = struct{}{}
}

func (s *Set[T]) Has(v T) bool {
	_, ok := s.Data[v]
	return ok
}

func (s *Set[T]) Remove(v T) {
	delete(s.Data, v)
}

func (s *Set[T]) Pop() (T, bool) {
	for item := range s.Data {
		delete(s.Data, item)
		return item, true
	}
	var zero T
	return zero, false
}

func (s *Set[T]) Clear() {
	clear(s.Data)
}

func (s *Set[T]) ToSlice() []T {
	slice := []T{}

	for val := range s.Data {
		slice = append(slice, val)
	}
	return slice
}
