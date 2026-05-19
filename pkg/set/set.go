// Package set defines ny own custom implementation of the Set data structure
package set

type Set[T comparable] map[T]struct{}

func NewSet[T comparable]() Set[T] {
	return make(Set[T])
}
func (s Set[T]) Add(v T) {
	s[v] = struct{}{}
}

func (s Set[T]) Has(v T) bool {
	_, ok := s[v]
	return ok
}

func (s Set[T]) Remove(v T) {
	delete(s, v)
}

func (s Set[T]) Pop() (T, bool) {
	for item := range s {
		delete(s, item)
		return item, true
	}

	var zero T
	return zero, false
}

func (s Set[T]) Clear() {
	clear(s)
}

func (s Set[T]) ToSlice() []T {
	slice := []T{}

	for val := range s {
		slice = append(slice, val)
	}
	return slice
}
