package repositories

import "errors"

// ErrNotFound is returned when a lookup matches no row. Callers should test with
// errors.Is rather than comparing pgx.ErrNoRows, so the pgx dependency stays
// inside this package.
var ErrNotFound = errors.New("record not found")
