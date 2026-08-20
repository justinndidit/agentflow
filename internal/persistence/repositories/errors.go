package repositories

import (
	"errors"

	"github.com/jackc/pgx/v5"
)

// ErrNotFound is returned when a lookup matches no row. Callers should test with
// errors.Is rather than comparing pgx.ErrNoRows, so the pgx dependency stays
// inside this package.
var ErrNotFound = errors.New("record not found")

// ErrFenced is returned when a guarded scheduling write matches no row.
//
// It is categorically different from ErrNotFound: the task exists, but this
// caller no longer owns it. Its lease expired, the reaper returned the task,
// and someone else has since claimed it — so the work this caller just finished
// has already been redone by another node, and its result must be discarded
// rather than written over the newer one.
//
// Callers should treat this as an expected outcome and log it, not as an error
// to retry. Retrying is exactly the wrong response: the epoch will never match
// again.
var ErrFenced = errors.New("lease superseded")

// isNoRows keeps the pgx dependency inside this package: callers recognise
// absence through ErrNotFound rather than by importing pgx themselves.
func isNoRows(err error) bool {
	return errors.Is(err, pgx.ErrNoRows)
}
