package repositories

import (
	"time"

	"github.com/jackc/pgx/v5/pgtype"
)

// durationToInterval converts a Go duration into a Postgres INTERVAL.
//
// Valid must be set: a pgtype value with Valid=false encodes as NULL regardless
// of the other fields, which is a silent way to write nothing at all.
func durationToInterval(d time.Duration) pgtype.Interval {
	return pgtype.Interval{Microseconds: d.Microseconds(), Valid: true}
}

// intervalToDuration converts a Postgres INTERVAL back into a Go duration.
//
// Months and Days are carried separately by Postgres because their length is
// calendar-dependent. We never write either, but they are converted with nominal
// lengths so a hand-edited row does not silently read back as zero.
func intervalToDuration(i pgtype.Interval) time.Duration {
	if !i.Valid {
		return 0
	}
	return time.Duration(i.Microseconds)*time.Microsecond +
		time.Duration(i.Days)*24*time.Hour +
		time.Duration(i.Months)*30*24*time.Hour
}
