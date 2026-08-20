package repositories

import (
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
)

// Valid=false encodes as NULL regardless of the other fields, so a converter
// that forgets it writes no timeout at all while looking correct in Go.
func TestDurationToInterval_AlwaysValid(t *testing.T) {
	for _, d := range []time.Duration{
		0, time.Nanosecond, time.Microsecond, time.Second,
		5 * time.Minute, 24 * time.Hour, -time.Second,
	} {
		if got := durationToInterval(d); !got.Valid {
			t.Errorf("durationToInterval(%s).Valid = false, want true", d)
		}
	}
}

func TestDurationToInterval(t *testing.T) {
	tests := []struct {
		name             string
		duration         time.Duration
		wantMicroseconds int64
	}{
		{"zero", 0, 0},
		{"one second", time.Second, 1_000_000},
		{"five minutes", 5 * time.Minute, 300_000_000},
		{"one hour", time.Hour, 3_600_000_000},
		{"negative", -time.Second, -1_000_000},
		// Postgres INTERVAL resolves to microseconds, so anything finer is lost
		// on the way in. Truncation is the expected behaviour, not a rounding.
		{"sub-microsecond truncates to zero", 999 * time.Nanosecond, 0},
		{"fractional microsecond truncates down", 1500 * time.Nanosecond, 1},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := durationToInterval(test.duration)
			if got.Microseconds != test.wantMicroseconds {
				t.Errorf("Microseconds = %d, want %d", got.Microseconds, test.wantMicroseconds)
			}
			// Months and Days are calendar-dependent and never written by this
			// codebase; a non-zero value here would mean a duration was being
			// split into units whose length depends on when it is evaluated.
			if got.Months != 0 {
				t.Errorf("Months = %d, want 0", got.Months)
			}
			if got.Days != 0 {
				t.Errorf("Days = %d, want 0", got.Days)
			}
		})
	}
}

func TestIntervalToDuration(t *testing.T) {
	tests := []struct {
		name     string
		interval pgtype.Interval
		want     time.Duration
	}{
		{
			// A NULL interval is a missing timeout, not a zero-length one. Both
			// read as 0 here; the distinction has to be carried by the pointer
			// on TaskRow.Timeout rather than by this value.
			name:     "null reads as zero",
			interval: pgtype.Interval{Microseconds: 5_000_000, Valid: false},
			want:     0,
		},
		{
			name:     "microseconds",
			interval: pgtype.Interval{Microseconds: 300_000_000, Valid: true},
			want:     5 * time.Minute,
		},
		{
			name:     "days use a nominal 24 hours",
			interval: pgtype.Interval{Days: 2, Valid: true},
			want:     48 * time.Hour,
		},
		{
			name:     "months use a nominal 30 days",
			interval: pgtype.Interval{Months: 1, Valid: true},
			want:     30 * 24 * time.Hour,
		},
		{
			name:     "all three components sum",
			interval: pgtype.Interval{Months: 1, Days: 2, Microseconds: 1_000_000, Valid: true},
			want:     30*24*time.Hour + 48*time.Hour + time.Second,
		},
		{
			name:     "negative",
			interval: pgtype.Interval{Microseconds: -1_000_000, Valid: true},
			want:     -time.Second,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := intervalToDuration(test.interval); got != test.want {
				t.Errorf("intervalToDuration() = %s, want %s", got, test.want)
			}
		})
	}
}

// Round-tripping is what the task timeout actually does: written on insert, read
// back on claim. Anything at microsecond resolution or coarser must survive.
func TestIntervalRoundTrip(t *testing.T) {
	for _, d := range []time.Duration{
		0,
		time.Microsecond,
		time.Second,
		30 * time.Second,
		2 * time.Minute,
		time.Hour,
		25 * time.Hour,
		-5 * time.Minute,
	} {
		if got := intervalToDuration(durationToInterval(d)); got != d {
			t.Errorf("round trip of %s = %s", d, got)
		}
	}
}

// Durations finer than a microsecond cannot survive, so the round trip is
// expected to truncate rather than to preserve. Pinned so the loss is a known
// property of the storage format and not a surprise at dispatch.
func TestIntervalRoundTrip_TruncatesBelowMicrosecond(t *testing.T) {
	got := intervalToDuration(durationToInterval(1500 * time.Nanosecond))
	if got != time.Microsecond {
		t.Errorf("round trip of 1500ns = %s, want 1µs", got)
	}
}
