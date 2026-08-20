package engine

import (
	"testing"
	"time"
)

func TestBackoff_Exponential(t *testing.T) {
	// Jitter off, so the growth curve itself is under test.
	backoff := Backoff{Base: time.Second, Max: time.Hour}

	tests := []struct {
		attempt int
		want    time.Duration
	}{
		{1, time.Second},
		{2, 2 * time.Second},
		{3, 4 * time.Second},
		{4, 8 * time.Second},
		{5, 16 * time.Second},
	}

	for _, test := range tests {
		if got := backoff.For(test.attempt); got != test.want {
			t.Errorf("For(%d) = %s, want %s", test.attempt, got, test.want)
		}
	}
}

func TestBackoff_RespectsMax(t *testing.T) {
	backoff := Backoff{Base: time.Second, Max: 10 * time.Second}

	for attempt := 1; attempt <= 20; attempt++ {
		if got := backoff.For(attempt); got > backoff.Max {
			t.Errorf("For(%d) = %s, want no more than %s", attempt, got, backoff.Max)
		}
	}
}

// A large attempt count must not overflow the shift into a negative delay,
// which would schedule a retry in the past and produce a hot loop.
func TestBackoff_DoesNotOverflow(t *testing.T) {
	backoff := Backoff{Base: time.Second, Max: time.Hour}

	for _, attempt := range []int{62, 63, 64, 128, 1_000_000} {
		got := backoff.For(attempt)
		if got <= 0 {
			t.Errorf("For(%d) = %s, want a positive delay", attempt, got)
		}
		if got > backoff.Max {
			t.Errorf("For(%d) = %s, want no more than %s", attempt, got, backoff.Max)
		}
	}
}

// An uncapped backoff still must not overflow.
func TestBackoff_NoMaxDoesNotOverflow(t *testing.T) {
	backoff := Backoff{Base: time.Second}

	if got := backoff.For(1_000_000); got <= 0 {
		t.Errorf("For(1000000) = %s, want a positive delay", got)
	}
}

func TestBackoff_ClampsAttemptFloor(t *testing.T) {
	backoff := Backoff{Base: time.Second, Max: time.Hour}

	for _, attempt := range []int{0, -1, -100} {
		if got := backoff.For(attempt); got != time.Second {
			t.Errorf("For(%d) = %s, want the base delay", attempt, got)
		}
	}
}

// Jitter spreads retries below the target rather than around it, so the cap
// stays meaningful — a symmetric jitter would schedule some retries past Max.
func TestBackoff_JitterStaysWithinTheWindow(t *testing.T) {
	backoff := Backoff{Base: 10 * time.Second, Max: time.Hour, Jitter: 0.5}

	for range 500 {
		got := backoff.For(1)
		if got > 10*time.Second {
			t.Fatalf("For(1) = %s, want no more than the 10s target", got)
		}
		if got < 5*time.Second {
			t.Fatalf("For(1) = %s, want at least 5s with 50%% jitter", got)
		}
	}
}

// The point of jitter: a provider outage fails every task at once, and without
// spread every node retries in lockstep and knocks the provider over again.
func TestBackoff_JitterActuallyVaries(t *testing.T) {
	backoff := Backoff{Base: time.Second, Max: time.Minute, Jitter: 1.0}

	seen := map[time.Duration]bool{}
	for range 200 {
		seen[backoff.For(3)] = true
	}

	if len(seen) < 50 {
		t.Errorf("200 calls produced %d distinct delays; retries are not being spread", len(seen))
	}
}

func TestBackoff_ZeroJitterIsDeterministic(t *testing.T) {
	backoff := Backoff{Base: time.Second, Max: time.Minute, Jitter: 0}

	first := backoff.For(3)
	for range 50 {
		if got := backoff.For(3); got != first {
			t.Fatalf("For(3) returned %s then %s with jitter off", first, got)
		}
	}
}

// Out-of-range jitter is clamped rather than producing a negative window.
func TestBackoff_ClampsJitter(t *testing.T) {
	for _, jitter := range []float64{-1, 2, 100} {
		backoff := Backoff{Base: 10 * time.Second, Max: time.Hour, Jitter: jitter}

		for range 100 {
			got := backoff.For(1)
			if got < 0 || got > 10*time.Second {
				t.Fatalf("jitter %v produced %s, want it within [0s, 10s]", jitter, got)
			}
		}
	}
}

func TestDefaultBackoff_IsSane(t *testing.T) {
	if DefaultBackoff.Base <= 0 {
		t.Error("DefaultBackoff.Base must be positive")
	}
	if DefaultBackoff.Max < DefaultBackoff.Base {
		t.Error("DefaultBackoff.Max must be at least Base")
	}
	for attempt := 1; attempt <= 10; attempt++ {
		got := DefaultBackoff.For(attempt)
		if got <= 0 || got > DefaultBackoff.Max {
			t.Errorf("For(%d) = %s, outside (0, %s]", attempt, got, DefaultBackoff.Max)
		}
	}
}
