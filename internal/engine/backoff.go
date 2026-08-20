package engine

import (
	"math"
	"math/rand/v2"
	"time"
)

// Backoff schedules a failed task's next attempt.
//
// Jitter is not decoration. A provider outage fails every in-flight task at
// once, and without jitter every node retries them in lockstep — the retry
// stampede arrives at the recovering provider as a synchronised spike and knocks
// it over again. Spreading the retries across the window is what turns a
// thundering herd into a ramp.
type Backoff struct {
	// Base is the delay after the first failure.
	Base time.Duration

	// Max caps the exponential growth, so a long-lived task with a generous
	// retry budget does not end up scheduled hours out.
	Max time.Duration

	// Jitter is the fraction of the computed delay that is randomised, from 0
	// (none) to 1 (the delay is uniform across the whole window).
	Jitter float64
}

// DefaultBackoff is a conservative starting point: quick first retry for
// transient blips, capped at five minutes, fully jittered.
var DefaultBackoff = Backoff{
	Base:   2 * time.Second,
	Max:    5 * time.Minute,
	Jitter: 1.0,
}

// For returns how long to wait before attempt number `attempt` is eligible
// again. attempt is the number of attempts already made, so the first failure
// passes 1.
//
// The delay is Base * 2^(attempt-1), capped at Max, then jittered downward.
// Jittering down rather than around the target keeps the cap meaningful — a
// symmetric jitter would schedule some retries past Max.
func (b Backoff) For(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}

	base := b.Base
	if base <= 0 {
		base = time.Second
	}

	// Doubling rather than math.Pow, with the overflow check before the double
	// rather than a clamp on the exponent. time.Duration is int64 nanoseconds,
	// so an uncapped backoff wraps to zero or negative somewhere past attempt
	// 62 — and a negative delay schedules the retry in the past, turning the
	// backoff into a hot loop. Saturating is the only safe direction.
	delay := base
	for range attempt - 1 {
		if delay > math.MaxInt64/2 {
			delay = math.MaxInt64
			break
		}
		delay *= 2
		if b.Max > 0 && delay >= b.Max {
			delay = b.Max
			break
		}
	}

	if b.Max > 0 && delay > b.Max {
		delay = b.Max
	}

	jitter := math.Min(math.Max(b.Jitter, 0), 1)
	if jitter == 0 {
		return delay
	}

	// Uniform in [delay*(1-jitter), delay].
	floor := time.Duration(float64(delay) * (1 - jitter))
	spread := delay - floor
	if spread <= 0 {
		return delay
	}
	return floor + time.Duration(rand.Int64N(int64(spread)))
}
