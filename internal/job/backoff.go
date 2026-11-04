package job

import (
	"math/rand/v2"
	"time"
)

// Backoff returns the retry delay after the given attempt (1-based):
// base*2^(attempt-1), capped at max, then randomized by ±jitter (0.2 = ±20%).
//
// It uses randomness, so callers must compute it outside any deterministic
// state machine and pass the result in.
func Backoff(attempt int, base, max time.Duration, jitter float64) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	d := base
	for i := 1; i < attempt && d < max; i++ {
		d *= 2
	}
	if d > max {
		d = max
	}
	if jitter > 0 {
		f := 1 + jitter*(2*rand.Float64()-1)
		d = time.Duration(float64(d) * f)
	}
	return d
}

// DefaultBackoff is Backoff with the defaults: base 1s, max 10m, jitter 20%.
func DefaultBackoff(attempt int) time.Duration {
	return Backoff(attempt, time.Second, 10*time.Minute, 0.2)
}
