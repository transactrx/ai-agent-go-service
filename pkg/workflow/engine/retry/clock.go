package retry

import "time"

// clock is the time-source seam the helper uses so tests can fast-forward.
// Exported types stay on time.Time / time.Duration; only the source is
// abstracted.
type clock interface {
	Now() time.Time
	NewTimer(d time.Duration) clockTimer
}

// clockTimer mirrors *time.Timer for the minimal surface area we use.
type clockTimer interface {
	C() <-chan time.Time
	Stop() bool
}

type systemClock struct{}

func (systemClock) Now() time.Time                          { return time.Now() }
func (systemClock) NewTimer(d time.Duration) clockTimer     { return systemTimer{t: time.NewTimer(d)} }

type systemTimer struct{ t *time.Timer }

func (s systemTimer) C() <-chan time.Time { return s.t.C }
func (s systemTimer) Stop() bool          { return s.t.Stop() }
