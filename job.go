package taskq

import (
	"context"
	"time"
)

// JobFunc is the unit of work executed by the queue.
//
// The context passed to the function is the queue's lifetime context. It is
// cancelled when Stop is called, so long-running jobs should honor ctx to
// exit promptly during a hard stop. During a graceful Shutdown or Close the
// context remains valid until every job has finished.
type JobFunc func(ctx context.Context) error

type job struct {
	ID          string
	fn          JobFunc
	Delay       time.Duration
	MaxAttempts int
	attempts    int
	runAt       time.Time
	seq         uint64
}

// JobOption configures a single job at dispatch time.
type JobOption func(*job)

// WithDelay schedules the job to run after the given delay instead of
// immediately. Negative delays are treated as zero.
func WithDelay(delay time.Duration) JobOption {
	return func(j *job) {
		if delay < 0 {
			delay = 0
		}
		j.Delay = delay
	}
}

// WithMaxAttempts sets how many times the job may run before it is
// considered permanently failed. Values below 1 are treated as 1. It
// overrides the queue-wide default set by WithDefaultMaxAttempts.
func WithMaxAttempts(max int) JobOption {
	return func(j *job) {
		j.MaxAttempts = max
	}
}

// WithID assigns a custom job ID, used in logs and failure callbacks. If it
// is empty (or the option is omitted) the queue generates an ID.
func WithID(id string) JobOption {
	return func(j *job) {
		j.ID = id
	}
}
