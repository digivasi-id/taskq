package taskq

import "time"

type JobFunc func() error

type job struct {
	ID          string
	fn          JobFunc
	Delay       time.Duration
	MaxAttempts int
	attempts    int
	runAt       time.Time
	seq         uint64
}

type JobOption func(*job)

func WithDelay(delay time.Duration) JobOption {
	return func(j *job) {
		if delay < 0 {
			delay = 0
		}
		j.Delay = delay
	}
}

func WithMaxAttempts(max int) JobOption {
	return func(j *job) {
		j.MaxAttempts = max
	}
}

func WithID(id string) JobOption {
	return func(j *job) {
		j.ID = id
	}
}
