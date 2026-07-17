package taskq

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func TestJobOptions(t *testing.T) {
	var j job

	WithDelay(2 * time.Second)(&j)
	if j.Delay != 2*time.Second {
		t.Fatalf("delay = %s, want %s", j.Delay, 2*time.Second)
	}

	WithMaxAttempts(5)(&j)
	if j.MaxAttempts != 5 {
		t.Fatalf("max attempts = %d, want 5", j.MaxAttempts)
	}

	WithID("custom-id")(&j)
	if j.ID != "custom-id" {
		t.Fatalf("id = %q, want custom-id", j.ID)
	}
}

func TestJobRetriesUntilSuccess(t *testing.T) {
	q := testQueue(1, WithDefaultMaxAttempts(4), WithBackoff(func(attempt int) time.Duration {
		return 10 * time.Millisecond
	}))
	defer func() {
		if err := q.Close(); err != nil {
			t.Fatalf("close queue: %v", err)
		}
	}()

	var attempts atomic.Int32
	done := make(chan struct{})

	if _, err := q.Dispatch(func(context.Context) error {
		if attempts.Add(1) < 3 {
			return errors.New("retry me")
		}
		close(done)
		return nil
	}); err != nil {
		t.Fatalf("dispatch: %v", err)
	}

	waitFor(t, done, time.Second, "retry completion")
	if got := attempts.Load(); got != 3 {
		t.Fatalf("attempts = %d, want 3", got)
	}
}
