package taskq

import (
	"context"
	"errors"
	"io"
	"log"
	"testing"
	"time"
)

func testQueue(workers int, opts ...Option) *Queue {
	all := append([]Option{WithLogger(log.New(io.Discard, "", 0))}, opts...)
	return New(workers, all...)
}

func waitFor(t *testing.T, ch <-chan struct{}, d time.Duration, msg string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(d):
		t.Fatalf("timeout waiting for %s", msg)
	}
}

func TestDispatchRunsJob(t *testing.T) {
	q := testQueue(1)
	defer func() {
		if err := q.Close(); err != nil {
			t.Fatalf("close queue: %v", err)
		}
	}()

	done := make(chan struct{})
	if _, err := q.Dispatch(func() error {
		close(done)
		return nil
	}); err != nil {
		t.Fatalf("dispatch: %v", err)
	}

	waitFor(t, done, time.Second, "job execution")
}

func TestShutdownRejectsNewJobs(t *testing.T) {
	q := testQueue(1)

	block := make(chan struct{})
	if _, err := q.Dispatch(func() error {
		<-block
		return nil
	}); err != nil {
		t.Fatalf("dispatch: %v", err)
	}

	shutdownDone := make(chan error, 1)
	go func() {
		shutdownDone <- q.Shutdown(context.Background())
	}()

	close(block)

	if err := <-shutdownDone; err != nil {
		t.Fatalf("shutdown: %v", err)
	}

	if _, err := q.Dispatch(func() error { return nil }); !errors.Is(err, ErrQueueClosed) {
		t.Fatalf("dispatch after shutdown error = %v, want ErrQueueClosed", err)
	}
}

func TestNewAppliesFallbacks(t *testing.T) {
	q := New(0, WithLogger(nil), WithDefaultMaxAttempts(0), WithBackoff(nil), nil)
	defer func() {
		if err := q.Close(); err != nil {
			t.Fatalf("close queue: %v", err)
		}
	}()

	if q.workers != 1 {
		t.Fatalf("workers = %d, want 1", q.workers)
	}
	if q.logger == nil {
		t.Fatal("logger fallback was not applied")
	}
	if q.maxAttempts != 1 {
		t.Fatalf("maxAttempts = %d, want 1", q.maxAttempts)
	}
	if q.backoff == nil {
		t.Fatal("backoff fallback was not applied")
	}
}

func TestLogfHandlesNilLogger(t *testing.T) {
	q := testQueue(1)
	defer func() {
		if err := q.Close(); err != nil {
			t.Fatalf("close queue: %v", err)
		}
	}()

	q.logger = nil
	q.logf("ignored")
}

func TestDispatchHandlesEmptyAndNegativeJobOptions(t *testing.T) {
	q := testQueue(1)
	defer func() {
		if err := q.Close(); err != nil {
			t.Fatalf("close queue: %v", err)
		}
	}()

	done := make(chan struct{})
	id, err := q.Dispatch(func() error {
		close(done)
		return nil
	}, WithID(""), WithMaxAttempts(0), WithDelay(-time.Second))
	if err != nil {
		t.Fatalf("dispatch: %v", err)
	}
	if id == "" {
		t.Fatal("expected generated job ID")
	}

	waitFor(t, done, time.Second, "job execution")
}

func TestDispatchRejectsNilFunction(t *testing.T) {
	q := testQueue(1)
	defer func() {
		if err := q.Close(); err != nil {
			t.Fatalf("close queue: %v", err)
		}
	}()

	if _, err := q.Dispatch(nil); err == nil {
		t.Fatal("expected error for nil job function")
	}
}

func TestDefaultBackoff(t *testing.T) {
	if got := defaultBackoff(0); got != 100*time.Millisecond {
		t.Fatalf("defaultBackoff(0) = %s, want 100ms", got)
	}
	if got := defaultBackoff(10); got != 2*time.Second {
		t.Fatalf("defaultBackoff(10) = %s, want 2s", got)
	}
}

func TestInternalQueueCancellationPaths(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	q := &Queue{
		ready:    make(chan *job),
		schedule: make(chan *job),
		ctx:      ctx,
	}

	if err := q.enqueueReady(&job{}); !errors.Is(err, ErrQueueClosed) {
		t.Fatalf("enqueueReady error = %v, want ErrQueueClosed", err)
	}
	if err := q.enqueueScheduled(&job{}); !errors.Is(err, ErrQueueClosed) {
		t.Fatalf("enqueueScheduled error = %v, want ErrQueueClosed", err)
	}
}

func TestDispatchReturnsErrQueueClosedWhenCanceled(t *testing.T) {
	ctx1, cancel1 := context.WithCancel(context.Background())
	cancel1()
	q1 := &Queue{
		ctx:      ctx1,
		ready:    make(chan *job),
		schedule: make(chan *job),
	}
	if _, err := q1.Dispatch(func() error { return nil }); !errors.Is(err, ErrQueueClosed) {
		t.Fatalf("dispatch immediate error = %v, want ErrQueueClosed", err)
	}

	ctx2, cancel2 := context.WithCancel(context.Background())
	cancel2()
	q2 := &Queue{
		ctx:      ctx2,
		ready:    make(chan *job),
		schedule: make(chan *job),
	}
	if _, err := q2.Dispatch(func() error { return nil }, WithDelay(time.Second)); !errors.Is(err, ErrQueueClosed) {
		t.Fatalf("dispatch delayed error = %v, want ErrQueueClosed", err)
	}
}
