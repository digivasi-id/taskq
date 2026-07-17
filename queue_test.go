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
	if _, err := q.Dispatch(func(context.Context) error {
		close(done)
		return nil
	}); err != nil {
		t.Fatalf("dispatch: %v", err)
	}

	waitFor(t, done, time.Second, "job execution")
}

func TestTryDispatchRunsJob(t *testing.T) {
	q := testQueue(1)
	defer func() {
		if err := q.Close(); err != nil {
			t.Fatalf("close queue: %v", err)
		}
	}()

	immediate := make(chan struct{})
	delayed := make(chan struct{})

	if _, err := q.TryDispatch(func(context.Context) error {
		close(immediate)
		return nil
	}); err != nil {
		t.Fatalf("try dispatch immediate: %v", err)
	}
	if _, err := q.TryDispatch(func(context.Context) error {
		close(delayed)
		return nil
	}, WithDelay(10*time.Millisecond)); err != nil {
		t.Fatalf("try dispatch delayed: %v", err)
	}

	waitFor(t, immediate, time.Second, "immediate job execution")
	waitFor(t, delayed, time.Second, "delayed job execution")
}

func TestTryDispatchQueueFull(t *testing.T) {
	q := testQueue(1, WithQueueSize(1))
	defer func() {
		if err := q.Close(); err != nil {
			t.Fatalf("close queue: %v", err)
		}
	}()

	started := make(chan struct{})
	release := make(chan struct{})
	if _, err := q.Dispatch(func(context.Context) error {
		close(started)
		<-release
		return nil
	}); err != nil {
		t.Fatalf("dispatch blocker: %v", err)
	}
	waitFor(t, started, time.Second, "blocker start")

	if _, err := q.Dispatch(func(context.Context) error { return nil }); err != nil {
		t.Fatalf("dispatch buffered: %v", err)
	}

	if _, err := q.TryDispatch(func(context.Context) error { return nil }); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("try dispatch on full queue error = %v, want ErrQueueFull", err)
	}

	close(release)
}

func TestTryDispatchDelayedQueueFull(t *testing.T) {
	q := &Queue{
		ready:       make(chan *job, 1),
		schedule:    make(chan *job, 1),
		maxAttempts: 1,
	}
	q.schedule <- &job{}

	if _, err := q.TryDispatch(func(context.Context) error { return nil }, WithDelay(time.Minute)); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("try dispatch delayed on full queue error = %v, want ErrQueueFull", err)
	}
}

func TestWithQueueSize(t *testing.T) {
	q := testQueue(1, WithQueueSize(2))
	defer func() {
		if err := q.Close(); err != nil {
			t.Fatalf("close queue: %v", err)
		}
	}()

	if cap(q.ready) != 2 || cap(q.schedule) != 2 {
		t.Fatalf("buffer caps = %d/%d, want 2/2", cap(q.ready), cap(q.schedule))
	}
}

func TestShutdownRejectsNewJobs(t *testing.T) {
	q := testQueue(1)

	block := make(chan struct{})
	if _, err := q.Dispatch(func(context.Context) error {
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

	if _, err := q.Dispatch(func(context.Context) error { return nil }); !errors.Is(err, ErrQueueClosed) {
		t.Fatalf("dispatch after shutdown error = %v, want ErrQueueClosed", err)
	}
}

func TestStopSignalsRunningJob(t *testing.T) {
	q := testQueue(1)

	started := make(chan struct{})
	if _, err := q.Dispatch(func(ctx context.Context) error {
		close(started)
		<-ctx.Done()
		return nil
	}); err != nil {
		t.Fatalf("dispatch: %v", err)
	}
	waitFor(t, started, time.Second, "job start")

	if err := q.Stop(nil); err != nil {
		t.Fatalf("stop: %v", err)
	}

	if _, err := q.Dispatch(func(context.Context) error { return nil }); !errors.Is(err, ErrQueueClosed) {
		t.Fatalf("dispatch after stop error = %v, want ErrQueueClosed", err)
	}
}

func TestStopCancelsPendingDelayedJobs(t *testing.T) {
	dropped := make(chan string, 1)
	q := testQueue(1, WithOnJobFailed(func(id string, err error) {
		if errors.Is(err, ErrQueueClosed) {
			dropped <- id
		}
	}))

	id, err := q.Dispatch(func(context.Context) error {
		t.Error("delayed job ran despite Stop")
		return nil
	}, WithDelay(time.Hour))
	if err != nil {
		t.Fatalf("dispatch: %v", err)
	}

	time.Sleep(50 * time.Millisecond)

	if err := q.Stop(context.Background()); err != nil {
		t.Fatalf("stop: %v", err)
	}

	select {
	case got := <-dropped:
		if got != id {
			t.Fatalf("dropped job = %s, want %s", got, id)
		}
	default:
		t.Fatal("expected drop callback for pending delayed job")
	}
}

func TestStopEscalatesActiveShutdown(t *testing.T) {
	q := testQueue(1)

	if _, err := q.Dispatch(func(context.Context) error { return nil }, WithDelay(time.Hour)); err != nil {
		t.Fatalf("dispatch: %v", err)
	}

	shutdownErr := make(chan error, 1)
	go func() {
		shutdownErr <- q.Shutdown(context.Background())
	}()

	time.Sleep(50 * time.Millisecond)

	if err := q.Stop(context.Background()); err != nil {
		t.Fatalf("stop: %v", err)
	}
	if err := <-shutdownErr; err != nil {
		t.Fatalf("shutdown: %v", err)
	}
}

func TestTerminateDropsBufferedJobs(t *testing.T) {
	dropped := make(chan string, 2)
	q := &Queue{
		ready:    make(chan *job, 1),
		schedule: make(chan *job, 1),
		doneCh:   make(chan struct{}),
		stopCh:   make(chan struct{}),
		onJobFailed: func(id string, err error) {
			dropped <- id
		},
	}
	q.ctx, q.cancel = context.WithCancel(context.Background())

	q.ready <- &job{ID: "buffered-ready"}
	q.schedule <- &job{ID: "buffered-schedule"}
	q.jobsWG.Add(2)
	close(q.stopCh)

	go q.terminate()
	waitFor(t, q.doneCh, time.Second, "terminate")

	if len(dropped) != 2 {
		t.Fatalf("dropped %d jobs, want 2", len(dropped))
	}
}

func TestOnJobFailedCalledOnExhaustedAttempts(t *testing.T) {
	type failure struct {
		id  string
		err error
	}
	failed := make(chan failure, 1)
	q := testQueue(1, WithOnJobFailed(func(id string, err error) {
		failed <- failure{id: id, err: err}
	}))
	defer func() {
		if err := q.Close(); err != nil {
			t.Fatalf("close queue: %v", err)
		}
	}()

	jobErr := errors.New("boom")
	id, err := q.Dispatch(func(context.Context) error { return jobErr }, WithMaxAttempts(1))
	if err != nil {
		t.Fatalf("dispatch: %v", err)
	}

	select {
	case f := <-failed:
		if f.id != id {
			t.Fatalf("failed job = %s, want %s", f.id, id)
		}
		if !errors.Is(f.err, jobErr) {
			t.Fatalf("failure error = %v, want %v", f.err, jobErr)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for failure callback")
	}
}

func TestOnJobFailedPanicIsRecovered(t *testing.T) {
	q := testQueue(1, WithOnJobFailed(func(string, error) {
		panic("callback boom")
	}))
	defer func() {
		if err := q.Close(); err != nil {
			t.Fatalf("close queue: %v", err)
		}
	}()

	failedRan := make(chan struct{})
	if _, err := q.Dispatch(func(context.Context) error {
		defer close(failedRan)
		return errors.New("boom")
	}, WithMaxAttempts(1)); err != nil {
		t.Fatalf("dispatch failing job: %v", err)
	}
	waitFor(t, failedRan, time.Second, "failing job execution")

	done := make(chan struct{})
	if _, err := q.Dispatch(func(context.Context) error {
		close(done)
		return nil
	}); err != nil {
		t.Fatalf("dispatch after callback panic: %v", err)
	}
	waitFor(t, done, time.Second, "job execution after callback panic")
}

func TestNewAppliesFallbacks(t *testing.T) {
	q := New(0, WithLogger(nil), WithDefaultMaxAttempts(0), WithBackoff(nil), WithQueueSize(0), nil)
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
	if cap(q.ready) != 4 || cap(q.schedule) != 4 {
		t.Fatalf("buffer caps = %d/%d, want 4/4", cap(q.ready), cap(q.schedule))
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
	id, err := q.Dispatch(func(context.Context) error {
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
	for i := 0; i < 20; i++ {
		if got := defaultBackoff(0); got < 50*time.Millisecond || got > 100*time.Millisecond {
			t.Fatalf("defaultBackoff(0) = %s, want in [50ms, 100ms]", got)
		}
		if got := defaultBackoff(2); got < 100*time.Millisecond || got > 200*time.Millisecond {
			t.Fatalf("defaultBackoff(2) = %s, want in [100ms, 200ms]", got)
		}
		if got := defaultBackoff(10); got < time.Second || got > 2*time.Second {
			t.Fatalf("defaultBackoff(10) = %s, want in [1s, 2s]", got)
		}
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
	if _, err := q1.Dispatch(func(context.Context) error { return nil }); !errors.Is(err, ErrQueueClosed) {
		t.Fatalf("dispatch immediate error = %v, want ErrQueueClosed", err)
	}

	ctx2, cancel2 := context.WithCancel(context.Background())
	cancel2()
	q2 := &Queue{
		ctx:      ctx2,
		ready:    make(chan *job),
		schedule: make(chan *job),
	}
	if _, err := q2.Dispatch(func(context.Context) error { return nil }, WithDelay(time.Second)); !errors.Is(err, ErrQueueClosed) {
		t.Fatalf("dispatch delayed error = %v, want ErrQueueClosed", err)
	}
}
