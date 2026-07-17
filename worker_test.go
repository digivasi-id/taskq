package taskq

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestDelayDefersExecution(t *testing.T) {
	q := testQueue(1)
	defer func() {
		if err := q.Close(); err != nil {
			t.Fatalf("close queue: %v", err)
		}
	}()

	started := make(chan struct{}, 1)
	release := make(chan struct{})

	if _, err := q.Dispatch(func(context.Context) error {
		started <- struct{}{}
		<-release
		return nil
	}, WithDelay(75*time.Millisecond)); err != nil {
		t.Fatalf("dispatch: %v", err)
	}

	select {
	case <-started:
		t.Fatal("job started before delay elapsed")
	case <-time.After(30 * time.Millisecond):
	}

	delayed := make(chan struct{})
	go func() {
		<-started
		close(delayed)
	}()
	waitFor(t, delayed, time.Second, "delayed start")

	close(release)
}

func TestWorkersRunInParallel(t *testing.T) {
	q := testQueue(2)
	defer func() {
		if err := q.Close(); err != nil {
			t.Fatalf("close queue: %v", err)
		}
	}()

	var started atomic.Int32
	release := make(chan struct{})
	ready := make(chan struct{})
	var once sync.Once

	job := func(context.Context) error {
		if started.Add(1) == 2 {
			once.Do(func() { close(ready) })
		}
		<-release
		return nil
	}

	if _, err := q.Dispatch(job); err != nil {
		t.Fatalf("dispatch 1: %v", err)
	}
	if _, err := q.Dispatch(job); err != nil {
		t.Fatalf("dispatch 2: %v", err)
	}

	waitFor(t, ready, time.Second, "parallel worker start")
	close(release)
}

func TestWorkerAndSchedulerIgnoreNilJobs(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	q := &Queue{
		ctx:      ctx,
		ready:    make(chan *job),
		schedule: make(chan *job),
	}

	q.workerWG.Add(1)
	q.schedulerWG.Add(1)

	workerDone := make(chan struct{})
	schedulerDone := make(chan struct{})
	go func() {
		q.worker()
		close(workerDone)
	}()
	go func() {
		q.scheduler()
		close(schedulerDone)
	}()

	readySent := make(chan struct{})
	scheduleSent := make(chan struct{})
	go func() {
		q.ready <- nil
		close(readySent)
	}()
	go func() {
		q.schedule <- nil
		close(scheduleSent)
	}()

	waitFor(t, readySent, time.Second, "ready nil send")
	waitFor(t, scheduleSent, time.Second, "schedule nil send")
	cancel()

	waitFor(t, workerDone, time.Second, "worker shutdown")
	waitFor(t, schedulerDone, time.Second, "scheduler shutdown")
}

func TestScheduledHeapTieBreak(t *testing.T) {
	now := time.Now()
	h := scheduledHeap{
		{at: now, seq: 1},
		{at: now, seq: 2},
	}

	if !h.Less(0, 1) {
		t.Fatal("expected earlier sequence to sort first when times match")
	}
	if h.Less(1, 0) {
		t.Fatal("expected later sequence to sort after earlier one when times match")
	}
}

func TestSchedulerStopsOnContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	q := &Queue{
		ctx:      ctx,
		schedule: make(chan *job),
	}

	q.schedulerWG.Add(1)
	done := make(chan struct{})
	go func() {
		q.scheduler()
		close(done)
	}()

	time.Sleep(10 * time.Millisecond)
	cancel()
	waitFor(t, done, time.Second, "scheduler shutdown")
}

func TestSchedulerHandlesPastDueJob(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	q := &Queue{
		ctx:      ctx,
		ready:    make(chan *job, 1),
		schedule: make(chan *job, 1),
	}

	q.schedulerWG.Add(1)
	done := make(chan struct{})
	go func() {
		q.scheduler()
		close(done)
	}()

	q.schedule <- &job{runAt: time.Now().Add(-time.Second), seq: 1}

	select {
	case <-q.ready:
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for ready job")
	}

	cancel()
	waitFor(t, done, time.Second, "scheduler shutdown")
}

func TestSchedulerReceivesWhileTimerActive(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	q := &Queue{
		ctx:      ctx,
		ready:    make(chan *job, 1),
		schedule: make(chan *job),
	}

	q.schedulerWG.Add(1)
	q.jobsWG.Add(2)
	done := make(chan struct{})
	go func() {
		q.scheduler()
		close(done)
	}()

	firstSent := make(chan struct{})
	go func() {
		q.schedule <- &job{runAt: time.Now().Add(500 * time.Millisecond), seq: 1}
		close(firstSent)
	}()
	waitFor(t, firstSent, time.Second, "first schedule send")

	time.Sleep(100 * time.Millisecond)

	secondSent := make(chan struct{})
	go func() {
		q.schedule <- &job{runAt: time.Now().Add(600 * time.Millisecond), seq: 2}
		close(secondSent)
	}()
	waitFor(t, secondSent, time.Second, "second schedule send")

	cancel()
	waitFor(t, done, time.Second, "scheduler shutdown")
}

func TestSchedulerCancelsWhileEnqueuingReady(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	q := &Queue{
		ctx:      ctx,
		ready:    make(chan *job),
		schedule: make(chan *job, 1),
	}

	q.schedulerWG.Add(1)
	q.jobsWG.Add(1)
	done := make(chan struct{})
	go func() {
		q.scheduler()
		close(done)
	}()

	q.schedule <- &job{runAt: time.Now().Add(-time.Second), seq: 1}
	time.Sleep(100 * time.Millisecond)
	cancel()
	waitFor(t, done, time.Second, "scheduler cancellation while enqueuing ready")
}

func TestPanicJobFailsWithStackTrace(t *testing.T) {
	failed := make(chan error, 1)
	q := testQueue(1, WithOnJobFailed(func(id string, err error) {
		failed <- err
	}))

	if _, err := q.Dispatch(func(context.Context) error {
		panic("boom")
	}, WithMaxAttempts(1)); err != nil {
		t.Fatalf("dispatch: %v", err)
	}

	select {
	case err := <-failed:
		if !strings.Contains(err.Error(), "panic: boom") {
			t.Fatalf("failure error = %v, want panic message", err)
		}
		if !strings.Contains(err.Error(), "goroutine") {
			t.Fatalf("failure error = %v, want stack trace", err)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for failure callback")
	}

	if err := q.Shutdown(context.Background()); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
}

func TestRetryWithNegativeBackoff(t *testing.T) {
	q := testQueue(1, WithDefaultMaxAttempts(2), WithBackoff(func(int) time.Duration {
		return -time.Second
	}))
	defer func() {
		if err := q.Close(); err != nil {
			t.Fatalf("close queue: %v", err)
		}
	}()

	done := make(chan struct{})
	var attempts atomic.Int32
	_, err := q.Dispatch(func(context.Context) error {
		if attempts.Add(1) == 1 {
			return errors.New("retry")
		}
		close(done)
		return nil
	})
	if err != nil {
		t.Fatalf("dispatch: %v", err)
	}

	waitFor(t, done, time.Second, "negative backoff retry")
}

func TestFinishFailedJobCancelsRetryScheduling(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	q := &Queue{
		ctx:      ctx,
		schedule: make(chan *job),
		backoff:  func(int) time.Duration { return time.Millisecond },
		logger:   noopLogger,
	}

	q.jobsWG.Add(1)
	q.finishFailedJob(&job{
		ID:          "retry-job",
		MaxAttempts: 2,
		runAt:       time.Now(),
		seq:         1,
	}, errors.New("retry"))
}

func TestDelayedJobsRunInOrder(t *testing.T) {
	q := testQueue(1)
	defer func() {
		if err := q.Close(); err != nil {
			t.Fatalf("close queue: %v", err)
		}
	}()

	first := make(chan struct{})
	second := make(chan struct{})

	if _, err := q.Dispatch(func(context.Context) error {
		close(first)
		return nil
	}, WithDelay(20*time.Millisecond)); err != nil {
		t.Fatalf("dispatch first: %v", err)
	}
	if _, err := q.Dispatch(func(context.Context) error {
		close(second)
		return nil
	}, WithDelay(40*time.Millisecond)); err != nil {
		t.Fatalf("dispatch second: %v", err)
	}

	waitFor(t, first, time.Second, "first delayed job")
	waitFor(t, second, time.Second, "second delayed job")
}

func TestShutdownWithNilAndCanceledContext(t *testing.T) {
	q1 := testQueue(1)
	if err := q1.Shutdown(nil); err != nil {
		t.Fatalf("shutdown with nil context: %v", err)
	}

	q2 := testQueue(1)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := q2.Shutdown(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("shutdown with canceled context error = %v, want context.Canceled", err)
	}
}
