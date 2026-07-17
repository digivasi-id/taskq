package taskq

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"
)

var (
	// ErrQueueClosed is returned by Dispatch and TryDispatch after Shutdown,
	// Stop, or Close has been called. It is also passed to the OnJobFailed
	// callback for jobs that were dropped because the queue stopped before
	// they could run.
	ErrQueueClosed = errors.New("taskq: queue closed")

	// ErrQueueFull is returned by TryDispatch when the queue buffer is full
	// and the job cannot be accepted without blocking.
	ErrQueueFull = errors.New("taskq: queue full")

	noopLogger = log.New(io.Discard, "", 0)
)

// Logger is the minimal logging interface used by the queue. It is
// satisfied by *log.Logger. Implementations must be safe for concurrent
// use, since the queue logs from multiple goroutines.
type Logger interface {
	Printf(string, ...any)
}

// BackoffFunc returns how long to wait before the next retry, given the
// number of failed attempts so far (starting at 1). Negative results are
// treated as zero.
type BackoffFunc func(attempt int) time.Duration

// Option configures a Queue at construction time.
type Option func(*Queue)

// Queue is an in-memory task queue backed by a fixed pool of worker
// goroutines. Create one with New. A Queue is safe for concurrent use by
// multiple goroutines.
type Queue struct {
	workers int

	ready    chan *job
	schedule chan *job

	ctx    context.Context
	cancel context.CancelFunc

	workerWG    sync.WaitGroup
	schedulerWG sync.WaitGroup
	jobsWG      sync.WaitGroup

	mu         sync.Mutex
	closed     bool
	closedOnce sync.Once
	doneCh     chan struct{}
	stopCh     chan struct{}
	stopOnce   sync.Once

	logger      Logger
	maxAttempts int
	backoff     BackoffFunc
	queueSize   int
	onJobFailed func(id string, err error)

	nextJobID atomic.Uint64
	nextSeq   atomic.Uint64
}

// New creates a queue with the given number of workers and starts them.
// Worker counts below 1 are treated as 1.
//
// By default the queue is silent (use WithLogger to see job lifecycle
// logs), retries jobs up to 3 times, and waits between retries using a
// jittered exponential backoff that starts at 100ms and is capped at 2s.
func New(workers int, opts ...Option) *Queue {
	if workers < 1 {
		workers = 1
	}

	q := &Queue{
		workers:     workers,
		doneCh:      make(chan struct{}),
		stopCh:      make(chan struct{}),
		logger:      noopLogger,
		maxAttempts: 3,
		backoff:     defaultBackoff,
	}
	q.ctx, q.cancel = context.WithCancel(context.Background())

	for _, opt := range opts {
		if opt != nil {
			opt(q)
		}
	}

	if q.logger == nil {
		q.logger = noopLogger
	}
	if q.maxAttempts < 1 {
		q.maxAttempts = 1
	}
	if q.backoff == nil {
		q.backoff = defaultBackoff
	}
	if q.queueSize < 1 {
		q.queueSize = workers * 4
	}
	q.ready = make(chan *job, q.queueSize)
	q.schedule = make(chan *job, q.queueSize)

	for i := 0; i < q.workers; i++ {
		q.workerWG.Add(1)
		go q.worker()
	}

	q.schedulerWG.Add(1)
	go q.scheduler()

	return q
}

// WithLogger sets the logger used for job lifecycle events (completions,
// retries, failures, drops). The queue is silent by default; passing nil
// keeps it silent.
func WithLogger(l Logger) Option {
	return func(q *Queue) {
		q.logger = l
	}
}

// WithDefaultMaxAttempts sets the default number of attempts for jobs that
// do not specify WithMaxAttempts. Values below 1 are treated as 1.
func WithDefaultMaxAttempts(max int) Option {
	return func(q *Queue) {
		q.maxAttempts = max
	}
}

// WithBackoff replaces the default retry backoff. Passing nil keeps the
// default jittered exponential backoff.
func WithBackoff(fn BackoffFunc) Option {
	return func(q *Queue) {
		q.backoff = fn
	}
}

// WithQueueSize sets the capacity of the internal job buffers. The default
// is workers*4. When the buffer is full, Dispatch blocks and TryDispatch
// returns ErrQueueFull. Values below 1 fall back to the default.
func WithQueueSize(n int) Option {
	return func(q *Queue) {
		q.queueSize = n
	}
}

// WithOnJobFailed registers a callback invoked when a job permanently
// fails: either its attempts are exhausted (err is the job's last error) or
// it was dropped because the queue stopped before it could run (err is
// ErrQueueClosed).
//
// The callback may be invoked concurrently from multiple goroutines and
// should not block; a panic inside it is recovered and logged.
func WithOnJobFailed(fn func(id string, err error)) Option {
	return func(q *Queue) {
		q.onJobFailed = fn
	}
}

// Dispatch submits a job to the queue and returns its ID. It blocks when
// the queue buffer is full until space frees up or the queue stops; use
// TryDispatch if blocking is not acceptable. It returns ErrQueueClosed
// after Shutdown, Stop, or Close has been called.
//
// Avoid calling Dispatch from inside a running job on a saturated queue:
// if every worker blocks in Dispatch, none of them can free buffer space
// and the queue deadlocks. Use TryDispatch there instead.
func (q *Queue) Dispatch(fn JobFunc, opts ...JobOption) (string, error) {
	return q.dispatch(fn, true, opts)
}

// TryDispatch is like Dispatch but never blocks: if the queue buffer is
// full the job is rejected with ErrQueueFull.
func (q *Queue) TryDispatch(fn JobFunc, opts ...JobOption) (string, error) {
	return q.dispatch(fn, false, opts)
}

func (q *Queue) dispatch(fn JobFunc, block bool, opts []JobOption) (string, error) {
	if fn == nil {
		return "", errors.New("taskq: nil job function")
	}

	q.mu.Lock()
	if q.closed {
		q.mu.Unlock()
		return "", ErrQueueClosed
	}

	j := &job{
		ID:          q.nextID(),
		fn:          fn,
		MaxAttempts: q.maxAttempts,
		runAt:       time.Now(),
	}
	for _, opt := range opts {
		if opt != nil {
			opt(j)
		}
	}
	if j.ID == "" {
		j.ID = q.nextID()
	}
	if j.MaxAttempts < 1 {
		j.MaxAttempts = 1
	}
	if j.Delay > 0 {
		j.runAt = time.Now().Add(j.Delay)
	}
	j.seq = q.nextSequence()
	q.jobsWG.Add(1)
	q.mu.Unlock()

	target := q.schedule
	enqueue := q.enqueueScheduled
	if time.Until(j.runAt) <= 0 {
		target = q.ready
		enqueue = q.enqueueReady
	}

	var err error
	if block {
		err = enqueue(j)
	} else {
		err = tryEnqueue(target, j)
	}
	if err != nil {
		q.jobsWG.Done()
		return "", err
	}
	return j.ID, nil
}

// Shutdown gracefully drains the queue: it stops accepting new jobs and
// waits until every already-accepted job — including delayed jobs and
// pending retries — has finished. Note that a job dispatched with a long
// WithDelay keeps Shutdown waiting until it runs; use Stop to cancel
// pending work instead.
//
// The context bounds only how long Shutdown waits: on ctx expiry it
// returns ctx.Err() while draining continues in the background. A nil
// context is treated as context.Background(). Shutdown is safe to call
// multiple times and concurrently with Stop.
func (q *Queue) Shutdown(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}

	q.mu.Lock()
	q.closed = true
	q.mu.Unlock()

	q.closedOnce.Do(func() {
		go q.terminate()
	})

	return q.awaitDone(ctx)
}

// Stop hard-stops the queue: it stops accepting new jobs, cancels the
// context passed to running jobs, and waits for those jobs to return.
// Delayed jobs and queued jobs that have not started are dropped (best
// effort — a job already handed to a worker may still run) and reported to
// the OnJobFailed callback with ErrQueueClosed.
//
// The context bounds only how long Stop waits, like Shutdown. Calling Stop
// while a Shutdown drain is in progress escalates it to a hard stop.
func (q *Queue) Stop(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}

	q.mu.Lock()
	q.closed = true
	q.mu.Unlock()

	q.stopOnce.Do(func() {
		close(q.stopCh)
	})
	q.closedOnce.Do(func() {
		go q.terminate()
	})

	return q.awaitDone(ctx)
}

// Close gracefully drains the queue with no time bound. It is shorthand
// for Shutdown(context.Background()).
func (q *Queue) Close() error {
	return q.Shutdown(context.Background())
}

func (q *Queue) awaitDone(ctx context.Context) error {
	select {
	case <-q.doneCh:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (q *Queue) terminate() {
	jobsDone := make(chan struct{})
	go func() {
		q.jobsWG.Wait()
		close(jobsDone)
	}()

	select {
	case <-jobsDone:
	case <-q.stopCh:
		q.cancel()
		for drained := false; !drained; {
			select {
			case j := <-q.ready:
				q.dropJob(j)
			case j := <-q.schedule:
				q.dropJob(j)
			case <-jobsDone:
				drained = true
			}
		}
	}

	q.cancel()
	q.schedulerWG.Wait()
	q.workerWG.Wait()
	close(q.doneCh)
}

func (q *Queue) nextID() string {
	return fmt.Sprintf("job-%d", q.nextJobID.Add(1))
}

func (q *Queue) nextSequence() uint64 {
	return q.nextSeq.Add(1)
}

func (q *Queue) enqueueReady(j *job) error {
	select {
	case q.ready <- j:
		return nil
	case <-q.ctx.Done():
		return ErrQueueClosed
	}
}

func (q *Queue) enqueueScheduled(j *job) error {
	select {
	case q.schedule <- j:
		return nil
	case <-q.ctx.Done():
		return ErrQueueClosed
	}
}

func tryEnqueue(ch chan *job, j *job) error {
	select {
	case ch <- j:
		return nil
	default:
		return ErrQueueFull
	}
}

func (q *Queue) dropJob(j *job) {
	q.logf("job %s dropped: %v", j.ID, ErrQueueClosed)
	q.notifyFailed(j.ID, ErrQueueClosed)
	q.jobsWG.Done()
}

func (q *Queue) notifyFailed(id string, err error) {
	if q.onJobFailed == nil {
		return
	}
	defer func() {
		if r := recover(); r != nil {
			q.logf("taskq: OnJobFailed callback panicked for job %s: %v", id, r)
		}
	}()
	q.onJobFailed(id, err)
}

func (q *Queue) logf(format string, args ...any) {
	if q.logger == nil {
		return
	}
	q.logger.Printf(format, args...)
}

func defaultBackoff(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}

	delay := 100 * time.Millisecond
	for i := 1; i < attempt; i++ {
		delay *= 2
		if delay >= 2*time.Second {
			delay = 2 * time.Second
			break
		}
	}

	half := delay / 2
	return half + time.Duration(rand.Int64N(int64(half)+1))
}
