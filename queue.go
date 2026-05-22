package taskq

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

var (
	ErrQueueClosed = errors.New("taskq: queue closed")
	noopLogger     = log.New(io.Discard, "", 0)
)

type Logger interface {
	Printf(string, ...any)
}

type BackoffFunc func(attempt int) time.Duration

type Option func(*Queue)

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

	logger      Logger
	maxAttempts int
	backoff     BackoffFunc

	nextJobID atomic.Uint64
	nextSeq   atomic.Uint64
}

func New(workers int, opts ...Option) *Queue {
	if workers < 1 {
		workers = 1
	}

	q := &Queue{
		workers:     workers,
		ready:       make(chan *job, workers*4),
		schedule:    make(chan *job, workers*4),
		doneCh:      make(chan struct{}),
		logger:      log.Default(),
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

	for i := 0; i < q.workers; i++ {
		q.workerWG.Add(1)
		go q.worker()
	}

	q.schedulerWG.Add(1)
	go q.scheduler()

	return q
}

func WithLogger(l Logger) Option {
	return func(q *Queue) {
		q.logger = l
	}
}

func WithDefaultMaxAttempts(max int) Option {
	return func(q *Queue) {
		q.maxAttempts = max
	}
}

func WithBackoff(fn BackoffFunc) Option {
	return func(q *Queue) {
		q.backoff = fn
	}
}

func (q *Queue) Dispatch(fn JobFunc, opts ...JobOption) (string, error) {
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

	if time.Until(j.runAt) <= 0 {
		if err := q.enqueueReady(j); err != nil {
			q.jobsWG.Done()
			return "", err
		}
		return j.ID, nil
	}

	if err := q.enqueueScheduled(j); err != nil {
		q.jobsWG.Done()
		return "", err
	}
	return j.ID, nil
}

func (q *Queue) Shutdown(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}

	q.mu.Lock()
	q.closed = true
	q.mu.Unlock()

	q.closedOnce.Do(func() {
		go func() {
			q.jobsWG.Wait()
			q.cancel()
			q.schedulerWG.Wait()
			q.workerWG.Wait()
			close(q.doneCh)
		}()
	})

	select {
	case <-q.doneCh:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (q *Queue) Close() error {
	return q.Shutdown(context.Background())
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
			return 2 * time.Second
		}
	}
	return delay
}
