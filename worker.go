package taskq

import (
	"container/heap"
	"fmt"
	"runtime/debug"
	"time"
)

type scheduledJob struct {
	job *job
	at  time.Time
	seq uint64
}

type scheduledHeap []*scheduledJob

func (h scheduledHeap) Len() int { return len(h) }

func (h scheduledHeap) Less(i, j int) bool {
	if h[i].at.Equal(h[j].at) {
		return h[i].seq < h[j].seq
	}
	return h[i].at.Before(h[j].at)
}

func (h scheduledHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *scheduledHeap) Push(x any) {
	*h = append(*h, x.(*scheduledJob))
}

func (h *scheduledHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[:n-1]
	return item
}

func (q *Queue) worker() {
	defer q.workerWG.Done()

	for {
		select {
		case <-q.ctx.Done():
			return
		default:
		}

		select {
		case <-q.ctx.Done():
			return
		case j := <-q.ready:
			if j == nil {
				continue
			}
			q.runJob(j)
		}
	}
}

func (q *Queue) scheduler() {
	var (
		pending scheduledHeap
		timer   *time.Timer
		timerC  <-chan time.Time
	)

	stopTimer := func() {
		if timer == nil {
			return
		}
		timer.Stop()
		timer = nil
		timerC = nil
	}

	defer func() {
		stopTimer()
		for len(pending) > 0 {
			sj := heap.Pop(&pending).(*scheduledJob)
			q.dropJob(sj.job)
		}
		q.schedulerWG.Done()
	}()

	for {
		if len(pending) == 0 {
			stopTimer()
			select {
			case <-q.ctx.Done():
				return
			case j := <-q.schedule:
				if j != nil {
					heap.Push(&pending, &scheduledJob{job: j, at: j.runAt, seq: j.seq})
				}
			}
			continue
		}

		next := pending[0]
		wait := time.Until(next.at)
		if wait < 0 {
			wait = 0
		}
		stopTimer()
		timer = time.NewTimer(wait)
		timerC = timer.C

		select {
		case <-q.ctx.Done():
			return
		case j := <-q.schedule:
			if j != nil {
				heap.Push(&pending, &scheduledJob{job: j, at: j.runAt, seq: j.seq})
			}
		case <-timerC:
			now := time.Now()
			for len(pending) > 0 {
				next = pending[0]
				if next.at.After(now) {
					break
				}
				heap.Pop(&pending)
				if err := q.enqueueReady(next.job); err != nil {
					q.dropJob(next.job)
				}
			}
		}
	}
}

func (q *Queue) runJob(j *job) {
	var runErr error
	defer func() {
		if r := recover(); r != nil {
			runErr = fmt.Errorf("panic: %v\n%s", r, debug.Stack())
		}
		if runErr == nil {
			q.logf("job %s completed", j.ID)
			q.jobsWG.Done()
			return
		}
		q.finishFailedJob(j, runErr)
	}()

	runErr = j.fn(q.ctx)
}

func (q *Queue) finishFailedJob(j *job, runErr error) {
	j.attempts++

	if j.attempts >= j.MaxAttempts {
		q.logf("job %s failed after %d attempt(s): %v", j.ID, j.attempts, runErr)
		q.notifyFailed(j.ID, runErr)
		q.jobsWG.Done()
		return
	}

	delay := q.backoff(j.attempts)
	if delay < 0 {
		delay = 0
	}
	j.runAt = time.Now().Add(delay)

	q.logf("job %s retrying in %s (%d/%d): %v", j.ID, delay, j.attempts+1, j.MaxAttempts, runErr)
	if err := q.enqueueScheduled(j); err != nil {
		q.logf("job %s retry scheduling failed: %v", j.ID, err)
		q.notifyFailed(j.ID, runErr)
		q.jobsWG.Done()
	}
}
