[![Build Status](https://github.com/digivasi-id/taskq/actions/workflows/ci.yml/badge.svg)](https://github.com/digivasi-id/taskq/actions/workflows/ci.yml)
[![Coverage](https://img.shields.io/endpoint?url=https%3A%2F%2Fraw.githubusercontent.com%2Fdigivasi-id%2Ftaskq%2Fbadges%2Fcoverage.json)](https://github.com/digivasi-id/taskq/actions/workflows/coverage.yml)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)
[![Go Reference](https://img.shields.io/badge/go.dev-reference-blue?logo=go&logoColor=white)](https://pkg.go.dev/github.com/digivasi-id/taskq)

# taskq - Lightweight In-Memory Task Queue

taskq is a small in-memory task queue for Go.
It lets your app run background jobs without Redis, Kafka, or other extra services.

## Example usage

```go
package main

import (
	"context"
	"fmt"

	taskq "github.com/digivasi-id/taskq"
)

func main() {
	q := taskq.New(5)
	defer q.Close()

	_, _ = q.Dispatch(func(ctx context.Context) error {
		fmt.Println("job executed")
		return nil
	})
}
```

## Features

- worker pool
- retries with max attempts
- jittered exponential backoff
- delayed jobs
- context-aware jobs (cancelled on hard stop)
- automatic job IDs
- non-blocking dispatch (`TryDispatch`)
- failure callback for permanently failed jobs
- graceful drain and hard stop
- zero dependencies

## How to use

Create one queue when your app starts, store it in your service, and call `Dispatch` whenever you want to send work to the background.

```go
type App struct {
	Queue *taskq.Queue
}

func NewApp() *App {
	return &App{Queue: taskq.New(5)}
}
```

`Dispatch` blocks if the queue buffer is full (backpressure). Use `TryDispatch` when you would rather get `taskq.ErrQueueFull` back than wait:

```go
if _, err := q.TryDispatch(job); errors.Is(err, taskq.ErrQueueFull) {
	// shed load, log, or fall back to synchronous handling
}
```

## Job options

```go
taskq.WithMaxAttempts(5)
taskq.WithDelay(30 * time.Second)
taskq.WithID("job-123")
```

## Queue options

```go
taskq.WithDefaultMaxAttempts(3)
taskq.WithBackoff(func(attempt int) time.Duration { return time.Second })
taskq.WithLogger(logger)   // the queue is silent by default
taskq.WithQueueSize(64)    // buffer capacity, default workers*4
taskq.WithOnJobFailed(func(id string, err error) {
	// called when a job exhausts its attempts or is dropped on Stop
})
```

## Shutdown

There are two ways to stop a queue:

- `Close()` / `Shutdown(ctx)` — graceful drain. Stops accepting new jobs and
  waits for every accepted job to finish, **including delayed jobs and
  pending retries**. A job dispatched with `WithDelay(time.Hour)` keeps
  `Close` waiting until it runs.
- `Stop(ctx)` — hard stop. Cancels the `ctx` passed to running jobs and
  waits for them to return; delayed and queued jobs that have not started
  are dropped and reported to the `WithOnJobFailed` callback with
  `taskq.ErrQueueClosed`.

The context passed to `Shutdown`/`Stop` bounds only how long the call
waits; on expiry it returns `ctx.Err()` while teardown continues in the
background.

```go
ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
defer cancel()
_ = q.Stop(ctx)
```

## Caveats

- In-memory only: jobs are lost if the process exits.
- `Dispatch` blocks when the buffer is full. Avoid calling `Dispatch` from
  inside a running job on a saturated queue — if every worker blocks in
  `Dispatch`, the queue deadlocks. Use `TryDispatch` there instead.
- Long-running jobs should honor their `ctx` so `Stop` can interrupt them.

## Notes

- Requires Go 1.22+
- Best for small to medium Go services

## Testing

```bash
go test -race ./...
```
