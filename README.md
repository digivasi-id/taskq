[![Build Status](https://github.com/digivasi-id/taskq/actions/workflows/ci.yml/badge.svg)](https://github.com/digivasi-id/taskq/actions/workflows/ci.yml)
[![Coverage](https://github.com/digivasi-id/taskq/actions/workflows/coverage.yml/badge.svg)](https://github.com/digivasi-id/taskq/actions/workflows/coverage.yml)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

# taskq - Lightweight In-Memory Task Queue

taskq is a small in-memory task queue for Go.
It lets your app run background jobs without Redis, Kafka, or other extra services.

## Example usage

```go
package main

import (
	"fmt"

	taskq "github.com/digivasi-id/taskq"
)

func main() {
	q := taskq.New(5)
	defer q.Close()

	_, _ = q.Dispatch(func() error {
		fmt.Println("job executed")
		return nil
	})
}
```

## Features

- worker pool
- retries with max attempts
- exponential backoff
- delayed jobs
- automatic job IDs
- graceful shutdown

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
taskq.WithLogger(logger)
```

## Notes

- Requires Go 1.25+
- In-memory only
- Best for small to medium Go services

## Testing

```bash
go test ./...
```
