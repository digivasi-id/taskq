package taskq_test

import (
	"context"
	"fmt"
	"time"

	taskq "github.com/digivasi-id/taskq"
)

func Example() {
	q := taskq.New(2)

	_, _ = q.Dispatch(func(ctx context.Context) error {
		fmt.Println("job executed")
		return nil
	})

	_ = q.Close()
	// Output: job executed
}

func ExampleQueue_Dispatch_withOptions() {
	q := taskq.New(1)

	_, _ = q.Dispatch(func(ctx context.Context) error {
		fmt.Println("delayed job executed")
		return nil
	},
		taskq.WithID("email-42"),
		taskq.WithDelay(10*time.Millisecond),
		taskq.WithMaxAttempts(5),
	)

	_ = q.Close()
	// Output: delayed job executed
}

func ExampleQueue_Stop() {
	q := taskq.New(1, taskq.WithOnJobFailed(func(id string, err error) {
		fmt.Printf("job %s failed: %v\n", id, err)
	}))

	_, _ = q.Dispatch(func(ctx context.Context) error {
		return nil
	}, taskq.WithID("nightly-report"), taskq.WithDelay(time.Hour))

	time.Sleep(50 * time.Millisecond)
	_ = q.Stop(context.Background())
	// Output: job nightly-report failed: taskq: queue closed
}
