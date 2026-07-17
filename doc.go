// Package taskq provides a lightweight in-memory task queue backed by a
// fixed pool of worker goroutines, with no external dependencies.
//
// Jobs are plain functions dispatched onto the queue, with optional
// per-job delay, retry limit, and ID. Failed jobs are retried with a
// jittered exponential backoff. The queue supports both a graceful drain
// (Shutdown, Close) and a hard stop (Stop) that cancels pending work.
//
// A Queue is safe for concurrent use by multiple goroutines. Jobs live
// only in process memory: they are lost if the process exits.
package taskq
