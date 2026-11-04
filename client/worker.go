package client

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"regexp"
	"runtime/debug"
	"sync"
	"time"

	"github.com/supraj221/goqueue/internal/broker"
	"github.com/supraj221/goqueue/internal/job"
)

// ErrShutdownTimeout is returned by Run when in-flight jobs did not finish
// within ShutdownTimeout and were nacked for retry.
var ErrShutdownTimeout = errors.New("goqueue: worker shutdown timed out")

type permanentError struct{ err error }

func (p *permanentError) Error() string { return p.err.Error() }
func (p *permanentError) Unwrap() error { return p.err }

// Permanent marks a handler error as non-retryable: the job is nacked with
// retry=false and moves to dead.
func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return &permanentError{err}
}

// Handler processes one job. The context is cancelled if the job's lease is
// lost or the worker's shutdown deadline passes. Handlers must be idempotent:
// delivery is at-least-once.
type Handler func(ctx context.Context, j job.Job) error

// WorkerOptions configures a Worker.
type WorkerOptions struct {
	Queues []string
	// ID identifies this worker as the lease owner. Auto-generated if empty.
	ID string
	// Concurrency bounds jobs running at once. Default 10.
	Concurrency int
	// Lease is the lease length requested per claim and per heartbeat.
	// Default 30s. Heartbeats go out every Lease/3.
	Lease time.Duration
	// BatchSize is the most jobs claimed per dequeue call. Default Concurrency.
	BatchSize int
	// PollWait is the long-poll duration. Default 5s.
	PollWait time.Duration
	// ShutdownTimeout is how long Run waits for in-flight jobs after ctx is
	// cancelled before nacking them for retry. Default 30s.
	ShutdownTimeout time.Duration
	Logger          *slog.Logger
}

// Worker pulls jobs from goqueue and runs handlers.
type Worker struct {
	c    *Client
	opts WorkerOptions
	log  *slog.Logger

	mu       sync.RWMutex
	handlers map[string]Handler

	inflightMu sync.Mutex
	inflight   map[string]*flight
}

type flight struct {
	job       job.Job
	abandoned bool
}

var badID = regexp.MustCompile(`[^A-Za-z0-9_.:-]`)

// NewWorker returns a Worker that uses c.
func NewWorker(c *Client, opts WorkerOptions) *Worker {
	if opts.ID == "" {
		host, _ := os.Hostname()
		var r [4]byte
		_, _ = rand.Read(r[:])
		opts.ID = badID.ReplaceAllString(fmt.Sprintf("%s-%d-%s", host, os.Getpid(), hex.EncodeToString(r[:])), "_")
	}
	if opts.Concurrency <= 0 {
		opts.Concurrency = 10
	}
	if opts.Lease <= 0 {
		opts.Lease = 30 * time.Second
	}
	if opts.BatchSize <= 0 || opts.BatchSize > opts.Concurrency {
		opts.BatchSize = opts.Concurrency
	}
	if opts.PollWait <= 0 {
		opts.PollWait = 5 * time.Second
	}
	if opts.ShutdownTimeout <= 0 {
		opts.ShutdownTimeout = 30 * time.Second
	}
	log := opts.Logger
	if log == nil {
		log = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	return &Worker{c: c, opts: opts, log: log, handlers: map[string]Handler{}, inflight: map[string]*flight{}}
}

// ID returns the worker's lease-owner ID.
func (w *Worker) ID() string { return w.opts.ID }

// Handle registers fn for jobs of the given type. Register before Run.
func (w *Worker) Handle(jobType string, fn func(ctx context.Context, j job.Job) error) {
	w.mu.Lock()
	w.handlers[jobType] = fn
	w.mu.Unlock()
}

func (w *Worker) handler(t string) (Handler, bool) {
	w.mu.RLock()
	defer w.mu.RUnlock()
	h, ok := w.handlers[t]
	return h, ok
}

// Run claims and processes jobs until ctx is cancelled. It then stops
// claiming and waits up to ShutdownTimeout for in-flight jobs; any still
// running after that are nacked with retry and Run returns ErrShutdownTimeout.
// It returns nil after a clean drain, or a non-nil error for a configuration
// problem such as invalid queue names.
func (w *Worker) Run(ctx context.Context) error {
	if len(w.opts.Queues) == 0 {
		return fmt.Errorf("%w: worker needs at least one queue", broker.ErrInvalid)
	}
	// Handlers run under workCtx, which outlives ctx so shutdown can drain.
	workCtx, cancelWork := context.WithCancel(context.WithoutCancel(ctx))
	defer cancelWork()

	sem := make(chan struct{}, w.opts.Concurrency)
	var wg sync.WaitGroup
	var fatal error

loop:
	for ctx.Err() == nil {
		// Wait for one free slot, then grab as many more as the batch allows.
		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
			break loop
		}
		n := 1
		for n < w.opts.BatchSize {
			select {
			case sem <- struct{}{}:
				n++
				continue
			default:
			}
			break
		}
		jobs, err := w.c.Dequeue(ctx, broker.DequeueRequest{
			Queues: w.opts.Queues, WorkerID: w.opts.ID, Lease: w.opts.Lease, Max: n, Wait: w.opts.PollWait,
		})
		for i := len(jobs); i < n; i++ { // release slots that got no job
			<-sem
		}
		if err != nil {
			if ctx.Err() != nil {
				break loop
			}
			if errors.Is(err, broker.ErrInvalid) {
				fatal = err
				break loop
			}
			w.log.Warn("dequeue failed", "err", err)
			_ = sleep(ctx, time.Second)
			continue
		}
		for _, j := range jobs {
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer func() { <-sem }()
				w.runJob(workCtx, j)
			}()
		}
	}

	// Drain.
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
		return fatal
	case <-time.After(w.opts.ShutdownTimeout):
	}
	cancelWork()
	w.inflightMu.Lock()
	var left []job.Job
	for _, f := range w.inflight {
		f.abandoned = true
		left = append(left, f.job)
	}
	w.inflightMu.Unlock()
	nctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()
	for _, j := range left {
		if err := w.c.Nack(nctx, j.ID, j.LeaseToken, "worker shutdown", true); err != nil {
			w.log.Warn("nack on shutdown failed", "job", j.ID, "err", err)
		}
	}
	if fatal != nil {
		return fatal
	}
	return fmt.Errorf("%w: %d jobs nacked for retry", ErrShutdownTimeout, len(left))
}

var errLeaseLost = errors.New("lease lost")

// runJob executes one claimed job: heartbeat in the background, run the
// handler, then ack or nack.
func (w *Worker) runJob(workCtx context.Context, j job.Job) {
	log := w.log.With("job", j.ID, "type", j.Type, "attempt", j.Attempts)
	h, ok := w.handler(j.Type)
	if !ok {
		w.settle(workCtx, j, func(ctx context.Context) error {
			return w.c.Nack(ctx, j.ID, j.LeaseToken, "no handler for job type "+j.Type, false)
		}, log)
		return
	}

	f := &flight{job: j}
	w.inflightMu.Lock()
	w.inflight[j.ID] = f
	w.inflightMu.Unlock()

	jctx, cancel := context.WithCancelCause(workCtx)
	hbDone := make(chan struct{})
	stopHB := make(chan struct{})
	go func() {
		defer close(hbDone)
		w.heartbeat(jctx, cancel, stopHB, j, log)
	}()

	err := w.invoke(jctx, h, j)

	close(stopHB)
	<-hbDone
	lost := errors.Is(context.Cause(jctx), errLeaseLost)
	cancel(nil)

	w.inflightMu.Lock()
	abandoned := f.abandoned
	delete(w.inflight, j.ID)
	w.inflightMu.Unlock()
	if abandoned {
		return // Run already nacked it at shutdown
	}
	if lost {
		log.Warn("lease lost; dropping result")
		return
	}

	var perm *permanentError
	var pan *panicError
	switch {
	case err == nil:
		w.settle(workCtx, j, func(ctx context.Context) error { return w.c.Ack(ctx, j.ID, j.LeaseToken) }, log)
	case errors.As(err, &pan):
		log.Error("handler panicked", "panic", pan.value, "stack", string(pan.stack))
		w.settle(workCtx, j, func(ctx context.Context) error {
			return w.c.Nack(ctx, j.ID, j.LeaseToken, err.Error(), true)
		}, log)
	case errors.As(err, &perm):
		w.settle(workCtx, j, func(ctx context.Context) error {
			return w.c.Nack(ctx, j.ID, j.LeaseToken, err.Error(), false)
		}, log)
	default:
		log.Info("handler failed; will retry", "err", err)
		w.settle(workCtx, j, func(ctx context.Context) error {
			return w.c.Nack(ctx, j.ID, j.LeaseToken, err.Error(), true)
		}, log)
	}
}

// settle sends the ack or nack, detached from cancellation so it still goes out
// during shutdown, with a bounded timeout.
func (w *Worker) settle(workCtx context.Context, j job.Job, fn func(context.Context) error, log *slog.Logger) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(workCtx), 15*time.Second)
	defer cancel()
	if err := fn(ctx); err != nil {
		if errors.Is(err, broker.ErrLeaseLost) {
			log.Warn("lease lost before ack/nack")
			return
		}
		log.Error("ack/nack failed", "err", err)
	}
}

type panicError struct {
	value any
	stack []byte
}

func (p *panicError) Error() string { return fmt.Sprintf("panic: %v", p.value) }

func (w *Worker) invoke(ctx context.Context, h Handler, j job.Job) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = &panicError{value: r, stack: debug.Stack()}
		}
	}()
	return h(ctx, j)
}

// heartbeat extends the lease every Lease/3 until stop is closed. If the
// broker says the lease is lost, it cancels the handler's context.
func (w *Worker) heartbeat(ctx context.Context, cancel context.CancelCauseFunc, stop <-chan struct{}, j job.Job, log *slog.Logger) {
	t := time.NewTicker(w.opts.Lease / 3)
	defer t.Stop()
	for {
		select {
		case <-stop:
			return
		case <-ctx.Done():
			return
		case <-t.C:
		}
		hctx, hcancel := context.WithTimeout(context.WithoutCancel(ctx), w.opts.Lease/3+time.Second)
		_, err := w.c.Heartbeat(hctx, j.ID, j.LeaseToken, w.opts.Lease)
		hcancel()
		switch {
		case err == nil:
		case errors.Is(err, broker.ErrLeaseLost), errors.Is(err, broker.ErrNotFound):
			log.Warn("heartbeat: lease lost; cancelling handler")
			cancel(errLeaseLost)
			return
		default:
			log.Warn("heartbeat failed", "err", err)
		}
	}
}
