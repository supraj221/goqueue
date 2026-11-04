package client_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/supraj221/goqueue/client"
	"github.com/supraj221/goqueue/internal/api"
	"github.com/supraj221/goqueue/internal/broker"
	"github.com/supraj221/goqueue/internal/broker/brokertest"
	"github.com/supraj221/goqueue/internal/job"
)

func startAPI(t *testing.T, b broker.Broker) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(api.NewServer(b, api.Options{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}).Handler())
	t.Cleanup(srv.Close)
	return srv
}

func fastRetry() client.Option { return client.WithRetry(6, 5*time.Millisecond, 20*time.Millisecond) }

func TestClientRoundTripAndTypedErrors(t *testing.T) {
	srv := startAPI(t, brokertest.NewMemBroker())
	c := client.New([]string{srv.URL}, fastRetry())
	ctx := context.Background()

	res, err := c.Enqueue(ctx, broker.EnqueueRequest{Queue: "q", Type: "t", Payload: []byte(`{"a":1}`), IdempotencyKey: "k"})
	if err != nil || !res.Created {
		t.Fatalf("enqueue: %+v %v", res, err)
	}
	dup, err := c.Enqueue(ctx, broker.EnqueueRequest{Queue: "q", Type: "t", IdempotencyKey: "k"})
	if err != nil || dup.Created || dup.Job.ID != res.Job.ID {
		t.Fatalf("dup enqueue: %+v %v", dup, err)
	}
	got, err := c.Get(ctx, res.Job.ID)
	if err != nil || got.ID != res.Job.ID {
		t.Fatalf("get: %+v %v", got, err)
	}
	jobs, err := c.Dequeue(ctx, broker.DequeueRequest{Queues: []string{"q"}, WorkerID: "w", Lease: 30 * time.Second, Max: 3})
	if err != nil || len(jobs) != 1 {
		t.Fatalf("dequeue: %v %v", jobs, err)
	}
	if _, err := c.Heartbeat(ctx, jobs[0].ID, jobs[0].LeaseToken, time.Minute); err != nil {
		t.Fatal(err)
	}
	if err := c.Ack(ctx, jobs[0].ID, 99); !errors.Is(err, broker.ErrLeaseLost) {
		t.Errorf("stale ack: %v", err)
	}
	if err := c.Ack(ctx, jobs[0].ID, jobs[0].LeaseToken); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Get(ctx, "missing"); !errors.Is(err, broker.ErrNotFound) {
		t.Errorf("get missing: %v", err)
	}
	if _, err := c.Enqueue(ctx, broker.EnqueueRequest{Queue: "", Type: "t"}); !errors.Is(err, broker.ErrInvalid) {
		t.Errorf("invalid enqueue: %v", err)
	}
	st, err := c.Stats(ctx)
	if err != nil || st.Counts["q"][job.StateSucceeded] != 1 {
		t.Errorf("stats: %+v %v", st, err)
	}
	if _, err := c.Cluster(ctx); !errors.Is(err, broker.ErrNotFound) {
		t.Errorf("cluster on non-cluster backend: %v", err)
	}
}

func TestFollowsLeaderHint(t *testing.T) {
	leader := startAPI(t, brokertest.NewMemBroker())
	var hits atomic.Int32
	follower := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.Header().Set("X-Goqueue-Leader", leader.URL)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusMisdirectedRequest)
		_, _ = w.Write([]byte(`{"error":"not_leader","message":"go elsewhere"}`))
	}))
	defer follower.Close()

	c := client.New([]string{follower.URL}, fastRetry())
	ctx := context.Background()
	for i := 0; i < 3; i++ {
		if _, err := c.Enqueue(ctx, broker.EnqueueRequest{Queue: "q", Type: "t"}); err != nil {
			t.Fatalf("enqueue %d: %v", i, err)
		}
	}
	if n := hits.Load(); n != 1 {
		t.Errorf("follower hit %d times, want 1 (client should stick to the leader)", n)
	}
	st, err := c.Stats(ctx)
	if err != nil || st.Counts["q"][job.StatePending] != 3 {
		t.Errorf("stats %+v %v", st, err)
	}
}

func TestNotLeaderWithoutHintRotates(t *testing.T) {
	real := startAPI(t, brokertest.NewMemBroker())
	bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusMisdirectedRequest)
		_, _ = w.Write([]byte(`{"error":"not_leader","message":"no leader yet"}`))
	}))
	defer bad.Close()
	c := client.New([]string{bad.URL, real.URL}, fastRetry())
	if _, err := c.Enqueue(context.Background(), broker.EnqueueRequest{Queue: "q", Type: "t"}); err != nil {
		t.Fatal(err)
	}
}

func TestRotatesOnConnectionErrorAnd503(t *testing.T) {
	real := startAPI(t, brokertest.NewMemBroker())
	dead := httptest.NewServer(http.NotFoundHandler())
	deadURL := dead.URL
	dead.Close() // connection refused
	var hits503 atomic.Int32
	unavailable := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits503.Add(1)
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(`{"error":"unavailable","message":"down"}`))
	}))
	defer unavailable.Close()

	c := client.New([]string{deadURL, unavailable.URL, real.URL}, fastRetry())
	if _, err := c.Enqueue(context.Background(), broker.EnqueueRequest{Queue: "q", Type: "t"}); err != nil {
		t.Fatal(err)
	}
	if hits503.Load() == 0 {
		t.Error("never tried the 503 server")
	}
}

func TestGivesUpWithUnavailable(t *testing.T) {
	dead := httptest.NewServer(http.NotFoundHandler())
	u := dead.URL
	dead.Close()
	c := client.New([]string{u}, client.WithRetry(3, time.Millisecond, 2*time.Millisecond))
	_, err := c.Stats(context.Background())
	if !errors.Is(err, broker.ErrUnavailable) {
		t.Fatalf("err = %v, want ErrUnavailable", err)
	}
}

func TestContextCancelStopsRetries(t *testing.T) {
	dead := httptest.NewServer(http.NotFoundHandler())
	u := dead.URL
	dead.Close()
	c := client.New([]string{u}, client.WithRetry(100, 50*time.Millisecond, time.Second))
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err := c.Stats(ctx)
	if !errors.Is(err, context.DeadlineExceeded) || time.Since(start) > 2*time.Second {
		t.Fatalf("err = %v after %v", err, time.Since(start))
	}
}

func TestNo4xxRetry(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"error":"invalid","message":"nope"}`))
	}))
	defer srv.Close()
	c := client.New([]string{srv.URL}, fastRetry())
	_, err := c.Enqueue(context.Background(), broker.EnqueueRequest{Queue: "q", Type: "t"})
	if !errors.Is(err, broker.ErrInvalid) {
		t.Fatalf("err = %v", err)
	}
	var ce *client.Error
	if !errors.As(err, &ce) || ce.Status != 400 {
		t.Errorf("not a *client.Error: %v", err)
	}
	if hits.Load() != 1 {
		t.Errorf("hits = %d, want 1", hits.Load())
	}
}

func TestNotLeaderErrorAs(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusMisdirectedRequest)
		_, _ = w.Write([]byte(`{"error":"not_leader","message":"x"}`))
	}))
	defer srv.Close()
	c := client.New([]string{srv.URL}, client.WithRetry(2, time.Millisecond, time.Millisecond))
	_, err := c.Stats(context.Background())
	var nl *broker.NotLeaderError
	if err == nil || !errors.As(err, &nl) {
		t.Fatalf("err = %v, want NotLeaderError in chain", err)
	}
}

// ---- Worker ----

func newWorker(c *client.Client, queues ...string) *client.Worker {
	return client.NewWorker(c, client.WorkerOptions{
		Queues: queues, Concurrency: 4, Lease: 300 * time.Millisecond, PollWait: 200 * time.Millisecond,
		ShutdownTimeout: 3 * time.Second,
	})
}

func runWorker(t *testing.T, w *client.Worker) (stop func() error) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	errc := make(chan error, 1)
	go func() { errc <- w.Run(ctx) }()
	var once sync.Once
	var result error
	stop = func() error {
		once.Do(func() {
			cancel()
			select {
			case result = <-errc:
			case <-time.After(10 * time.Second):
				result = errors.New("Run did not return")
			}
		})
		return result
	}
	t.Cleanup(func() { _ = stop() })
	return stop
}

func waitJob(t *testing.T, b broker.Broker, id string, want job.State) job.Job {
	t.Helper()
	deadline := time.Now().Add(8 * time.Second)
	for {
		j, err := b.Get(context.Background(), id)
		if err != nil {
			t.Fatal(err)
		}
		if j.State == want {
			return j
		}
		if time.Now().After(deadline) {
			t.Fatalf("job %s in state %s, want %s (last_error %q)", id, j.State, want, j.LastError)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestWorkerOutcomes(t *testing.T) {
	mem := brokertest.NewMemBroker()
	c := client.New([]string{startAPI(t, mem).URL}, fastRetry())
	w := newWorker(c, "q")
	w.Handle("ok", func(ctx context.Context, j job.Job) error { return nil })
	w.Handle("fail", func(ctx context.Context, j job.Job) error { return errors.New("transient") })
	w.Handle("perm", func(ctx context.Context, j job.Job) error { return client.Permanent(errors.New("bad input")) })
	w.Handle("panic", func(ctx context.Context, j job.Job) error { panic("oh no") })
	runWorker(t, w)

	enq := func(typ string) string {
		r, err := c.Enqueue(context.Background(), broker.EnqueueRequest{Queue: "q", Type: typ})
		if err != nil {
			t.Fatal(err)
		}
		return r.Job.ID
	}
	ok, fail, perm, pan, unknown := enq("ok"), enq("fail"), enq("perm"), enq("panic"), enq("nobody")

	waitJob(t, mem, ok, job.StateSucceeded)
	if j := waitJob(t, mem, fail, job.StateScheduled); j.LastError != "transient" {
		t.Errorf("fail last_error = %q", j.LastError)
	}
	if j := waitJob(t, mem, perm, job.StateDead); j.LastError != "bad input" {
		t.Errorf("perm last_error = %q", j.LastError)
	}
	if j := waitJob(t, mem, pan, job.StateScheduled); j.LastError != "panic: oh no" {
		t.Errorf("panic last_error = %q", j.LastError)
	}
	if j := waitJob(t, mem, unknown, job.StateDead); j.LastError == "" {
		t.Errorf("unknown type: empty reason")
	}
}

func TestWorkerConcurrencyLimit(t *testing.T) {
	mem := brokertest.NewMemBroker()
	c := client.New([]string{startAPI(t, mem).URL}, fastRetry())
	w := client.NewWorker(c, client.WorkerOptions{Queues: []string{"q"}, Concurrency: 3, Lease: time.Second, PollWait: 100 * time.Millisecond})
	var cur, peak atomic.Int32
	w.Handle("t", func(ctx context.Context, j job.Job) error {
		n := cur.Add(1)
		for {
			p := peak.Load()
			if n <= p || peak.CompareAndSwap(p, n) {
				break
			}
		}
		time.Sleep(30 * time.Millisecond)
		cur.Add(-1)
		return nil
	})
	var ids []string
	for i := 0; i < 20; i++ {
		r, err := c.Enqueue(context.Background(), broker.EnqueueRequest{Queue: "q", Type: "t"})
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, r.Job.ID)
	}
	runWorker(t, w)
	for _, id := range ids {
		waitJob(t, mem, id, job.StateSucceeded)
	}
	if p := peak.Load(); p > 3 || p < 2 {
		t.Errorf("peak concurrency = %d, want 2..3", p)
	}
}

func TestWorkerHeartbeatKeepsLeaseAndLostLeaseCancels(t *testing.T) {
	mem := brokertest.NewMemBroker()
	c := client.New([]string{startAPI(t, mem).URL}, fastRetry())
	w := newWorker(c, "q") // lease 300ms: the handler outlives it only via heartbeats

	started := make(chan job.Job, 2)
	cancelled := make(chan error, 1)
	w.Handle("slow", func(ctx context.Context, j job.Job) error {
		started <- j
		time.Sleep(900 * time.Millisecond) // three lease lengths
		return nil
	})
	w.Handle("hang", func(ctx context.Context, j job.Job) error {
		started <- j
		<-ctx.Done()
		cancelled <- context.Cause(ctx)
		return ctx.Err()
	})
	runWorker(t, w)

	slow, _ := c.Enqueue(context.Background(), broker.EnqueueRequest{Queue: "q", Type: "slow"})
	j := waitJob(t, mem, slow.Job.ID, job.StateSucceeded)
	if j.Attempts != 1 {
		t.Errorf("slow job attempts = %d; lease expired despite heartbeats", j.Attempts)
	}
	<-started

	hang, _ := c.Enqueue(context.Background(), broker.EnqueueRequest{Queue: "q", Type: "hang"})
	running := <-started
	if running.ID != hang.Job.ID {
		t.Fatalf("started %s", running.ID)
	}
	// Steal the job out from under the worker: dead state makes the next heartbeat fail.
	if err := mem.Nack(context.Background(), running.ID, running.LeaseToken, "stolen", false); err != nil {
		t.Fatal(err)
	}
	select {
	case cause := <-cancelled:
		if cause == nil {
			t.Error("handler ctx cancelled without cause")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("handler context was not cancelled after lease loss")
	}
}

func TestWorkerGracefulShutdownDrains(t *testing.T) {
	mem := brokertest.NewMemBroker()
	c := client.New([]string{startAPI(t, mem).URL}, fastRetry())
	w := newWorker(c, "q")
	started := make(chan struct{})
	w.Handle("t", func(ctx context.Context, j job.Job) error {
		close(started)
		time.Sleep(400 * time.Millisecond)
		return nil
	})
	stop := runWorker(t, w)
	r, _ := c.Enqueue(context.Background(), broker.EnqueueRequest{Queue: "q", Type: "t"})
	<-started
	if err := stop(); err != nil {
		t.Fatalf("Run = %v, want nil after clean drain", err)
	}
	if j, _ := mem.Get(context.Background(), r.Job.ID); j.State != job.StateSucceeded {
		t.Errorf("state = %s, want succeeded", j.State)
	}
}

func TestWorkerShutdownTimeoutNacksRemaining(t *testing.T) {
	mem := brokertest.NewMemBroker()
	c := client.New([]string{startAPI(t, mem).URL}, fastRetry())
	w := client.NewWorker(c, client.WorkerOptions{
		Queues: []string{"q"}, Concurrency: 2, Lease: 5 * time.Second, PollWait: 100 * time.Millisecond,
		ShutdownTimeout: 300 * time.Millisecond,
	})
	started := make(chan struct{})
	release := make(chan struct{})
	w.Handle("t", func(ctx context.Context, j job.Job) error {
		close(started)
		<-release // ignores ctx on purpose
		return nil
	})
	defer close(release)
	stop := runWorker(t, w)
	r, _ := c.Enqueue(context.Background(), broker.EnqueueRequest{Queue: "q", Type: "t"})
	<-started
	err := stop()
	if !errors.Is(err, client.ErrShutdownTimeout) {
		t.Fatalf("Run = %v, want ErrShutdownTimeout", err)
	}
	j, _ := mem.Get(context.Background(), r.Job.ID)
	if j.State != job.StateScheduled || j.LastError != "worker shutdown" {
		t.Errorf("job after forced shutdown: state %s, last_error %q", j.State, j.LastError)
	}
}

func TestWorkerRequiresQueues(t *testing.T) {
	w := client.NewWorker(client.New([]string{"http://127.0.0.1:1"}), client.WorkerOptions{})
	if err := w.Run(context.Background()); !errors.Is(err, broker.ErrInvalid) {
		t.Fatalf("err = %v", err)
	}
}
