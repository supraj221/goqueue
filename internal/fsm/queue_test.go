package fsm

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/supraj221/goqueue/internal/broker"
	"github.com/supraj221/goqueue/internal/job"
	"github.com/supraj221/goqueue/internal/raft"
)

var t0 = time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)

func apply(q *Queue, data []byte) Result {
	return q.Apply(raft.LogEntry{Data: data}).(Result)
}

func mustOK(t *testing.T, r Result) Result {
	t.Helper()
	if r.Err != nil {
		t.Fatalf("unexpected error: %v", r.Err)
	}
	return r
}

type seq struct{ n int }

func (s *seq) id() string { s.n++; return fmt.Sprintf("J%08d", s.n) }

func enq(q *Queue, s *seq, queue string, prio int, now time.Time, runAt time.Time, key string, maxAttempts int) Result {
	return apply(q, EncodeEnqueue(Enqueue{Job: job.Job{
		ID: s.id(), Queue: queue, Type: "t", Priority: prio, RunAt: runAt,
		CreatedAt: now, MaxAttempts: maxAttempts, IdempotencyKey: key,
	}}))
}

func claim(q *Queue, queues []string, max int, now time.Time, lease time.Duration) []job.Job {
	r := apply(q, EncodeClaim(Claim{Queues: queues, WorkerID: "w1", Max: max, Now: now, Lease: lease}))
	if r.Err != nil {
		panic(r.Err)
	}
	return r.Jobs
}

func state(t *testing.T, q *Queue, id string) job.Job {
	t.Helper()
	j, ok := q.Get(id)
	if !ok {
		t.Fatalf("job %s missing", id)
	}
	return j
}

func TestEnqueueStates(t *testing.T) {
	q := New()
	s := &seq{}
	r := mustOK(t, enq(q, s, "a", 0, t0, time.Time{}, "", 0))
	if !r.Created || r.Job.State != job.StatePending || r.Job.MaxAttempts != job.DefaultMaxAttempts {
		t.Fatalf("pending enqueue: %+v", r.Job)
	}
	r = mustOK(t, enq(q, s, "a", 0, t0, t0.Add(time.Minute), "", 3))
	if r.Job.State != job.StateScheduled || r.Job.MaxAttempts != 3 {
		t.Fatalf("scheduled enqueue: %+v", r.Job)
	}
	st := q.Stats().Counts["a"]
	if st[job.StatePending] != 1 || st[job.StateScheduled] != 1 {
		t.Fatalf("stats: %v", st)
	}
}

func TestTransitions(t *testing.T) {
	type step func(t *testing.T, q *Queue, j job.Job) job.Job
	claimStep := func(now time.Time) step {
		return func(t *testing.T, q *Queue, j job.Job) job.Job {
			got := claim(q, []string{"q"}, 1, now, time.Second)
			if len(got) != 1 || got[0].ID != j.ID {
				t.Fatalf("claim got %v", got)
			}
			return got[0]
		}
	}
	nack := func(now time.Time, retry bool, retryAt time.Time) step {
		return func(t *testing.T, q *Queue, j job.Job) job.Job {
			mustOK(t, apply(q, EncodeNack(Nack{ID: j.ID, Token: j.LeaseToken, Now: now, Reason: "boom", Retry: retry, RetryAt: retryAt})))
			return j
		}
	}
	tests := []struct {
		name    string
		runAt   time.Time
		max     int
		steps   []step
		want    job.State
		attempt int
	}{
		{"pending->running", time.Time{}, 3, []step{claimStep(t0)}, job.StateRunning, 1},
		{"running->ack->succeeded", time.Time{}, 3, []step{claimStep(t0), func(t *testing.T, q *Queue, j job.Job) job.Job {
			mustOK(t, apply(q, EncodeAck(Ack{ID: j.ID, Token: j.LeaseToken, Now: t0})))
			return j
		}}, job.StateSucceeded, 1},
		{"nack retry->scheduled", time.Time{}, 3, []step{claimStep(t0), nack(t0, true, t0.Add(time.Minute))}, job.StateScheduled, 1},
		{"nack no retry->dead", time.Time{}, 3, []step{claimStep(t0), nack(t0, false, time.Time{})}, job.StateDead, 1},
		{"nack retry exhausted->dead", time.Time{}, 1, []step{claimStep(t0), nack(t0, true, t0.Add(time.Minute))}, job.StateDead, 1},
		{"scheduled->promote->pending", t0.Add(time.Minute), 3, []step{func(t *testing.T, q *Queue, j job.Job) job.Job {
			r := mustOK(t, apply(q, EncodePromoteScheduled(PromoteScheduled{Now: t0.Add(30 * time.Second)})))
			if r.N != 0 {
				t.Fatalf("promoted early")
			}
			r = mustOK(t, apply(q, EncodePromoteScheduled(PromoteScheduled{Now: t0.Add(time.Minute)})))
			if r.N != 1 {
				t.Fatalf("not promoted")
			}
			return j
		}}, job.StatePending, 0},
		{"lease expiry->pending", time.Time{}, 3, []step{claimStep(t0), func(t *testing.T, q *Queue, j job.Job) job.Job {
			mustOK(t, apply(q, EncodeExpireLeases(ExpireLeases{Now: t0.Add(2 * time.Second)})))
			return j
		}}, job.StatePending, 1},
		{"lease expiry exhausted->dead", time.Time{}, 1, []step{claimStep(t0), func(t *testing.T, q *Queue, j job.Job) job.Job {
			mustOK(t, apply(q, EncodeExpireLeases(ExpireLeases{Now: t0.Add(2 * time.Second)})))
			return j
		}}, job.StateDead, 1},
		{"not expired at boundary", time.Time{}, 3, []step{claimStep(t0), func(t *testing.T, q *Queue, j job.Job) job.Job {
			mustOK(t, apply(q, EncodeExpireLeases(ExpireLeases{Now: t0.Add(time.Second)})))
			return j
		}}, job.StateRunning, 1},
		{"claim reclaims expired lease", time.Time{}, 3, []step{claimStep(t0), func(t *testing.T, q *Queue, j job.Job) job.Job {
			got := claim(q, []string{"q"}, 1, t0.Add(5*time.Second), time.Second)
			if len(got) != 1 || got[0].Attempts != 2 || got[0].LeaseToken != 2 {
				t.Fatalf("reclaim: %+v", got)
			}
			return got[0]
		}}, job.StateRunning, 2},
		{"claim promotes due scheduled", t0.Add(time.Second), 3, []step{func(t *testing.T, q *Queue, j job.Job) job.Job {
			if got := claim(q, []string{"q"}, 1, t0, time.Second); len(got) != 0 {
				t.Fatalf("claimed early")
			}
			return claimStep(t0.Add(time.Second))(t, q, j)
		}}, job.StateRunning, 1},
		{"heartbeat extends", time.Time{}, 3, []step{claimStep(t0), func(t *testing.T, q *Queue, j job.Job) job.Job {
			r := mustOK(t, apply(q, EncodeHeartbeat(Heartbeat{ID: j.ID, Token: j.LeaseToken, Now: t0.Add(900 * time.Millisecond), Extend: time.Minute})))
			if !r.Job.LeaseExpiresAt.Equal(t0.Add(900*time.Millisecond + time.Minute)) {
				t.Fatalf("lease %v", r.Job.LeaseExpiresAt)
			}
			mustOK(t, apply(q, EncodeExpireLeases(ExpireLeases{Now: t0.Add(10 * time.Second)})))
			return j
		}}, job.StateRunning, 1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			q := New()
			s := &seq{}
			r := mustOK(t, enq(q, s, "q", 0, t0, tc.runAt, "", tc.max))
			j := *r.Job
			for _, st := range tc.steps {
				j = st(t, q, j)
			}
			got := state(t, q, j.ID)
			if got.State != tc.want || got.Attempts != tc.attempt {
				t.Fatalf("state=%s attempts=%d, want %s/%d", got.State, got.Attempts, tc.want, tc.attempt)
			}
			if (tc.want == job.StateSucceeded || tc.want == job.StateDead) && got.CompletedAt == nil {
				t.Fatalf("CompletedAt not set")
			}
			total := 0
			for _, n := range q.Stats().Counts["q"] {
				total += n
			}
			if total != 1 || q.Stats().Counts["q"][tc.want] != 1 {
				t.Fatalf("stats %v", q.Stats().Counts)
			}
		})
	}
}

func TestStaleTokenAndErrors(t *testing.T) {
	q := New()
	s := &seq{}
	r := mustOK(t, enq(q, s, "q", 0, t0, time.Time{}, "", 5))
	id := r.Job.ID
	first := claim(q, []string{"q"}, 1, t0, time.Second)[0]
	second := claim(q, []string{"q"}, 1, t0.Add(2*time.Second), time.Minute)[0]
	if second.LeaseToken != 2 || first.LeaseToken != 1 {
		t.Fatalf("tokens %d %d", first.LeaseToken, second.LeaseToken)
	}
	now := t0.Add(3 * time.Second)
	for name, res := range map[string]Result{
		"ack":       apply(q, EncodeAck(Ack{ID: id, Token: 1, Now: now})),
		"nack":      apply(q, EncodeNack(Nack{ID: id, Token: 1, Now: now, Retry: true})),
		"heartbeat": apply(q, EncodeHeartbeat(Heartbeat{ID: id, Token: 1, Now: now, Extend: time.Second})),
	} {
		if !errors.Is(res.Err, broker.ErrLeaseLost) {
			t.Fatalf("%s with stale token: %v", name, res.Err)
		}
	}
	if st := state(t, q, id); st.State != job.StateRunning || st.LeaseToken != 2 {
		t.Fatalf("stale op changed job: %+v", st)
	}
	if res := apply(q, EncodeAck(Ack{ID: "nope", Token: 1, Now: now})); !errors.Is(res.Err, broker.ErrNotFound) {
		t.Fatalf("missing: %v", res.Err)
	}
	mustOK(t, apply(q, EncodeAck(Ack{ID: id, Token: 2, Now: now})))
	if res := apply(q, EncodeAck(Ack{ID: id, Token: 2, Now: now})); !errors.Is(res.Err, broker.ErrLeaseLost) {
		t.Fatalf("double ack: %v", res.Err)
	}
}

func TestMalformedCommands(t *testing.T) {
	q := New()
	for _, data := range [][]byte{
		nil, []byte("garbage"), []byte(`{"v":2,"op":"enqueue"}`), []byte(`{"v":1,"op":"nope"}`),
		[]byte(`{"v":1,"op":"enqueue","job":"x"}`), []byte(`{"v":1,"op":"enqueue"}`),
		[]byte(`{"v":1,"op":"claim"}`), []byte(`[]`),
	} {
		r := apply(q, data)
		if !errors.Is(r.Err, broker.ErrInvalid) {
			t.Errorf("%q: err=%v", data, r.Err)
		}
	}
}

func TestIdempotency(t *testing.T) {
	q := New()
	s := &seq{}
	a := mustOK(t, enq(q, s, "q", 0, t0, time.Time{}, "k1", 0))
	b := mustOK(t, enq(q, s, "q", 5, t0.Add(time.Second), time.Time{}, "k1", 0))
	if !a.Created || b.Created || a.Job.ID != b.Job.ID {
		t.Fatalf("idempotent enqueue: %+v %+v", a, b)
	}
	if q.Len() != 1 {
		t.Fatalf("len %d", q.Len())
	}
	c := mustOK(t, enq(q, s, "q", 0, t0, time.Time{}, "k2", 0))
	if !c.Created || c.Job.ID == a.Job.ID {
		t.Fatalf("distinct key should create")
	}
}

func TestOrderingAcrossQueues(t *testing.T) {
	q := New()
	s := &seq{}
	mustOK(t, enq(q, s, "a", 0, t0, time.Time{}, "", 0))                  // J1
	mustOK(t, enq(q, s, "b", 0, t0, time.Time{}, "", 0))                  // J2
	mustOK(t, enq(q, s, "b", 9, t0.Add(time.Second), time.Time{}, "", 0)) // J3 high priority
	mustOK(t, enq(q, s, "c", 0, t0, time.Time{}, "", 0))                  // J4 not requested
	got := claim(q, []string{"a", "b"}, 10, t0.Add(time.Second), time.Minute)
	var ids []string
	for _, j := range got {
		ids = append(ids, j.ID)
	}
	want := []string{"J00000003", "J00000001", "J00000002"}
	if fmt.Sprint(ids) != fmt.Sprint(want) {
		t.Fatalf("order %v want %v", ids, want)
	}
}

func TestPurge(t *testing.T) {
	q := New()
	s := &seq{}
	a := mustOK(t, enq(q, s, "q", 0, t0, time.Time{}, "ka", 0)).Job
	b := mustOK(t, enq(q, s, "q", 0, t0, time.Time{}, "kb", 1)).Job
	c := mustOK(t, enq(q, s, "q", 0, t0, time.Time{}, "kc", 0)).Job
	claim(q, []string{"q"}, 3, t0, time.Minute)
	mustOK(t, apply(q, EncodeAck(Ack{ID: a.ID, Token: 1, Now: t0.Add(time.Second)})))
	mustOK(t, apply(q, EncodeNack(Nack{ID: b.ID, Token: 1, Now: t0.Add(time.Hour), Retry: false})))
	r := mustOK(t, apply(q, EncodePurge(Purge{Before: t0.Add(time.Minute)})))
	if r.N != 1 {
		t.Fatalf("purged %d", r.N)
	}
	if _, ok := q.Get(a.ID); ok {
		t.Fatal("a should be gone")
	}
	if _, ok := q.Get(b.ID); !ok {
		t.Fatal("b is too recent")
	}
	if _, ok := q.Get(c.ID); !ok {
		t.Fatal("running job must stay")
	}
	// the purged job's idempotency key is released
	again := mustOK(t, enq(q, s, "q", 0, t0, time.Time{}, "ka", 0))
	if !again.Created {
		t.Fatal("key should be reusable after purge")
	}
	r = mustOK(t, apply(q, EncodePurge(Purge{Before: t0.Add(2 * time.Hour)})))
	if r.N != 1 {
		t.Fatalf("second purge %d", r.N)
	}
	if got := q.Stats().Counts["q"]; got[job.StateSucceeded] != 0 || got[job.StateDead] != 0 {
		t.Fatalf("stats after purge: %v", got)
	}
}

func TestSubscribe(t *testing.T) {
	q := New()
	ch, cancel := q.Subscribe()
	defer cancel()
	s := &seq{}
	enq(q, s, "q", 0, t0, time.Time{}, "", 0)
	select {
	case <-ch:
	default:
		t.Fatal("no wake after enqueue")
	}
	select {
	case <-ch:
		t.Fatal("spurious wake")
	default:
	}
	enq(q, s, "q", 0, t0, t0.Add(time.Hour), "", 0) // scheduled: no wake
	select {
	case <-ch:
		t.Fatal("wake for scheduled job")
	default:
	}
}

func TestHasClaimable(t *testing.T) {
	q := New()
	s := &seq{}
	if q.HasClaimable([]string{"q"}, t0) {
		t.Fatal("empty")
	}
	enq(q, s, "q", 0, t0, t0.Add(time.Minute), "", 0)
	if q.HasClaimable([]string{"q"}, t0) {
		t.Fatal("scheduled not due")
	}
	if !q.HasClaimable([]string{"q"}, t0.Add(time.Minute)) {
		t.Fatal("scheduled due")
	}
}

// randomCommands builds a pseudo-random but valid-ish command stream.
func randomCommands(seed uint64, n int) [][]byte {
	rng := rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15))
	queues := []string{"a", "b", "c"}
	var cmds [][]byte
	now := t0
	var ids []string
	nextID := 0
	for i := 0; i < n; i++ {
		now = now.Add(time.Duration(rng.IntN(400)) * time.Millisecond)
		switch r := rng.IntN(100); {
		case r < 35:
			nextID++
			id := fmt.Sprintf("J%08d", nextID)
			ids = append(ids, id)
			j := job.Job{ID: id, Queue: queues[rng.IntN(3)], Type: "t", Priority: rng.IntN(3), CreatedAt: now,
				RunAt: now.Add(time.Duration(rng.IntN(3)) * time.Second), MaxAttempts: 1 + rng.IntN(3)}
			if rng.IntN(4) == 0 {
				j.IdempotencyKey = fmt.Sprintf("k%d", rng.IntN(50))
			}
			cmds = append(cmds, EncodeEnqueue(Enqueue{Job: j}))
		case r < 60:
			cmds = append(cmds, EncodeClaim(Claim{Queues: queues[:1+rng.IntN(3)], WorkerID: "w", Max: 1 + rng.IntN(4), Now: now, Lease: time.Duration(1+rng.IntN(3)) * time.Second}))
		case r < 70 && len(ids) > 0:
			cmds = append(cmds, EncodeAck(Ack{ID: ids[rng.IntN(len(ids))], Token: uint64(1 + rng.IntN(3)), Now: now}))
		case r < 78 && len(ids) > 0:
			cmds = append(cmds, EncodeNack(Nack{ID: ids[rng.IntN(len(ids))], Token: uint64(1 + rng.IntN(3)), Now: now, Reason: "r", Retry: rng.IntN(2) == 0, RetryAt: now.Add(time.Second)}))
		case r < 85 && len(ids) > 0:
			cmds = append(cmds, EncodeHeartbeat(Heartbeat{ID: ids[rng.IntN(len(ids))], Token: uint64(1 + rng.IntN(3)), Now: now, Extend: time.Second}))
		case r < 90:
			cmds = append(cmds, EncodeExpireLeases(ExpireLeases{Now: now}))
		case r < 96:
			cmds = append(cmds, EncodePromoteScheduled(PromoteScheduled{Now: now}))
		default:
			cmds = append(cmds, EncodePurge(Purge{Before: now.Add(-3 * time.Second)}))
		}
	}
	return cmds
}

func TestDeterminismAndSnapshotRoundTrip(t *testing.T) {
	for seed := uint64(1); seed <= 5; seed++ {
		cmds := randomCommands(seed, 4000)
		a, b := New(), New()
		for i, c := range cmds {
			ra := apply(a, c)
			rb := apply(b, c)
			if rkey(ra) != rkey(rb) {
				t.Fatalf("seed %d cmd %d: results differ: %v vs %v", seed, i, ra, rb)
			}
		}
		sa, err := a.Snapshot()
		if err != nil {
			t.Fatal(err)
		}
		sb, _ := b.Snapshot()
		if !bytes.Equal(sa, sb) {
			t.Fatalf("seed %d: snapshots differ", seed)
		}
		c := New()
		if err := c.Restore(sa); err != nil {
			t.Fatal(err)
		}
		sc, _ := c.Snapshot()
		if !bytes.Equal(sa, sc) {
			t.Fatalf("seed %d: restored snapshot differs", seed)
		}
		// Restored state must also behave identically going forward.
		more := randomCommands(seed+100, 1500)
		for _, cmd := range more {
			// shift into the future so both see the same inputs
			ra, rc := apply(a, cmd), apply(c, cmd)
			if rkey(ra) != rkey(rc) {
				t.Fatalf("seed %d: restored instance diverged: %v vs %v", seed, ra, rc)
			}
		}
		sa, _ = a.Snapshot()
		sc, _ = c.Snapshot()
		if !bytes.Equal(sa, sc) {
			t.Fatalf("seed %d: diverged after continuing", seed)
		}
		if a.Stats().Counts == nil {
			t.Fatal("nil counts")
		}
	}
}

// TestStatsMatchState checks the incremental counters against a recount.
func TestStatsMatchState(t *testing.T) {
	q := New()
	for _, c := range randomCommands(42, 6000) {
		apply(q, c)
	}
	want := map[string]map[job.State]int{}
	q.mu.RLock()
	for _, j := range q.jobs {
		if want[j.Queue] == nil {
			want[j.Queue] = map[job.State]int{}
		}
		want[j.Queue][j.State]++
	}
	q.mu.RUnlock()
	if fmt.Sprint(want) != fmt.Sprint(q.Stats().Counts) {
		t.Fatalf("counters drifted:\n%v\n%v", want, q.Stats().Counts)
	}
}

func preload(q *Queue, n int) {
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("P%09d", i)
		q.Apply(raft.LogEntry{Data: EncodeEnqueue(Enqueue{Job: job.Job{ID: id, Queue: "bench", Type: "t", CreatedAt: t0}})})
	}
}

func BenchmarkApplyEnqueue(b *testing.B) {
	q := New()
	payload := make([]byte, 0, 64)
	payload = append(payload, `{"k":"v"}`...)
	cmds := make([]raft.LogEntry, b.N)
	for i := range cmds {
		cmds[i] = raft.LogEntry{Data: EncodeEnqueue(Enqueue{Job: job.Job{
			ID: fmt.Sprintf("B%09d", i), Queue: "bench", Type: "t", Payload: payload, CreatedAt: t0}})}
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		q.Apply(cmds[i])
	}
}

func BenchmarkApplyClaim(b *testing.B) {
	if testing.Short() {
		b.Skip("1M job preload")
	}
	q := New()
	preload(q, 1_000_000)
	cmds := make([]raft.LogEntry, b.N)
	for i := range cmds {
		cmds[i] = raft.LogEntry{Data: EncodeClaim(Claim{Queues: []string{"bench"}, WorkerID: "w", Max: 1,
			Now: t0.Add(time.Duration(i) * time.Microsecond), Lease: time.Hour})}
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		q.Apply(cmds[i])
	}
}

func rkey(r Result) string {
	b, _ := json.Marshal(struct {
		Job     *job.Job
		Jobs    []job.Job
		Created bool
		N       int
		Err     string
	}{r.Job, r.Jobs, r.Created, r.N, fmt.Sprint(r.Err)})
	return string(b)
}
