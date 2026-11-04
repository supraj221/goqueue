package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type loadCfg struct {
	urls      []string
	producers int
	workers   int
	duration  time.Duration
	payload   int
	queue     string
	lease     time.Duration
	batch     int
	drain     time.Duration
	wait      time.Duration
	workTime  time.Duration
}

// loadState holds everything recorded during a run.
type loadState struct {
	cfg   loadCfg
	cl    *client
	runID string // keys of this run start with runID, so leftovers from earlier runs are ignored

	mu         sync.Mutex
	enqueued   map[string]struct{} // job IDs of enqueues that returned 2xx
	deliveries map[string]int      // job ID -> number of times a worker received it
	processed  map[string]struct{} // job IDs a worker acked successfully

	enqOK, enqErr atomic.Int64
	procCount     atomic.Int64
	ackLost       atomic.Int64 // acks answered 409 lease_lost
	dequeueErr    atomic.Int64
	producersDone atomic.Bool

	enqLat, qwait, e2e collector
	successMu          sync.Mutex
	successTimes       []int64 // unix nanos of every successful enqueue response

	start, loadEnd, drainEnd time.Time
	processedAtLoadEnd       int64
}

// collector gathers latency samples from many goroutines with low contention.
type collector struct {
	mu    sync.Mutex
	parts [][]time.Duration
}

func (c *collector) add(part []time.Duration) {
	if len(part) == 0 {
		return
	}
	c.mu.Lock()
	c.parts = append(c.parts, part)
	c.mu.Unlock()
}

func (c *collector) all() []time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []time.Duration
	for _, p := range c.parts {
		out = append(out, p...)
	}
	return out
}

// Latency summarizes a set of samples. Values are milliseconds.
type Latency struct {
	Count  int     `json:"count"`
	P50Ms  float64 `json:"p50_ms"`
	P95Ms  float64 `json:"p95_ms"`
	P99Ms  float64 `json:"p99_ms"`
	MaxMs  float64 `json:"max_ms"`
	MeanMs float64 `json:"mean_ms"`
}

func summarize(s []time.Duration) Latency {
	if len(s) == 0 {
		return Latency{}
	}
	sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })
	q := func(p float64) float64 {
		idx := int(float64(len(s))*p+0.9999999) - 1 // nearest-rank
		idx = max(0, min(idx, len(s)-1))
		return ms(s[idx])
	}
	var sum time.Duration
	for _, d := range s {
		sum += d
	}
	return Latency{Count: len(s), P50Ms: q(0.50), P95Ms: q(0.95), P99Ms: q(0.99), MaxMs: ms(s[len(s)-1]), MeanMs: ms(sum) / float64(len(s))}
}

func ms(d time.Duration) float64 { return float64(d) / float64(time.Millisecond) }

func randHex(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

type payloadDoc struct {
	T   int64  `json:"t"` // unix nanos when the producer started the enqueue
	K   string `json:"k"`
	Pad string `json:"pad,omitempty"`
}

func newLoadState(cfg loadCfg) *loadState {
	return &loadState{
		cfg: cfg, cl: newClient(cfg.urls), runID: randHex(4),
		enqueued:   make(map[string]struct{}),
		deliveries: make(map[string]int),
		processed:  make(map[string]struct{}),
	}
}

// run executes producers and workers for cfg.duration, then lets workers drain
// the backlog until every acknowledged enqueue is processed or cfg.drain passes.
// onLoadStart is invoked once, right before load begins.
func (s *loadState) run(ctx context.Context) {
	cfg := s.cfg
	runID := s.runID
	s.start = time.Now()
	loadCtx, cancelLoad := context.WithTimeout(ctx, cfg.duration)
	defer cancelLoad()
	workCtx, cancelWork := context.WithCancel(ctx)
	defer cancelWork()

	var pwg, wwg sync.WaitGroup
	for p := 0; p < cfg.producers; p++ {
		pwg.Add(1)
		go func() {
			defer pwg.Done()
			s.producer(loadCtx, ctx, fmt.Sprintf("%s-p%d", runID, p))
		}()
	}
	for w := 0; w < cfg.workers; w++ {
		wwg.Add(1)
		go func() {
			defer wwg.Done()
			s.worker(workCtx, fmt.Sprintf("bench-w%d", w))
		}()
	}

	pwg.Wait()
	s.loadEnd = time.Now()
	s.producersDone.Store(true)
	s.processedAtLoadEnd = s.procCount.Load()

	// Drain: wait until everything acknowledged has been processed.
	// A claim whose response was lost (leader change mid-request) leaves the job
	// leased to nobody until the lease expires, so draining must outlast the lease.
	deadline := time.Now().Add(max(cfg.drain, cfg.lease+10*time.Second))
	for time.Now().Before(deadline) && ctx.Err() == nil {
		if s.procCount.Load() >= s.enqOK.Load() {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	s.drainEnd = time.Now()
	cancelWork()
	wwg.Wait()
}

// producer enqueues in a closed loop until loadCtx ends. opCtx bounds retries
// of the final in-flight enqueue so it can still finish after the load ends.
func (s *loadState) producer(loadCtx, opCtx context.Context, name string) {
	var lat []time.Duration
	var times []int64
	pad := strings.Repeat("x", max(0, s.cfg.payload-64))
	for seq := 0; loadCtx.Err() == nil; seq++ {
		key := fmt.Sprintf("%s-%d", name, seq)
		t0 := time.Now()
		raw, _ := json.Marshal(payloadDoc{T: t0.UnixNano(), K: key, Pad: pad})
		// The same key is reused by every retry inside client.call (and by the
		// retry loop below), so a retry after failover cannot create a duplicate.
		octx, cancel := context.WithTimeout(opCtx, 60*time.Second)
		id, err := s.cl.enqueue(octx, s.cfg.queue, key, raw)
		cancel()
		now := time.Now()
		if err != nil {
			s.enqErr.Add(1)
			continue
		}
		lat = append(lat, now.Sub(t0))
		times = append(times, now.UnixNano())
		s.mu.Lock()
		s.enqueued[id] = struct{}{}
		s.mu.Unlock()
		s.enqOK.Add(1)
	}
	s.enqLat.add(lat)
	s.successMu.Lock()
	s.successTimes = append(s.successTimes, times...)
	s.successMu.Unlock()
}

func (s *loadState) worker(ctx context.Context, name string) {
	var qwait, e2e []time.Duration
	defer func() { s.qwait.add(qwait); s.e2e.add(e2e) }()
	for ctx.Err() == nil {
		if s.producersDone.Load() && s.procCount.Load() >= s.enqOK.Load() {
			return
		}
		jobs, err := s.cl.dequeue(ctx, s.cfg.queue, name, s.cfg.lease, s.cfg.batch, s.cfg.wait)
		if err != nil {
			if ctx.Err() == nil {
				s.dequeueErr.Add(1)
				time.Sleep(50 * time.Millisecond)
			}
			continue
		}
		claimed := time.Now()
		for _, j := range jobs {
			var pd payloadDoc
			_ = json.Unmarshal(j.Payload, &pd)
			if !strings.HasPrefix(pd.K, s.runID+"-") {
				// Left over from an earlier run against the same cluster: clean it
				// up without counting it.
				_, _ = s.cl.ack(ctx, j.ID, j.LeaseToken)
				continue
			}
			s.mu.Lock()
			s.deliveries[j.ID]++
			first := s.deliveries[j.ID] == 1
			s.mu.Unlock()
			if first && pd.T > 0 {
				qwait = append(qwait, claimed.Sub(time.Unix(0, pd.T)))
			}
			if s.cfg.workTime > 0 {
				time.Sleep(s.cfg.workTime)
			}
			acked, err := s.cl.ack(ctx, j.ID, j.LeaseToken)
			if err != nil {
				continue // leave it; the lease will expire and it will be redelivered
			}
			if !acked {
				// 409: either another worker owns the job, or an earlier
				// attempt of this very ack committed and only its response was
				// lost (leader change mid-request). GET tells them apart.
				s.ackLost.Add(1)
				if g, found, err := s.cl.getJob(ctx, j.ID); err != nil || !found || g.State != "succeeded" {
					continue
				}
			}
			s.mu.Lock()
			_, dup := s.processed[j.ID]
			s.processed[j.ID] = struct{}{}
			s.mu.Unlock()
			if !dup {
				s.procCount.Add(1)
				if pd.T > 0 {
					e2e = append(e2e, time.Since(time.Unix(0, pd.T)))
				}
			}
		}
	}
}

// verifyResult is the post-run accounting.
type verifyResult struct {
	Acknowledged     int            // enqueues that returned 2xx
	Processed        int            // acked by a worker
	RecoveredByGet   int            // ack response lost, but GET shows succeeded
	Lost             int            // acknowledged enqueue never reached succeeded
	LostStates       map[string]int // state (or "missing") of the lost jobs
	LostIDs          []string
	DuplicateJobs    int // jobs delivered to workers more than once
	ExtraDeliveries  int // sum of (deliveries-1) over those jobs
	DeliveredUnknown int // delivered jobs that were never acknowledged as enqueued
}

// verify checks every acknowledged enqueue against the server.
func (s *loadState) verify(ctx context.Context) verifyResult {
	s.mu.Lock()
	var pending []string
	res := verifyResult{Acknowledged: len(s.enqueued), LostStates: map[string]int{}}
	for id := range s.enqueued {
		if _, ok := s.processed[id]; ok {
			res.Processed++
		} else {
			pending = append(pending, id)
		}
	}
	for id, n := range s.deliveries {
		if n > 1 {
			res.DuplicateJobs++
			res.ExtraDeliveries += n - 1
		}
		if _, ok := s.enqueued[id]; !ok {
			res.DeliveredUnknown++
		}
	}
	s.mu.Unlock()

	var mu sync.Mutex
	sem := make(chan struct{}, 16)
	var wg sync.WaitGroup
	for _, id := range pending {
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			vctx, cancel := context.WithTimeout(ctx, 30*time.Second)
			defer cancel()
			j, found, err := s.cl.getJob(vctx, id)
			mu.Lock()
			defer mu.Unlock()
			switch {
			case err == nil && found && j.State == "succeeded":
				res.RecoveredByGet++
			default:
				res.Lost++
				st := "missing"
				if err != nil {
					st = "unknown(" + err.Error() + ")"
				} else if found {
					st = j.State
				}
				res.LostStates[st]++
				if len(res.LostIDs) < 20 {
					res.LostIDs = append(res.LostIDs, id)
				}
			}
		}()
	}
	wg.Wait()
	return res
}
