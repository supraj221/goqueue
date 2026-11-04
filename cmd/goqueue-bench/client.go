package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"
)

// client talks to a goqueue cluster directly over HTTP. It follows leader
// hints (421 + X-Goqueue-Leader) and retries connection errors and 503s with
// bounded exponential backoff. It deliberately does not depend on the
// project's client package, so the benchmark measures the server alone.
type client struct {
	hc *http.Client

	enqAttempt time.Duration // per-attempt timeout for enqueue (default 10s)

	mu   sync.Mutex
	urls []string
	cur  int
}

func newClient(urls []string) *client {
	tr := &http.Transport{
		MaxIdleConns:        512,
		MaxIdleConnsPerHost: 128,
		IdleConnTimeout:     60 * time.Second,
		DialContext:         (&net.Dialer{Timeout: 2 * time.Second}).DialContext,
	}
	return &client{hc: &http.Client{Transport: tr}, urls: append([]string(nil), urls...), enqAttempt: 10 * time.Second}
}

func (c *client) base() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.urls[c.cur]
}

// rotate moves to the next URL if bad is still the current one.
func (c *client) rotate(bad string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.urls[c.cur] == bad {
		c.cur = (c.cur + 1) % len(c.urls)
	}
}

// adopt makes addr the preferred URL.
func (c *client) adopt(addr string) {
	addr = strings.TrimRight(addr, "/")
	c.mu.Lock()
	defer c.mu.Unlock()
	for i, u := range c.urls {
		if u == addr {
			c.cur = i
			return
		}
	}
	c.urls = append(c.urls, addr)
	c.cur = len(c.urls) - 1
}

// apiResp is a final (non-retried) HTTP response.
type apiResp struct {
	Status int
	Body   []byte
}

// call performs a request, retrying until it gets a definitive answer (2xx or
// a non-retryable 4xx) or ctx is done. attemptTimeout bounds each HTTP try.
func (c *client) call(ctx context.Context, method, path string, body any, attemptTimeout time.Duration) (apiResp, error) {
	var payload []byte
	if body != nil {
		var err error
		if payload, err = json.Marshal(body); err != nil {
			return apiResp{}, err
		}
	}
	backoff := 10 * time.Millisecond
	var lastErr error
	for attempt := 0; ; attempt++ {
		if ctx.Err() != nil {
			if lastErr != nil {
				return apiResp{}, fmt.Errorf("%w (last: %v)", ctx.Err(), lastErr)
			}
			return apiResp{}, ctx.Err()
		}
		base := c.base()
		actx, cancel := context.WithTimeout(ctx, attemptTimeout)
		req, err := http.NewRequestWithContext(actx, method, base+path, bytes.NewReader(payload))
		if err != nil {
			cancel()
			return apiResp{}, err
		}
		if payload != nil {
			req.Header.Set("Content-Type", "application/json")
		}
		resp, err := c.hc.Do(req)
		if err != nil {
			cancel()
			lastErr = err
			c.rotate(base)
			sleepCtx(ctx, backoff)
			backoff = min(backoff*2, 250*time.Millisecond)
			continue
		}
		b, rerr := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		cancel()
		if rerr != nil {
			lastErr = rerr
			c.rotate(base)
			sleepCtx(ctx, backoff)
			continue
		}
		switch {
		case resp.StatusCode == http.StatusMisdirectedRequest:
			lastErr = errors.New("not leader")
			if leader := resp.Header.Get("X-Goqueue-Leader"); leader != "" {
				c.adopt(leader)
				sleepCtx(ctx, 2*time.Millisecond)
			} else {
				c.rotate(base)
				sleepCtx(ctx, backoff)
				backoff = min(backoff*2, 250*time.Millisecond)
			}
			continue
		case resp.StatusCode == http.StatusServiceUnavailable,
			resp.StatusCode == http.StatusBadGateway,
			resp.StatusCode == http.StatusGatewayTimeout,
			resp.StatusCode >= 500:
			lastErr = fmt.Errorf("status %d: %s", resp.StatusCode, strings.TrimSpace(string(b)))
			c.rotate(base)
			sleepCtx(ctx, backoff)
			backoff = min(backoff*2, 250*time.Millisecond)
			continue
		}
		return apiResp{Status: resp.StatusCode, Body: b}, nil
	}
}

func sleepCtx(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

// ---- typed operations ----

type jobJSON struct {
	ID         string          `json:"id"`
	State      string          `json:"state"`
	Payload    json.RawMessage `json:"payload"`
	LeaseToken uint64          `json:"lease_token"`
	Attempts   int             `json:"attempts"`
}

func (c *client) enqueue(ctx context.Context, queue, key string, payload json.RawMessage) (id string, err error) {
	r, err := c.call(ctx, http.MethodPost, "/v1/jobs", map[string]any{
		"queue": queue, "type": "bench", "payload": payload, "idempotency_key": key,
	}, c.enqAttempt)
	if err != nil {
		return "", err
	}
	if r.Status != http.StatusCreated && r.Status != http.StatusOK {
		return "", fmt.Errorf("enqueue: status %d %s", r.Status, r.Body)
	}
	var j jobJSON
	if err := json.Unmarshal(r.Body, &j); err != nil || j.ID == "" {
		// Tolerate an {"job": {...}} envelope.
		var env struct {
			Job jobJSON `json:"job"`
		}
		if err2 := json.Unmarshal(r.Body, &env); err2 != nil || env.Job.ID == "" {
			return "", fmt.Errorf("enqueue: cannot parse response %s", r.Body)
		}
		j = env.Job
	}
	return j.ID, nil
}

func (c *client) dequeue(ctx context.Context, queue, worker string, lease time.Duration, max int, wait time.Duration) ([]jobJSON, error) {
	r, err := c.call(ctx, http.MethodPost, "/v1/dequeue", map[string]any{
		"queues": []string{queue}, "worker_id": worker, "lease_ms": lease.Milliseconds(),
		"max": max, "wait_ms": wait.Milliseconds(),
	}, wait+10*time.Second)
	if err != nil {
		return nil, err
	}
	if r.Status != http.StatusOK {
		return nil, fmt.Errorf("dequeue: status %d %s", r.Status, r.Body)
	}
	var out struct {
		Jobs []jobJSON `json:"jobs"`
	}
	if err := json.Unmarshal(r.Body, &out); err != nil {
		return nil, err
	}
	return out.Jobs, nil
}

// ack returns (acked, err). A 409 lease_lost means someone else owns or already
// finished the job.
func (c *client) ack(ctx context.Context, id string, token uint64) (acked bool, err error) {
	r, err := c.call(ctx, http.MethodPost, "/v1/jobs/"+id+"/ack", map[string]any{"lease_token": token}, 10*time.Second)
	if err != nil {
		return false, err
	}
	switch r.Status {
	case http.StatusNoContent, http.StatusOK:
		return true, nil
	case http.StatusConflict:
		return false, nil
	}
	return false, fmt.Errorf("ack: status %d %s", r.Status, r.Body)
}

func (c *client) getJob(ctx context.Context, id string) (jobJSON, bool, error) {
	r, err := c.call(ctx, http.MethodGet, "/v1/jobs/"+id, nil, 10*time.Second)
	if err != nil {
		return jobJSON{}, false, err
	}
	if r.Status == http.StatusNotFound {
		return jobJSON{}, false, nil
	}
	if r.Status != http.StatusOK {
		return jobJSON{}, false, fmt.Errorf("get: status %d %s", r.Status, r.Body)
	}
	var j jobJSON
	if err := json.Unmarshal(r.Body, &j); err != nil {
		return jobJSON{}, false, err
	}
	return j, true, nil
}

type clusterStatus struct {
	ID       string `json:"id"`
	Role     string `json:"role"`
	Term     uint64 `json:"term"`
	LeaderID string `json:"leader_id"`
	Commit   uint64 `json:"commit_index"`
	Applied  uint64 `json:"last_applied"`
}

// clusterStatusAt queries one specific node without retries.
func clusterStatusAt(ctx context.Context, hc *http.Client, base string) (clusterStatus, error) {
	ctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, base+"/v1/cluster", nil)
	if err != nil {
		return clusterStatus{}, err
	}
	resp, err := hc.Do(req)
	if err != nil {
		return clusterStatus{}, err
	}
	defer func() { _ = resp.Body.Close() }()
	var st clusterStatus
	if resp.StatusCode != http.StatusOK {
		return st, fmt.Errorf("status %d", resp.StatusCode)
	}
	return st, json.NewDecoder(resp.Body).Decode(&st)
}
