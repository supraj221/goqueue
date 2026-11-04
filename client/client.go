// Package client is the Go SDK for goqueue: a Client for producers and
// workers, and a Worker that runs handlers with leases, heartbeats and
// graceful shutdown.
//
// The Client talks to one or more goqueued nodes. It follows leader hints
// (HTTP 421 plus X-Goqueue-Leader), rotates through the configured URLs on
// connection errors and 503, and backs off exponentially between attempts.
// 4xx responses other than 421 are never retried.
//
// Retries make enqueue at-least-once: if a connection drops after the server
// accepted a job, the retry can create a second job. Set IdempotencyKey on
// EnqueueRequest to make retries exactly-once.
package client

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/supraj221/goqueue/internal/broker"
	"github.com/supraj221/goqueue/internal/job"
)

// Error is returned for non-2xx responses. Unwrap yields the matching broker
// sentinel, so errors.Is(err, broker.ErrLeaseLost) works, and for 421 a
// *broker.NotLeaderError (errors.As).
type Error struct {
	Status  int
	Code    string // "invalid", "not_found", "lease_lost", "not_leader", "unavailable", ...
	Message string
	Leader  string // from X-Goqueue-Leader on 421
}

func (e *Error) Error() string {
	return fmt.Sprintf("goqueue: %d %s: %s", e.Status, e.Code, e.Message)
}

// Unwrap maps the error code to the broker sentinel.
func (e *Error) Unwrap() error {
	switch e.Code {
	case "invalid":
		return broker.ErrInvalid
	case "not_found":
		return broker.ErrNotFound
	case "lease_lost":
		return broker.ErrLeaseLost
	case "unavailable":
		return broker.ErrUnavailable
	case "not_leader":
		return &broker.NotLeaderError{LeaderAddr: e.Leader}
	}
	return nil
}

// Client is a goqueue HTTP client. It is safe for concurrent use and
// implements broker.Broker, so it can stand in for a local backend.
type Client struct {
	hc *http.Client

	mu   sync.Mutex
	urls []string
	cur  int

	attempts       int
	baseBackoff    time.Duration
	maxBackoff     time.Duration
	requestTimeout time.Duration
}

var _ broker.Broker = (*Client)(nil)

// Option configures a Client.
type Option func(*Client)

// WithHTTPClient sets the underlying http.Client. It should not set a Timeout
// shorter than the longest long-poll; use WithRequestTimeout instead.
func WithHTTPClient(hc *http.Client) Option { return func(c *Client) { c.hc = hc } }

// WithRetry sets the maximum number of attempts per call and the exponential
// backoff bounds between them. The default is 10 attempts, 100ms base, 600ms
// cap (about 4 seconds of waiting in total).
func WithRetry(attempts int, base, max time.Duration) Option {
	return func(c *Client) {
		c.attempts, c.baseBackoff, c.maxBackoff = max1(attempts), base, max
	}
}

// WithRequestTimeout bounds each single HTTP attempt (a long-poll adds its
// wait on top). Default 15s.
func WithRequestTimeout(d time.Duration) Option { return func(c *Client) { c.requestTimeout = d } }

func max1(n int) int {
	if n < 1 {
		return 1
	}
	return n
}

// New returns a Client for the given base URLs (for example
// "http://127.0.0.1:8080"). Any one node of a cluster is enough; the client
// discovers the leader from 421 hints.
func New(urls []string, opts ...Option) *Client {
	c := &Client{
		hc:             &http.Client{Transport: &http.Transport{MaxIdleConnsPerHost: 64, IdleConnTimeout: 90 * time.Second}},
		attempts:       10,
		baseBackoff:    100 * time.Millisecond,
		maxBackoff:     600 * time.Millisecond,
		requestTimeout: 15 * time.Second,
	}
	for _, u := range urls {
		c.urls = append(c.urls, normalize(u))
	}
	for _, o := range opts {
		o(c)
	}
	return c
}

func normalize(u string) string {
	u = strings.TrimRight(strings.TrimSpace(u), "/")
	if u != "" && !strings.Contains(u, "://") {
		u = "http://" + u
	}
	return u
}

// Close is a no-op that satisfies broker.Broker.
func (c *Client) Close() error { return nil }

func (c *Client) current() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.urls) == 0 {
		return ""
	}
	return c.urls[c.cur%len(c.urls)]
}

func (c *Client) rotate(from string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.urls) > 0 && c.urls[c.cur%len(c.urls)] == from {
		c.cur = (c.cur + 1) % len(c.urls)
	}
}

// setLeader makes addr the current URL, adding it if unknown.
func (c *Client) setLeader(addr string) {
	addr = normalize(addr)
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

func (c *Client) backoff(attempt int) time.Duration {
	d := c.baseBackoff
	for i := 0; i < attempt && d < c.maxBackoff; i++ {
		d *= 2
	}
	return min(d, c.maxBackoff)
}

func sleep(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// call performs one logical request with leader following and retries. It
// returns the final HTTP status; out (if non-nil) is decoded on 2xx.
func (c *Client) call(ctx context.Context, method, path string, in, out any, extraTimeout time.Duration) (int, error) {
	var payload []byte
	if in != nil {
		var err error
		if payload, err = json.Marshal(in); err != nil {
			return 0, fmt.Errorf("goqueue: encode request: %w", err)
		}
	}
	var lastErr error
	for attempt := 0; attempt < c.attempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		base := c.current()
		if base == "" {
			return 0, fmt.Errorf("%w: no server URLs configured", broker.ErrInvalid)
		}
		status, retry, err := c.once(ctx, method, base+path, payload, out, extraTimeout)
		if err == nil {
			return status, nil
		}
		lastErr = err
		if !retry {
			return status, err
		}
		var apiErr *Error
		if errors.As(err, &apiErr) && apiErr.Status == http.StatusMisdirectedRequest && apiErr.Leader != "" {
			// Hinted leader: switch and retry right away.
			c.setLeader(apiErr.Leader)
			continue
		}
		c.rotate(base)
		if err := sleep(ctx, c.backoff(attempt)); err != nil {
			return 0, err
		}
	}
	return 0, fmt.Errorf("goqueue: giving up after %d attempts: %w", c.attempts, lastErr)
}

// once makes a single attempt. retry reports whether the error is retryable.
func (c *Client) once(ctx context.Context, method, u string, payload []byte, out any, extra time.Duration) (status int, retry bool, err error) {
	rctx, cancel := context.WithTimeout(ctx, c.requestTimeout+extra)
	defer cancel()
	var body io.Reader
	if payload != nil {
		body = bytes.NewReader(payload)
	}
	req, err := http.NewRequestWithContext(rctx, method, u, body)
	if err != nil {
		return 0, false, fmt.Errorf("%w: %v", broker.ErrInvalid, err)
	}
	if payload != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.hc.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return 0, false, ctx.Err()
		}
		var ue *url.Error
		if errors.As(err, &ue) {
			err = ue.Err
		}
		return 0, true, fmt.Errorf("%w: %v", broker.ErrUnavailable, err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, 32<<20))
	if err != nil {
		if ctx.Err() != nil {
			return 0, false, ctx.Err()
		}
		return resp.StatusCode, true, fmt.Errorf("%w: reading response: %v", broker.ErrUnavailable, err)
	}
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		if out != nil && len(data) > 0 {
			if err := json.Unmarshal(data, out); err != nil {
				return resp.StatusCode, false, fmt.Errorf("goqueue: decode response: %w", err)
			}
		}
		return resp.StatusCode, false, nil
	}
	apiErr := &Error{Status: resp.StatusCode, Leader: resp.Header.Get("X-Goqueue-Leader")}
	var eb struct{ Error, Message string }
	if json.Unmarshal(data, &eb) == nil && eb.Error != "" {
		apiErr.Code, apiErr.Message = eb.Error, eb.Message
	} else {
		apiErr.Code, apiErr.Message = http.StatusText(resp.StatusCode), strings.TrimSpace(string(data))
	}
	switch resp.StatusCode {
	case http.StatusMisdirectedRequest:
		if apiErr.Code == "" {
			apiErr.Code = "not_leader"
		}
		return resp.StatusCode, true, apiErr
	case http.StatusServiceUnavailable:
		return resp.StatusCode, true, apiErr
	}
	return resp.StatusCode, false, apiErr
}

type enqueueBody struct {
	Queue          string          `json:"queue"`
	Type           string          `json:"type"`
	Payload        json.RawMessage `json:"payload,omitempty"`
	Priority       int             `json:"priority,omitempty"`
	RunAt          *time.Time      `json:"run_at,omitempty"`
	DelayMs        int64           `json:"delay_ms,omitempty"`
	MaxAttempts    int             `json:"max_attempts,omitempty"`
	IdempotencyKey string          `json:"idempotency_key,omitempty"`
}

// Enqueue adds a job. Created is false when the idempotency key matched an
// existing job.
func (c *Client) Enqueue(ctx context.Context, req broker.EnqueueRequest) (broker.EnqueueResult, error) {
	body := enqueueBody{
		Queue: req.Queue, Type: req.Type, Payload: req.Payload, Priority: req.Priority,
		DelayMs: req.Delay.Milliseconds(), MaxAttempts: req.MaxAttempts, IdempotencyKey: req.IdempotencyKey,
	}
	if !req.RunAt.IsZero() {
		body.RunAt = &req.RunAt
	}
	var j job.Job
	status, err := c.call(ctx, http.MethodPost, "/v1/jobs", body, &j, 0)
	if err != nil {
		return broker.EnqueueResult{}, err
	}
	return broker.EnqueueResult{Job: j, Created: status == http.StatusCreated}, nil
}

// Get returns a job by ID.
func (c *Client) Get(ctx context.Context, id string) (job.Job, error) {
	var j job.Job
	_, err := c.call(ctx, http.MethodGet, "/v1/jobs/"+url.PathEscape(id), nil, &j, 0)
	return j, err
}

// Dequeue claims up to req.Max jobs, long-polling for req.Wait.
func (c *Client) Dequeue(ctx context.Context, req broker.DequeueRequest) ([]job.Job, error) {
	body := map[string]any{
		"queues": req.Queues, "worker_id": req.WorkerID, "lease_ms": req.Lease.Milliseconds(),
		"max": req.Max, "wait_ms": req.Wait.Milliseconds(),
	}
	var resp struct {
		Jobs []job.Job `json:"jobs"`
	}
	if _, err := c.call(ctx, http.MethodPost, "/v1/dequeue", body, &resp, req.Wait); err != nil {
		return nil, err
	}
	return resp.Jobs, nil
}

// Heartbeat extends a lease.
func (c *Client) Heartbeat(ctx context.Context, id string, token uint64, extend time.Duration) (job.Job, error) {
	var j job.Job
	_, err := c.call(ctx, http.MethodPost, "/v1/jobs/"+url.PathEscape(id)+"/heartbeat",
		map[string]any{"lease_token": token, "extend_ms": extend.Milliseconds()}, &j, 0)
	return j, err
}

// Ack marks a job succeeded.
func (c *Client) Ack(ctx context.Context, id string, token uint64) error {
	_, err := c.call(ctx, http.MethodPost, "/v1/jobs/"+url.PathEscape(id)+"/ack", map[string]any{"lease_token": token}, nil, 0)
	return err
}

// Nack records a failure, optionally scheduling a retry.
func (c *Client) Nack(ctx context.Context, id string, token uint64, reason string, retry bool) error {
	_, err := c.call(ctx, http.MethodPost, "/v1/jobs/"+url.PathEscape(id)+"/nack",
		map[string]any{"lease_token": token, "error": reason, "retry": retry}, nil, 0)
	return err
}

// Stats returns job counts per queue and state.
func (c *Client) Stats(ctx context.Context) (broker.Stats, error) {
	var s broker.Stats
	_, err := c.call(ctx, http.MethodGet, "/v1/stats", nil, &s, 0)
	return s, err
}

// Cluster returns the raw JSON from GET /v1/cluster (404 on Postgres).
func (c *Client) Cluster(ctx context.Context) (json.RawMessage, error) {
	var raw json.RawMessage
	_, err := c.call(ctx, http.MethodGet, "/v1/cluster", nil, &raw, 0)
	return raw, err
}
