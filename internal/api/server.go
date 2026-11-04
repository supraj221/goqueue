// Package api serves the goqueue HTTP/JSON API (docs/PLAN.md section 6) on
// top of any broker.Broker.
package api

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"

	"github.com/supraj221/goqueue/internal/broker"
	"github.com/supraj221/goqueue/internal/metrics"
)

// ClusterStatusFunc returns the value served at GET /v1/cluster. It is nil
// for backends without a cluster (Postgres), which then answer 404.
type ClusterStatusFunc func() any

// Options configures a Server.
type Options struct {
	Logger *slog.Logger
	// Cluster, if set, backs GET /v1/cluster.
	Cluster ClusterStatusFunc
	// Ready reports readiness for GET /readyz. If nil, always ready.
	Ready func() error
	// Extra handlers mounted on the same mux, for example the raft RPC
	// handlers under "/raft/". Keys are mux patterns.
	Extra map[string]http.Handler
	// Registry receives the API's collectors and is served at /metrics.
	// Callers may register their own collectors on it (raft metrics). If nil,
	// the server creates its own.
	Registry *prometheus.Registry
}

// maxBodyBytes caps every request body.
const maxBodyBytes = 1 << 20

// Server is the HTTP API.
type Server struct {
	b       broker.Broker
	opts    Options
	log     *slog.Logger
	reg     *prometheus.Registry
	metrics *metrics.Metrics
	handler http.Handler
	queues  *queueCache
	now     func() time.Time
}

// NewServer builds a Server for b.
func NewServer(b broker.Broker, opts Options) *Server {
	s := &Server{b: b, opts: opts, now: time.Now, queues: newQueueCache()}
	s.log = opts.Logger
	if s.log == nil {
		s.log = slog.Default()
	}
	s.reg = opts.Registry
	if s.reg == nil {
		s.reg = prometheus.NewRegistry()
		s.reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	}
	s.metrics = metrics.New(s.reg)
	s.metrics.WatchDepth(b)
	s.handler = s.routes()
	return s
}

// Handler returns the root handler, including /metrics, /healthz, and /readyz.
func (s *Server) Handler() http.Handler { return s.handler }

// Metrics returns the server's queue and HTTP metrics.
func (s *Server) Metrics() *metrics.Metrics { return s.metrics }

// Registry returns the Prometheus registry served at /metrics.
func (s *Server) Registry() *prometheus.Registry { return s.reg }

// Serve listens on addr until ctx is cancelled, then shuts down gracefully,
// giving in-flight requests (including long-polls) up to 10 seconds.
func (s *Server) Serve(ctx context.Context, addr string) error {
	srv := &http.Server{
		Addr:              addr,
		Handler:           s.Handler(),
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      60 * time.Second,
		IdleTimeout:       2 * time.Minute,
	}
	errc := make(chan error, 1)
	go func() { errc <- srv.ListenAndServe() }()
	select {
	case err := <-errc:
		return err
	case <-ctx.Done():
	}
	sctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(sctx); err != nil {
		return err
	}
	if err := <-errc; err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

// queueCache remembers which queue a claimed job belongs to, so ack and nack
// can label metrics without an extra broker read. Entries are removed on
// ack/nack and expire after an hour if the worker disappears.
type queueCache struct {
	mu sync.Mutex
	m  map[string]queueEntry
}

type queueEntry struct {
	queue string
	at    time.Time
}

const queueCacheMax = 100_000

func newQueueCache() *queueCache { return &queueCache{m: map[string]queueEntry{}} }

func (c *queueCache) put(id, queue string, now time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.m) >= queueCacheMax {
		for k, v := range c.m {
			if now.Sub(v.at) > time.Hour {
				delete(c.m, k)
			}
		}
		if len(c.m) >= queueCacheMax {
			clear(c.m)
		}
	}
	c.m[id] = queueEntry{queue, now}
}

func (c *queueCache) take(id string) (string, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.m[id]
	delete(c.m, id)
	return e.queue, ok
}
