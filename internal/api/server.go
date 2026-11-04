// Package api serves the goqueue HTTP/JSON API (docs/PLAN.md section 6) on
// top of any broker.Broker.
package api

import (
	"log/slog"
	"net/http"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/supraj221/goqueue/internal/broker"
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

// Server is the HTTP API.
//
// SCAFFOLD: Agent B implements this. The constructor and Handler signatures
// are the contract cmd/goqueued (Agent C) codes against.
type Server struct {
	b    broker.Broker
	opts Options
}

// NewServer builds a Server for b.
func NewServer(b broker.Broker, opts Options) *Server {
	return &Server{b: b, opts: opts}
}

// Handler returns the root handler, including /metrics, /healthz, and /readyz.
func (s *Server) Handler() http.Handler {
	return http.NotFoundHandler()
}
