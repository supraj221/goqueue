// Command goqueued is the goqueue server. It serves the HTTP API in front of
// either a Postgres-backed broker or a node of a Raft-replicated cluster.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"

	"github.com/supraj221/goqueue/internal/api"
	"github.com/supraj221/goqueue/internal/broker"
	"github.com/supraj221/goqueue/internal/broker/pgbroker"
	"github.com/supraj221/goqueue/internal/broker/raftbroker"
	"github.com/supraj221/goqueue/internal/fsm"
	"github.com/supraj221/goqueue/internal/raft"
	"github.com/supraj221/goqueue/internal/raft/httptransport"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "goqueued:", err)
		os.Exit(1)
	}
}

type config struct {
	backend           string
	httpAddr          string
	pgDSN             string
	nodeID            string
	peers             string
	dataDir           string
	snapshotThreshold uint64
	logLevel          string
	retention         time.Duration
	claimConcurrency  int
	rpcTimeout        time.Duration
}

func run() error {
	var c config
	flag.StringVar(&c.backend, "backend", "postgres", "storage backend: postgres or raft")
	flag.StringVar(&c.httpAddr, "http", ":8080", "HTTP listen address (client API; also raft RPCs on the raft backend)")
	flag.StringVar(&c.pgDSN, "pg-dsn", os.Getenv("GOQUEUE_PG_DSN"), "Postgres DSN (postgres backend)")
	flag.StringVar(&c.nodeID, "node-id", "", "this node's ID, must appear in --peers (raft backend)")
	flag.StringVar(&c.peers, "peers", "", "cluster members: id=http://host:port,id=http://host:port,... (raft backend)")
	flag.StringVar(&c.dataDir, "data-dir", "./data", "directory for the Raft log and snapshots (raft backend)")
	flag.Uint64Var(&c.snapshotThreshold, "snapshot-threshold", 10000, "applied entries between Raft snapshots")
	flag.StringVar(&c.logLevel, "log-level", "info", "log level: debug, info, warn, error")
	flag.DurationVar(&c.retention, "retention", time.Hour, "how long finished jobs are kept (raft backend)")
	flag.IntVar(&c.claimConcurrency, "claim-concurrency", 4, "max concurrent Claim proposals (raft backend)")
	flag.DurationVar(&c.rpcTimeout, "raft-rpc-timeout", 5*time.Second, "timeout for each Raft RPC; must cover sending a whole snapshot")
	flag.Parse()

	var lvl slog.Level
	if err := lvl.UnmarshalText([]byte(c.logLevel)); err != nil {
		return fmt.Errorf("bad --log-level: %w", err)
	}
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: lvl}))
	slog.SetDefault(log)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	switch c.backend {
	case "postgres":
		return runPostgres(ctx, c, log)
	case "raft":
		return runRaft(ctx, c, log)
	default:
		return fmt.Errorf("unknown --backend %q (want postgres or raft)", c.backend)
	}
}

func newRegistry() *prometheus.Registry {
	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	return reg
}

func runPostgres(ctx context.Context, c config, log *slog.Logger) error {
	if c.pgDSN == "" {
		return errors.New("--pg-dsn (or GOQUEUE_PG_DSN) is required for the postgres backend")
	}
	b, err := pgbroker.New(ctx, c.pgDSN, pgbroker.Options{Logger: log})
	if err != nil {
		return fmt.Errorf("postgres: %w", err)
	}
	defer func() { _ = b.Close() }()
	srv := api.NewServer(b, api.Options{Logger: log, Registry: newRegistry()})
	log.Info("goqueued listening", "backend", "postgres", "addr", c.httpAddr)
	return srv.Serve(ctx, c.httpAddr)
}

// parsePeers parses "n1=http://a:1,n2=http://b:2".
func parsePeers(s string) (map[string]string, error) {
	out := map[string]string{}
	for _, part := range strings.Split(s, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		id, addr, ok := strings.Cut(part, "=")
		if !ok || id == "" || addr == "" {
			return nil, fmt.Errorf("bad peer %q, want id=url", part)
		}
		if !strings.Contains(addr, "://") {
			addr = "http://" + addr
		}
		out[id] = strings.TrimRight(addr, "/")
	}
	if len(out) == 0 {
		return nil, errors.New("--peers is required for the raft backend")
	}
	return out, nil
}

func runRaft(ctx context.Context, c config, log *slog.Logger) error {
	peers, err := parsePeers(c.peers)
	if err != nil {
		return err
	}
	if _, ok := peers[c.nodeID]; !ok {
		return fmt.Errorf("--node-id %q is not in --peers", c.nodeID)
	}
	ids := make([]string, 0, len(peers))
	for id := range peers {
		ids = append(ids, id)
	}
	sort.Strings(ids)

	store, err := raft.NewFileStorage(c.dataDir)
	if err != nil {
		return fmt.Errorf("storage: %w", err)
	}
	defer func() {
		if err := store.Close(); err != nil {
			log.Error("closing raft storage", "err", err)
		}
	}()

	q := fsm.New()
	tr := httptransport.New(peers, c.rpcTimeout)
	node, err := raft.NewNode(raft.Config{
		ID: c.nodeID, Peers: ids, SnapshotThreshold: c.snapshotThreshold,
		Logger: log.With("component", "raft", "node", c.nodeID),
	}, q, store, tr)
	if err != nil {
		return fmt.Errorf("raft: %w", err)
	}

	b := raftbroker.New(node, q, raftbroker.Options{
		PeerAddrs: peers, Logger: log.With("component", "raftbroker"), Retention: c.retention, PurgeInterval: min(time.Minute, max(time.Second, c.retention/4)), ClaimConcurrency: c.claimConcurrency,
	})
	reg := newRegistry()
	if err := b.RegisterMetrics(reg); err != nil {
		return fmt.Errorf("metrics: %w", err)
	}

	srv := api.NewServer(b, api.Options{
		Logger:   log,
		Registry: reg,
		Cluster:  func() any { return node.Status() },
		Ready: func() error {
			if node.Status().LeaderID == "" {
				return errors.New("no leader known")
			}
			return nil
		},
		Extra: map[string]http.Handler{"/raft/": httptransport.NewHandler(node)},
	})

	if err := node.Start(); err != nil {
		return fmt.Errorf("raft start: %w", err)
	}
	log.Info("goqueued listening", "backend", "raft", "node", c.nodeID, "addr", c.httpAddr, "peers", len(peers))
	serveErr := srv.Serve(ctx, c.httpAddr)

	log.Info("shutting down")
	_ = b.Close()
	node.Stop()
	if serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
		return serveErr
	}
	return nil
}

var _ broker.Broker = (*raftbroker.Broker)(nil)
