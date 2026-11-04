package raft_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/supraj221/goqueue/internal/raft"
	"github.com/supraj221/goqueue/internal/raft/memnet"
)

// ---------------------------------------------------------------------------
// Recording state machine
// ---------------------------------------------------------------------------

// recSM records every applied command with its log index. Its snapshot is the
// full record, so a restored node's sequence is comparable with the others.
type recSM struct {
	mu  sync.Mutex
	idx []uint64
	val []string
}

type recSnap struct {
	Idx []uint64
	Val []string
}

func (s *recSM) Apply(e raft.LogEntry) any {
	s.mu.Lock()
	s.idx = append(s.idx, e.Index)
	s.val = append(s.val, string(e.Data))
	s.mu.Unlock()
	return string(e.Data)
}

func (s *recSM) Snapshot() ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return json.Marshal(recSnap{Idx: s.idx, Val: s.val})
}

func (s *recSM) Restore(data []byte) error {
	var rs recSnap
	if err := json.Unmarshal(data, &rs); err != nil {
		return err
	}
	s.mu.Lock()
	s.idx, s.val = rs.Idx, rs.Val
	s.mu.Unlock()
	return nil
}

func (s *recSM) values() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.val...)
}

func (s *recSM) lookup(index uint64) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	i := sort.Search(len(s.idx), func(i int) bool { return s.idx[i] >= index })
	if i < len(s.idx) && s.idx[i] == index {
		return s.val[i], true
	}
	return "", false
}

// ---------------------------------------------------------------------------
// Counting transport
// ---------------------------------------------------------------------------

type counters struct {
	rejections atomic.Int64 // AppendEntries answered Success=false
	snapshots  atomic.Int64 // InstallSnapshot RPCs sent
}

type countingTransport struct {
	raft.Transport
	c *counters
}

func (t countingTransport) AppendEntries(ctx context.Context, to string, req *raft.AppendEntriesRequest) (*raft.AppendEntriesResponse, error) {
	resp, err := t.Transport.AppendEntries(ctx, to, req)
	if err == nil && !resp.Success && resp.Term == req.Term {
		t.c.rejections.Add(1)
	}
	return resp, err
}

func (t countingTransport) InstallSnapshot(ctx context.Context, to string, req *raft.InstallSnapshotRequest) (*raft.InstallSnapshotResponse, error) {
	t.c.snapshots.Add(1)
	return t.Transport.InstallSnapshot(ctx, to, req)
}

// ---------------------------------------------------------------------------
// Cluster harness
// ---------------------------------------------------------------------------

type cluster struct {
	t   testing.TB
	net *memnet.Network
	ids []string
	cnt counters

	snapThreshold uint64
	electMin      time.Duration
	electMax      time.Duration
	heartbeat     time.Duration

	mu       sync.Mutex
	stores   []raft.Storage
	dirs     []string // non-nil: use FileStorage in these directories
	sms      []*recSM
	nodes    []*raft.Node
	up       []bool
	isolated []bool

	// safety checking
	leaderMu      sync.Mutex
	leadersByTerm map[uint64]string
	stopCheck     chan struct{}
	checkWG       sync.WaitGroup
}

func quietLogger() *slog.Logger {
	if os.Getenv("RAFT_TEST_LOG") != "" {
		return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))
	}
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func newCluster(t testing.TB, n int) *cluster { return newClusterSnap(t, n, 0) }

func newClusterSnap(t testing.TB, n int, snapThreshold uint64) *cluster {
	return newClusterTimes(t, n, snapThreshold, 50*time.Millisecond, 100*time.Millisecond, 15*time.Millisecond)
}

// newFileCluster is like newClusterSnap but every node persists to a FileStorage.
func newFileCluster(t testing.TB, n int, snapThreshold uint64) *cluster {
	dirs := make([]string, n)
	for i := range dirs {
		dirs[i] = filepath.Join(t.TempDir(), fmt.Sprintf("node%d", i))
	}
	return newClusterFull(t, n, snapThreshold, 50*time.Millisecond, 100*time.Millisecond, 15*time.Millisecond, dirs)
}

func newClusterTimes(t testing.TB, n int, snapThreshold uint64, electMin, electMax, heartbeat time.Duration) *cluster {
	return newClusterFull(t, n, snapThreshold, electMin, electMax, heartbeat, nil)
}

func newClusterFull(t testing.TB, n int, snapThreshold uint64, electMin, electMax, heartbeat time.Duration, dirs []string) *cluster {
	c := &cluster{
		t:             t,
		electMin:      electMin,
		electMax:      electMax,
		heartbeat:     heartbeat,
		dirs:          dirs,
		net:           memnet.New(),
		snapThreshold: snapThreshold,
		leadersByTerm: map[uint64]string{},
		stopCheck:     make(chan struct{}),
	}
	for i := 0; i < n; i++ {
		c.ids = append(c.ids, fmt.Sprintf("n%d", i))
		if dirs != nil {
			c.stores = append(c.stores, nil)
		} else {
			c.stores = append(c.stores, raft.NewMemStorage())
		}
		c.sms = append(c.sms, nil)
		c.nodes = append(c.nodes, nil)
		c.up = append(c.up, false)
		c.isolated = append(c.isolated, false)
	}
	for i := 0; i < n; i++ {
		c.start(i)
	}
	c.checkWG.Add(1)
	go c.safetyLoop()
	t.Cleanup(c.cleanup)
	return c
}

func (c *cluster) config(i int) raft.Config {
	return raft.Config{
		ID:                  c.ids[i],
		Peers:               c.ids,
		ElectionTimeoutMin:  c.electMin,
		ElectionTimeoutMax:  c.electMax,
		HeartbeatInterval:   c.heartbeat,
		MaxEntriesPerAppend: 64,
		SnapshotThreshold:   c.snapThreshold,
		Logger:              quietLogger(),
	}
}

// start creates a fresh state machine and node over the persisted storage.
func (c *cluster) start(i int) {
	if c.dirs != nil && c.stores[i] == nil {
		fs, err := raft.NewFileStorage(c.dirs[i])
		if err != nil {
			c.t.Fatalf("NewFileStorage(%d): %v", i, err)
		}
		c.stores[i] = fs
	}
	sm := &recSM{}
	tr := countingTransport{Transport: c.net.Transport(c.ids[i]), c: &c.cnt}
	node, err := raft.NewNode(c.config(i), sm, c.stores[i], tr)
	if err != nil {
		c.t.Fatalf("NewNode(%d): %v", i, err)
	}
	c.mu.Lock()
	c.sms[i], c.nodes[i], c.up[i] = sm, node, true
	c.mu.Unlock()
	c.net.Register(c.ids[i], node)
	if err := node.Start(); err != nil {
		c.t.Fatalf("Start(%d): %v", i, err)
	}
}

// crash stops node i but keeps its storage, like a process kill.
func (c *cluster) crash(i int) {
	c.mu.Lock()
	node := c.nodes[i]
	c.up[i] = false
	c.mu.Unlock()
	c.net.Unregister(c.ids[i])
	node.Stop()
	if c.dirs != nil {
		_ = c.stores[i].Close()
		c.stores[i] = nil
	}
}

func (c *cluster) restart(i int) { c.start(i) }

func (c *cluster) crashAll() {
	for i := range c.ids {
		if c.isUp(i) {
			c.crash(i)
		}
	}
}

func (c *cluster) cleanup() {
	close(c.stopCheck)
	c.checkWG.Wait()
	c.mu.Lock()
	var nodes []*raft.Node
	for i, n := range c.nodes {
		if c.up[i] {
			nodes = append(nodes, n)
		}
	}
	c.mu.Unlock()
	for _, n := range nodes {
		n.Stop()
	}
	if c.dirs != nil {
		for i, st := range c.stores {
			if st != nil {
				_ = st.Close()
				c.stores[i] = nil
			}
		}
	}
	c.checkApplied()
}

func (c *cluster) isUp(i int) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.up[i]
}

func (c *cluster) node(i int) *raft.Node {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.nodes[i]
}

func (c *cluster) sm(i int) *recSM {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.sms[i]
}

func (c *cluster) status(i int) raft.Status { return c.node(i).Status() }

func (c *cluster) connected(i int) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.up[i] && !c.isolated[i]
}

func (c *cluster) disconnect(i int) {
	c.mu.Lock()
	c.isolated[i] = true
	c.mu.Unlock()
	c.net.Disconnect(c.ids[i])
}

func (c *cluster) connect(i int) {
	c.mu.Lock()
	c.isolated[i] = false
	c.mu.Unlock()
	c.net.Connect(c.ids[i])
}

// ---------------------------------------------------------------------------
// Safety invariants
// ---------------------------------------------------------------------------

func (c *cluster) safetyLoop() {
	defer c.checkWG.Done()
	tick := time.NewTicker(4 * time.Millisecond)
	defer tick.Stop()
	n := 0
	for {
		select {
		case <-c.stopCheck:
			return
		case <-tick.C:
		}
		c.observeLeaders()
		if n++; n%10 == 0 {
			c.checkApplied()
		}
	}
}

// observeLeaders enforces Election Safety: at most one leader per term.
func (c *cluster) observeLeaders() {
	c.mu.Lock()
	var nodes []*raft.Node
	for i, n := range c.nodes {
		if c.up[i] {
			nodes = append(nodes, n)
		}
	}
	c.mu.Unlock()
	for _, n := range nodes {
		s := n.Status()
		if s.Role != raft.Leader {
			continue
		}
		c.leaderMu.Lock()
		if prev, ok := c.leadersByTerm[s.Term]; ok && prev != s.ID {
			c.t.Errorf("ELECTION SAFETY VIOLATION: term %d has two leaders: %s and %s", s.Term, prev, s.ID)
		}
		c.leadersByTerm[s.Term] = s.ID
		c.leaderMu.Unlock()
	}
}

// checkApplied enforces State Machine Safety: every node's applied sequence
// is a prefix of the longest one (same index and value at each position).
func (c *cluster) checkApplied() {
	c.mu.Lock()
	sms := append([]*recSM(nil), c.sms...)
	c.mu.Unlock()
	type seq struct {
		idx []uint64
		val []string
	}
	var seqs []seq
	for _, s := range sms {
		if s == nil {
			continue
		}
		s.mu.Lock()
		seqs = append(seqs, seq{append([]uint64(nil), s.idx...), append([]string(nil), s.val...)})
		s.mu.Unlock()
	}
	for i, s := range seqs {
		for k := 1; k < len(s.idx); k++ {
			if s.idx[k] <= s.idx[k-1] {
				c.t.Errorf("node %d applied index %d after %d: entries must be applied once, in order", i, s.idx[k], s.idx[k-1])
				break
			}
		}
	}
	longest := 0
	for i, s := range seqs {
		if len(s.val) > len(seqs[longest].val) {
			longest = i
		}
	}
	for i, s := range seqs {
		for k := range s.val {
			if s.idx[k] != seqs[longest].idx[k] || s.val[k] != seqs[longest].val[k] {
				c.t.Errorf("STATE MACHINE SAFETY VIOLATION: node %d position %d applied (%d,%q), longest has (%d,%q)",
					i, k, s.idx[k], s.val[k], seqs[longest].idx[k], seqs[longest].val[k])
				break
			}
		}
	}
}

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

func waitFor(timeout time.Duration, cond func() bool) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(5 * time.Millisecond)
	}
	return cond()
}

// leaders returns the connected nodes that currently believe they lead.
func (c *cluster) leaders(among []int) []int {
	var out []int
	for _, i := range among {
		if c.connected(i) && c.status(i).Role == raft.Leader {
			out = append(out, i)
		}
	}
	return out
}

func (c *cluster) connectedIDs() []int {
	var out []int
	for i := range c.ids {
		if c.connected(i) {
			out = append(out, i)
		}
	}
	return out
}

// checkOneLeader waits until exactly one connected node (optionally limited to
// among) is leader, stably, and returns it.
func (c *cluster) checkOneLeader(among ...int) int {
	c.t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		set := among
		if len(set) == 0 {
			set = c.connectedIDs()
		}
		ls := c.leaders(set)
		if len(ls) == 1 {
			term := c.status(ls[0]).Term
			time.Sleep(20 * time.Millisecond)
			ls2 := c.leaders(set)
			if len(ls2) == 1 && ls2[0] == ls[0] && c.status(ls[0]).Term == term {
				return ls[0]
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	c.t.Fatalf("expected one leader, got none or several")
	return -1
}

// checkNoLeader fails if any connected node is leader.
func (c *cluster) checkNoLeader(among ...int) {
	c.t.Helper()
	set := among
	if len(set) == 0 {
		set = c.connectedIDs()
	}
	if ls := c.leaders(set); len(ls) > 0 {
		c.t.Fatalf("node %d claims to be leader but should not", ls[0])
	}
}

// nCommitted returns how many nodes have applied the entry at index, failing
// if they disagree on its value.
func (c *cluster) nCommitted(index uint64) (int, string) {
	c.t.Helper()
	count, val := 0, ""
	for i := range c.ids {
		if !c.isUp(i) {
			continue
		}
		v, ok := c.sm(i).lookup(index)
		if !ok {
			continue
		}
		if count > 0 && v != val {
			c.t.Fatalf("nodes applied different values at index %d: %q vs %q", index, val, v)
		}
		count, val = count+1, v
	}
	return count, val
}

func (c *cluster) waitCommitted(index uint64, want int, cmd string, timeout time.Duration) bool {
	return waitFor(timeout, func() bool {
		n, v := c.nCommitted(index)
		return n >= want && v == cmd
	})
}

// one submits cmd to the current leader, retrying across leader changes, and
// returns its index once at least expected nodes have applied it.
func (c *cluster) one(cmd string, expected int, retry bool) uint64 {
	c.t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	proposed := false
	for time.Now().Before(deadline) {
		for i := range c.ids {
			if !c.connected(i) {
				continue
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			res, idx, err := c.node(i).Propose(ctx, []byte(cmd))
			cancel()
			if err != nil {
				continue
			}
			if res != cmd {
				c.t.Fatalf("Apply result %v, want %q", res, cmd)
			}
			if c.waitCommitted(idx, expected, cmd, 4*time.Second) {
				return idx
			}
			proposed = true
		}
		if proposed && !retry {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	c.t.Fatalf("one(%q) failed to reach agreement on %d nodes", cmd, expected)
	return 0
}

// proposeAsync fires a proposal at node i and ignores the outcome.
func (c *cluster) proposeAsync(i int, cmd string, timeout time.Duration) {
	node := c.node(i)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		// The proposal may legitimately fail (leader loss, timeout) during chaos.
		_, _, _ = node.Propose(ctx, []byte(cmd))
	}()
}

func isOneOf(err error, targets ...error) bool {
	for _, t := range targets {
		if errors.Is(err, t) {
			return true
		}
	}
	return false
}

// dedupe collapses adjacent duplicates. one() re-proposes after an
// indeterminate failure (ErrLeadershipLost), which can commit a command twice;
// that is legitimate Raft behavior, and exact-sequence checks must allow it.
func dedupe(vals []string) []string {
	var out []string
	for i, v := range vals {
		if i == 0 || v != vals[i-1] {
			out = append(out, v)
		}
	}
	return out
}
