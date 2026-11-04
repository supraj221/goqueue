// Package memnet is an in-memory network for running real Raft clusters inside
// a test process, with fault injection.
//
// Usage:
//
//	net := memnet.New()
//	for _, id := range ids {
//		node, _ := raft.NewNode(cfg, sm, st, net.Transport(id))
//		net.Register(id, node) // *raft.Node implements raft.RPCHandler
//		node.Start()
//	}
//	net.Disconnect("n1")          // n1 can neither send nor receive
//	net.Partition([]string{"n1", "n2"}, []string{"n3"})
//	net.Heal()                    // everything reachable again
//	net.SetDropRate(0.1)          // lose 10% of RPCs
//	net.SetDelay(time.Millisecond, 20*time.Millisecond) // random latency, reorders
//
// To simulate a crash and restart, Stop the node and Register the new one
// under the same ID (or Unregister it for a while). All methods are safe for
// concurrent use.
package memnet

import (
	"context"
	"errors"
	"math/rand"
	"sync"
	"time"

	"github.com/supraj221/goqueue/internal/raft"
)

// Errors returned by the transports.
var (
	// ErrUnreachable: the destination is disconnected, partitioned away from
	// the sender, or not registered.
	ErrUnreachable = errors.New("memnet: destination unreachable")
	// ErrDropped: the message (request or response) was dropped.
	ErrDropped = errors.New("memnet: message dropped")
)

// unreachableDelay is how long a call to an unreachable node takes to fail,
// roughly a fast connection error. It keeps retry loops from spinning.
const unreachableDelay = 3 * time.Millisecond

// Network connects Raft nodes in memory.
type Network struct {
	mu       sync.Mutex
	handlers map[string]raft.RPCHandler
	down     map[string]bool
	group    map[string]int // partition group per node; nil means no partition
	dropRate float64
	minDelay time.Duration
	maxDelay time.Duration
	rng      *rand.Rand
}

// New returns a fully connected, reliable, zero-latency network.
func New() *Network {
	return &Network{
		handlers: map[string]raft.RPCHandler{},
		down:     map[string]bool{},
		rng:      rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// Register attaches the inbound handler for node id, replacing any previous one.
func (n *Network) Register(id string, h raft.RPCHandler) {
	n.mu.Lock()
	n.handlers[id] = h
	n.mu.Unlock()
}

// Unregister removes node id; RPCs to it fail with ErrUnreachable.
func (n *Network) Unregister(id string) {
	n.mu.Lock()
	delete(n.handlers, id)
	n.mu.Unlock()
}

// Disconnect cuts node id off from everyone, in both directions, including
// RPCs already in flight.
func (n *Network) Disconnect(id string) {
	n.mu.Lock()
	n.down[id] = true
	n.mu.Unlock()
}

// Connect undoes Disconnect.
func (n *Network) Connect(id string) {
	n.mu.Lock()
	delete(n.down, id)
	n.mu.Unlock()
}

// Partition splits the network: nodes can only talk to nodes in the same
// group. Nodes not listed in any group are isolated from everybody.
func (n *Network) Partition(groups ...[]string) {
	g := map[string]int{}
	for i, grp := range groups {
		for _, id := range grp {
			g[id] = i + 1
		}
	}
	n.mu.Lock()
	n.group = g
	n.mu.Unlock()
}

// Heal removes all partitions and reconnects every disconnected node.
func (n *Network) Heal() {
	n.mu.Lock()
	n.group = nil
	n.down = map[string]bool{}
	n.mu.Unlock()
}

// SetDropRate makes each RPC fail with probability p (0..1). Half of the drops
// lose the request before the handler runs; half lose the response after it
// ran, so the caller sees an error even though the effect happened.
func (n *Network) SetDropRate(p float64) {
	n.mu.Lock()
	n.dropRate = p
	n.mu.Unlock()
}

// SetDelay adds a uniformly random latency in [min, max) to each direction of
// every RPC. Because delays are independent, messages overtake one another,
// which reorders them.
func (n *Network) SetDelay(min, max time.Duration) {
	n.mu.Lock()
	n.minDelay, n.maxDelay = min, max
	n.mu.Unlock()
}

// Transport returns the raft.Transport that node from sends through.
func (n *Network) Transport(from string) raft.Transport {
	return &transport{net: n, from: from}
}

type plan struct {
	h                   raft.RPCHandler
	dropReq, dropResp   bool
	delayReq, delayResp time.Duration
}

func (n *Network) reachableLocked(from, to string) bool {
	if n.down[from] || n.down[to] {
		return false
	}
	if n.group != nil {
		gf, ok1 := n.group[from]
		gt, ok2 := n.group[to]
		if !ok1 || !ok2 || gf != gt {
			return false
		}
	}
	return true
}

func (n *Network) delayLocked() time.Duration {
	if n.maxDelay <= n.minDelay {
		return n.minDelay
	}
	return n.minDelay + time.Duration(n.rng.Int63n(int64(n.maxDelay-n.minDelay)))
}

func (n *Network) plan(from, to string) (plan, bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	h, ok := n.handlers[to]
	if !ok || !n.reachableLocked(from, to) {
		return plan{}, false
	}
	p := plan{h: h, delayReq: n.delayLocked(), delayResp: n.delayLocked()}
	if n.dropRate > 0 && n.rng.Float64() < n.dropRate {
		if n.rng.Intn(2) == 0 {
			p.dropReq = true
		} else {
			p.dropResp = true
		}
	}
	return p, true
}

func (n *Network) reachable(from, to string) bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.reachableLocked(from, to)
}

func sleep(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// do runs one RPC: request leg, handler, response leg. Reachability is
// checked again at each leg so a partition created mid-flight takes effect.
func (n *Network) do(ctx context.Context, from, to string, invoke func(raft.RPCHandler) error) error {
	p, ok := n.plan(from, to)
	if !ok {
		if err := sleep(ctx, unreachableDelay); err != nil {
			return err
		}
		return ErrUnreachable
	}
	if err := sleep(ctx, p.delayReq); err != nil {
		return err
	}
	if p.dropReq {
		return ErrDropped
	}
	if !n.reachable(from, to) {
		return ErrUnreachable
	}
	if err := invoke(p.h); err != nil {
		return err
	}
	if err := sleep(ctx, p.delayResp); err != nil {
		return err
	}
	if p.dropResp {
		return ErrDropped
	}
	if !n.reachable(to, from) {
		return ErrUnreachable
	}
	return nil
}

type transport struct {
	net  *Network
	from string
}

func (t *transport) RequestVote(ctx context.Context, to string, req *raft.RequestVoteRequest) (*raft.RequestVoteResponse, error) {
	var resp *raft.RequestVoteResponse
	err := t.net.do(ctx, t.from, to, func(h raft.RPCHandler) (err error) {
		resp, err = h.HandleRequestVote(ctx, req)
		return
	})
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (t *transport) AppendEntries(ctx context.Context, to string, req *raft.AppendEntriesRequest) (*raft.AppendEntriesResponse, error) {
	var resp *raft.AppendEntriesResponse
	err := t.net.do(ctx, t.from, to, func(h raft.RPCHandler) (err error) {
		resp, err = h.HandleAppendEntries(ctx, req)
		return
	})
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (t *transport) InstallSnapshot(ctx context.Context, to string, req *raft.InstallSnapshotRequest) (*raft.InstallSnapshotResponse, error) {
	var resp *raft.InstallSnapshotResponse
	err := t.net.do(ctx, t.from, to, func(h raft.RPCHandler) (err error) {
		resp, err = h.HandleInstallSnapshot(ctx, req)
		return
	})
	if err != nil {
		return nil, err
	}
	return resp, nil
}
