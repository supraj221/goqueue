package raft

import (
	"context"
	"errors"
)

// Node is one member of a Raft cluster.
//
// SCAFFOLD: the method set below is the contract. Agent A replaces the bodies
// (and adds fields) without changing any signature.
type Node struct{}

// NewNode creates a node. It loads persisted state and the latest snapshot
// (calling sm.Restore) but does not start timers or send RPCs until Start.
func NewNode(cfg Config, sm StateMachine, st Storage, tr Transport) (*Node, error) {
	return nil, errors.New("raft: not implemented")
}

// Start begins the node's event loop.
func (n *Node) Start() error { return errors.New("raft: not implemented") }

// Stop halts the node. Pending Propose and ReadIndex calls return ErrStopped.
// It does not close the Storage.
func (n *Node) Stop() {}

// Propose replicates data as an EntryCommand and waits until it is committed
// and applied on this node. It returns StateMachine.Apply's result and the
// entry's log index. On a non-leader it returns ErrNotLeader immediately.
func (n *Node) Propose(ctx context.Context, data []byte) (result any, index uint64, err error) {
	return nil, 0, errors.New("raft: not implemented")
}

// ReadIndex blocks until it is safe to serve a linearizable read from this
// node's state machine: the node confirmed it is still leader with a majority
// and has applied everything committed at the time of the call.
func (n *Node) ReadIndex(ctx context.Context) error { return errors.New("raft: not implemented") }

// Status returns a snapshot of the node's state.
func (n *Node) Status() Status { return Status{} }

// LeaderChanges returns a channel that receives true when this node becomes
// leader and false when it stops being leader. The channel is buffered;
// slow readers may miss intermediate values but always see the latest one
// eventually. Every call returns the same channel.
func (n *Node) LeaderChanges() <-chan bool { return nil }

// HandleRequestVote handles an inbound RequestVote RPC.
func (n *Node) HandleRequestVote(ctx context.Context, req *RequestVoteRequest) (*RequestVoteResponse, error) {
	return nil, errors.New("raft: not implemented")
}

// HandleAppendEntries handles an inbound AppendEntries RPC.
func (n *Node) HandleAppendEntries(ctx context.Context, req *AppendEntriesRequest) (*AppendEntriesResponse, error) {
	return nil, errors.New("raft: not implemented")
}

// HandleInstallSnapshot handles an inbound InstallSnapshot RPC.
func (n *Node) HandleInstallSnapshot(ctx context.Context, req *InstallSnapshotRequest) (*InstallSnapshotResponse, error) {
	return nil, errors.New("raft: not implemented")
}
