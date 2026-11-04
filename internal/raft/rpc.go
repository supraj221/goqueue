package raft

import "context"

// RPCHandler is the inbound side of the Raft RPCs. *Node implements it, and
// network layers (memnet, httptransport) deliver incoming RPCs through it.
type RPCHandler interface {
	HandleRequestVote(ctx context.Context, req *RequestVoteRequest) (*RequestVoteResponse, error)
	HandleAppendEntries(ctx context.Context, req *AppendEntriesRequest) (*AppendEntriesResponse, error)
	HandleInstallSnapshot(ctx context.Context, req *InstallSnapshotRequest) (*InstallSnapshotResponse, error)
}

var _ RPCHandler = (*Node)(nil)
