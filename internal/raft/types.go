// Package raft is a from-scratch implementation of the Raft consensus
// algorithm (Ongaro and Ousterhout, "In Search of an Understandable Consensus
// Algorithm", 2014), following Figure 2 of the paper, plus PreVote, fast log
// backtracking, snapshots, and ReadIndex.
//
// This file is the public contract used by the rest of goqueue. The
// implementation lives in the other files of this package.
package raft

import (
	"context"
	"errors"
	"log/slog"
	"time"
)

// LogEntry is one entry in the replicated log. Index starts at 1.
type LogEntry struct {
	Index uint64    `json:"index"`
	Term  uint64    `json:"term"`
	Type  EntryType `json:"type"`
	Data  []byte    `json:"data,omitempty"`
}

// EntryType distinguishes client commands from internal entries.
type EntryType uint8

const (
	EntryCommand EntryType = iota // passed to StateMachine.Apply
	EntryNoop                     // appended by a new leader; never passed to Apply
)

// StateMachine is the replicated application. Apply is called with committed
// EntryCommand entries, in log order, exactly once per entry per process
// lifetime (after a restart, entries after the last snapshot are re-applied
// on top of Restore). Apply must be deterministic.
type StateMachine interface {
	Apply(e LogEntry) any
	// Snapshot serializes the full state as of the last applied entry. It is
	// called on the apply goroutine, so no Apply runs concurrently.
	Snapshot() ([]byte, error)
	// Restore replaces the full state with a snapshot.
	Restore(data []byte) error
}

// HardState is the state Raft must persist before answering any RPC.
type HardState struct {
	Term     uint64 `json:"term"`
	VotedFor string `json:"voted_for"`
}

// Snapshot is a compacted prefix of the log.
type Snapshot struct {
	LastIndex uint64 `json:"last_index"`
	LastTerm  uint64 `json:"last_term"`
	Data      []byte `json:"data"`
}

// Storage persists Raft state. Every write method must be durable (fsynced)
// before it returns. Implementations: MemStorage (tests) and FileStorage.
type Storage interface {
	LoadHardState() (HardState, error)
	SaveHardState(HardState) error

	// FirstIndex is the index of the first entry still in the log (after the
	// snapshot); LastIndex is the index of the last entry, or the snapshot's
	// LastIndex if the log is empty.
	FirstIndex() (uint64, error)
	LastIndex() (uint64, error)
	// Entries returns entries in [lo, hi).
	Entries(lo, hi uint64) ([]LogEntry, error)
	// Term returns the term of the entry at index, including the snapshot's
	// LastIndex.
	Term(index uint64) (uint64, error)
	// Append writes entries; if they overlap existing entries, the existing
	// suffix from the first entry's index onward is replaced.
	Append(entries []LogEntry) error

	LoadSnapshot() (Snapshot, error) // zero Snapshot if none
	// SaveSnapshot stores the snapshot and discards log entries up to and
	// including snap.LastIndex.
	SaveSnapshot(snap Snapshot) error

	Close() error
}

// Transport sends RPCs to peers. Inbound RPCs are delivered by calling the
// Node's Handle* methods.
type Transport interface {
	RequestVote(ctx context.Context, to string, req *RequestVoteRequest) (*RequestVoteResponse, error)
	AppendEntries(ctx context.Context, to string, req *AppendEntriesRequest) (*AppendEntriesResponse, error)
	InstallSnapshot(ctx context.Context, to string, req *InstallSnapshotRequest) (*InstallSnapshotResponse, error)
}

type RequestVoteRequest struct {
	Term         uint64 `json:"term"`
	CandidateID  string `json:"candidate_id"`
	LastLogIndex uint64 `json:"last_log_index"`
	LastLogTerm  uint64 `json:"last_log_term"`
	// PreVote marks a pre-vote probe: the receiver answers as if voting in
	// Term but changes no state.
	PreVote bool `json:"pre_vote,omitempty"`
}

type RequestVoteResponse struct {
	Term        uint64 `json:"term"`
	VoteGranted bool   `json:"vote_granted"`
}

type AppendEntriesRequest struct {
	Term         uint64     `json:"term"`
	LeaderID     string     `json:"leader_id"`
	PrevLogIndex uint64     `json:"prev_log_index"`
	PrevLogTerm  uint64     `json:"prev_log_term"`
	Entries      []LogEntry `json:"entries,omitempty"`
	LeaderCommit uint64     `json:"leader_commit"`
}

type AppendEntriesResponse struct {
	Term    uint64 `json:"term"`
	Success bool   `json:"success"`
	// On failure, for fast backtracking: ConflictTerm is the term of the
	// follower's entry at PrevLogIndex (0 if the follower's log is too short),
	// and ConflictIndex is the first index the follower has for ConflictTerm
	// (or its last index + 1 if the log is too short).
	ConflictTerm  uint64 `json:"conflict_term,omitempty"`
	ConflictIndex uint64 `json:"conflict_index,omitempty"`
}

// InstallSnapshotRequest sends a whole snapshot in one RPC (no chunking).
type InstallSnapshotRequest struct {
	Term     uint64   `json:"term"`
	LeaderID string   `json:"leader_id"`
	Snapshot Snapshot `json:"snapshot"`
}

type InstallSnapshotResponse struct {
	Term uint64 `json:"term"`
}

// Config configures a Node. Zero durations take the defaults noted.
type Config struct {
	ID    string   // this node's ID; must be in Peers
	Peers []string // IDs of every voting member, including ID

	ElectionTimeoutMin  time.Duration // default 150ms
	ElectionTimeoutMax  time.Duration // default 300ms
	HeartbeatInterval   time.Duration // default 50ms
	MaxEntriesPerAppend int           // default 256
	SnapshotThreshold   uint64        // applied entries before a snapshot; default 10000, 0 uses default
	Logger              *slog.Logger  // default slog.Default()
}

// Role is a node's current role.
type Role string

const (
	Follower  Role = "follower"
	Candidate Role = "candidate"
	Leader    Role = "leader"
)

// Status is a point-in-time view of a node, for metrics and /v1/cluster.
type Status struct {
	ID            string `json:"id"`
	Role          Role   `json:"role"`
	Term          uint64 `json:"term"`
	LeaderID      string `json:"leader_id"`
	CommitIndex   uint64 `json:"commit_index"`
	LastApplied   uint64 `json:"last_applied"`
	LastLogIndex  uint64 `json:"last_log_index"`
	SnapshotIndex uint64 `json:"snapshot_index"`
	LeaderChanges uint64 `json:"leader_changes"` // times this node observed a new leader
	Snapshots     uint64 `json:"snapshots"`      // snapshots taken or installed
}

var (
	// ErrNotLeader is returned by Propose and ReadIndex on a non-leader. Use
	// Status().LeaderID to find the leader.
	ErrNotLeader = errors.New("raft: not leader")
	// ErrLeadershipLost means the node stopped being leader before the
	// proposal or read could be confirmed; the outcome of a proposal is
	// unknown (it may still commit under the next leader).
	ErrLeadershipLost = errors.New("raft: leadership lost")
	// ErrStopped is returned after Stop.
	ErrStopped = errors.New("raft: node stopped")
)
