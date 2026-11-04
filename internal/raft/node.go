package raft

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"log/slog"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// Concurrency model
//
// One goroutine (Node.run, the "event loop") owns every piece of mutable Raft
// state: term, vote, role, the in-memory log, nextIndex/matchIndex, commit
// index, pending reads. Everything else talks to it over channels:
//
//	ticker            -> elections, heartbeats, CheckQuorum
//	rpcC              <- Handle* methods (request + reply channel)
//	respC             <- goroutines that sent outbound RPCs
//	proposeC / readC  <- Propose / ReadIndex callers
//	snapC             <- the apply goroutine, after it compacted the log
//
// Outbound RPCs run in their own goroutines so the loop never waits on the
// network. A second goroutine (Node.applyLoop) applies committed entries to
// the StateMachine and takes snapshots, so a slow state machine cannot stall
// heartbeats.
//
// The only mutexes are for state shared with other goroutines:
//
//	wmu       the table of Propose waiters (loop registers, apply goroutine
//	          and loop resolve)
//	aqMu      the queue of work for the apply goroutine
//	appliedMu callers of waitApplied (ReadIndex)
//	statusMu  the published Status copy
//
// Storage is called from both the loop and the apply goroutine (SaveSnapshot),
// so a Storage must be safe for concurrent use. MemStorage and FileStorage are.

const (
	defaultElectionMin       = 150 * time.Millisecond
	defaultElectionMax       = 300 * time.Millisecond
	defaultHeartbeat         = 50 * time.Millisecond
	defaultMaxEntries        = 256
	defaultSnapshotThreshold = 10000
	maxProposalBatch         = 128
)

type rpcKind uint8

const (
	rpcVote rpcKind = iota
	rpcAppend
	rpcSnapshot
)

// rpcMsg is an inbound RPC handed to the event loop.
type rpcMsg struct {
	kind  rpcKind
	vote  *RequestVoteRequest
	app   *AppendEntriesRequest
	snap  *InstallSnapshotRequest
	reply chan rpcReply // buffered(1)
}

type rpcReply struct {
	vote *RequestVoteResponse
	app  *AppendEntriesResponse
	snap *InstallSnapshotResponse
	err  error
}

// respMsg is the result of an outbound RPC, delivered back to the event loop.
type respMsg struct {
	kind rpcKind
	from string
	term uint64 // our term when the request was sent
	err  error

	vote *RequestVoteResponse
	app  *AppendEntriesResponse
	snap *InstallSnapshotResponse

	preVote bool
	round   uint64

	prevIdx  uint64 // append: PrevLogIndex of the request
	nEntries int    // append: number of entries sent
	tag      uint64 // readEpoch when the request was sent
	snapIdx  uint64 // snapshot: LastIndex that was sent
}

type proposeReq struct {
	data  []byte
	reply chan proposeReply // buffered(1)
}

type proposeReply struct {
	w     *waiter
	index uint64
	err   error
}

type proposeResult struct {
	res any
	err error
}

// waiter is a Propose call waiting for its entry to be applied.
type waiter struct {
	term uint64
	ch   chan proposeResult // buffered(1)
}

type readReq struct {
	reply chan readReply // buffered(1)
}

type readReply struct {
	index uint64
	err   error
}

type pendingRead struct {
	epoch uint64
	reply chan readReply
}

type snapDone struct{ index, term uint64 }

// applyItem is work for the apply goroutine: committed entries to apply or a
// snapshot to restore, in log order.
type applyItem struct {
	entries []LogEntry
	snap    *Snapshot
}

type appliedWaiter struct {
	index uint64
	ch    chan struct{}
}

// Node is one member of a Raft cluster.
type Node struct {
	cfg    Config
	id     string
	others []string
	quorum int
	sm     StateMachine
	st     Storage
	tr     Transport
	lg     *slog.Logger

	rpcTimeout time.Duration
	tickEvery  time.Duration
	rng        *rand.Rand

	rpcC     chan *rpcMsg
	respC    chan *respMsg
	proposeC chan *proposeReq
	readC    chan *readReq
	snapC    chan snapDone
	lcC      chan bool

	ctx      context.Context
	cancel   context.CancelFunc
	stopC    chan struct{}
	stopOnce sync.Once
	wg       sync.WaitGroup
	started  atomic.Bool

	// ---- owned by the event loop ----
	term              uint64
	votedFor          string
	hsDirty           bool
	role              Role
	leaderID          string
	leaderChanges     uint64
	lastLeaderContact time.Time
	electionDeadline  time.Time

	log         []LogEntry // log[i].Index == snapIndex+1+i
	snapIndex   uint64
	snapTerm    uint64
	commitIndex uint64
	applyQueued uint64 // highest index handed to the apply goroutine

	preCandidate bool
	round        uint64
	preVotes     map[string]bool
	votes        map[string]bool

	nextIndex  map[string]uint64
	matchIndex map[string]uint64
	inflight   map[string]bool
	lastSent   map[string]time.Time
	lastAck    map[string]time.Time
	ackEpoch   map[string]uint64
	noopIndex  uint64

	readEpoch uint64
	reads     []*pendingRead

	// ---- shared ----
	leaderFlag atomic.Bool // mirrors role == Leader for fast-path rejection

	wmu     sync.Mutex
	waiters map[uint64]*waiter

	lastApplied atomic.Uint64
	appliedMu   sync.Mutex
	appliedWait []*appliedWaiter

	aqMu  sync.Mutex
	aq    []applyItem
	aqSig chan struct{}

	statusMu  sync.RWMutex
	status    Status
	snapshots atomic.Uint64

	// owned by the apply goroutine
	aLastSnap uint64 // index of the last snapshot taken or restored
	aLastTerm uint64 // term of the entry at lastApplied
}

// NewNode creates a node. It loads persisted state and the latest snapshot
// (calling sm.Restore) but does not start timers or send RPCs until Start.
func NewNode(cfg Config, sm StateMachine, st Storage, tr Transport) (*Node, error) {
	if cfg.ID == "" {
		return nil, errors.New("raft: Config.ID is empty")
	}
	found := false
	var others []string
	for _, p := range cfg.Peers {
		if p == cfg.ID {
			found = true
		} else {
			others = append(others, p)
		}
	}
	if !found {
		return nil, fmt.Errorf("raft: node %q is not in Peers %v", cfg.ID, cfg.Peers)
	}
	if sm == nil || st == nil || (tr == nil && len(others) > 0) {
		return nil, errors.New("raft: state machine, storage and transport are required")
	}
	if cfg.ElectionTimeoutMin <= 0 {
		cfg.ElectionTimeoutMin = defaultElectionMin
	}
	if cfg.ElectionTimeoutMax <= 0 {
		cfg.ElectionTimeoutMax = defaultElectionMax
	}
	if cfg.ElectionTimeoutMax <= cfg.ElectionTimeoutMin {
		cfg.ElectionTimeoutMax = cfg.ElectionTimeoutMin * 2
	}
	if cfg.HeartbeatInterval <= 0 {
		cfg.HeartbeatInterval = defaultHeartbeat
	}
	if cfg.MaxEntriesPerAppend <= 0 {
		cfg.MaxEntriesPerAppend = defaultMaxEntries
	}
	if cfg.SnapshotThreshold == 0 {
		cfg.SnapshotThreshold = defaultSnapshotThreshold
	}
	lg := cfg.Logger
	if lg == nil {
		lg = slog.Default()
	}

	h := fnv.New64a()
	h.Write([]byte(cfg.ID))
	n := &Node{
		cfg:        cfg,
		id:         cfg.ID,
		others:     others,
		quorum:     len(cfg.Peers)/2 + 1,
		sm:         sm,
		st:         st,
		tr:         tr,
		lg:         lg.With("raft_id", cfg.ID),
		rpcTimeout: max(cfg.ElectionTimeoutMin, 50*time.Millisecond),
		tickEvery:  max(cfg.HeartbeatInterval/4, time.Millisecond),
		rng:        rand.New(rand.NewSource(time.Now().UnixNano() ^ int64(h.Sum64()))),
		rpcC:       make(chan *rpcMsg),
		respC:      make(chan *respMsg, 256),
		proposeC:   make(chan *proposeReq, 256),
		readC:      make(chan *readReq, 64),
		snapC:      make(chan snapDone, 8),
		lcC:        make(chan bool, 16),
		stopC:      make(chan struct{}),
		aqSig:      make(chan struct{}, 1),
		waiters:    make(map[uint64]*waiter),
		role:       Follower,
		nextIndex:  map[string]uint64{},
		matchIndex: map[string]uint64{},
		inflight:   map[string]bool{},
		lastSent:   map[string]time.Time{},
		lastAck:    map[string]time.Time{},
		ackEpoch:   map[string]uint64{},
	}
	n.ctx, n.cancel = context.WithCancel(context.Background())

	hs, err := st.LoadHardState()
	if err != nil {
		return nil, fmt.Errorf("raft: load hard state: %w", err)
	}
	n.term, n.votedFor = hs.Term, hs.VotedFor

	snap, err := st.LoadSnapshot()
	if err != nil {
		return nil, fmt.Errorf("raft: load snapshot: %w", err)
	}
	if snap.LastIndex > 0 {
		if err := sm.Restore(snap.Data); err != nil {
			return nil, fmt.Errorf("raft: restore snapshot: %w", err)
		}
	}
	n.snapIndex, n.snapTerm = snap.LastIndex, snap.LastTerm
	n.commitIndex, n.applyQueued = snap.LastIndex, snap.LastIndex
	n.lastApplied.Store(snap.LastIndex)
	n.aLastSnap, n.aLastTerm = snap.LastIndex, snap.LastTerm

	first, err := st.FirstIndex()
	if err != nil {
		return nil, err
	}
	last, err := st.LastIndex()
	if err != nil {
		return nil, err
	}
	if first != snap.LastIndex+1 {
		return nil, fmt.Errorf("raft: storage first index %d does not follow snapshot index %d", first, snap.LastIndex)
	}
	if last >= first {
		if n.log, err = st.Entries(first, last+1); err != nil {
			return nil, fmt.Errorf("raft: load log: %w", err)
		}
	}
	n.publishStatus()
	return n, nil
}

// Start begins the node's event loop.
func (n *Node) Start() error {
	select {
	case <-n.stopC:
		return ErrStopped
	default:
	}
	if !n.started.CompareAndSwap(false, true) {
		return errors.New("raft: node already started")
	}
	n.wg.Add(2)
	go n.run()
	go n.applyLoop()
	return nil
}

// Stop halts the node. Pending Propose and ReadIndex calls return ErrStopped.
// It does not close the Storage.
func (n *Node) Stop() {
	n.signalStop()
	n.wg.Wait()
}

func (n *Node) signalStop() {
	n.stopOnce.Do(func() {
		close(n.stopC)
		n.cancel()
	})
}

// fatal stops the node after an unrecoverable error (storage failure).
func (n *Node) fatal(err error) {
	n.lg.Error("raft: fatal error, stopping node", "err", err)
	n.signalStop()
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

// Propose replicates data as an EntryCommand and waits until it is committed
// and applied on this node. It returns StateMachine.Apply's result and the
// entry's log index. On a non-leader it returns ErrNotLeader immediately.
// The caller must not modify data after the call.
func (n *Node) Propose(ctx context.Context, data []byte) (result any, index uint64, err error) {
	if err := ctx.Err(); err != nil {
		return nil, 0, err
	}
	select {
	case <-n.stopC:
		return nil, 0, ErrStopped
	default:
	}
	if !n.leaderFlag.Load() {
		return nil, 0, ErrNotLeader
	}
	req := &proposeReq{data: data, reply: make(chan proposeReply, 1)}
	select {
	case n.proposeC <- req:
	case <-n.stopC:
		return nil, 0, ErrStopped
	case <-ctx.Done():
		return nil, 0, ctx.Err()
	}
	var rep proposeReply
	select {
	case rep = <-req.reply:
	case <-n.stopC:
		return nil, 0, ErrStopped
	case <-ctx.Done():
		return nil, 0, ctx.Err()
	}
	if rep.err != nil {
		return nil, 0, rep.err
	}
	select {
	case r := <-rep.w.ch:
		if r.err != nil {
			return nil, rep.index, r.err
		}
		return r.res, rep.index, nil
	case <-n.stopC:
		return nil, rep.index, ErrStopped
	case <-ctx.Done():
		return nil, rep.index, ctx.Err()
	}
}

// ReadIndex blocks until it is safe to serve a linearizable read from this
// node's state machine: the node confirmed it is still leader with a majority
// and has applied everything committed at the time of the call.
func (n *Node) ReadIndex(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case <-n.stopC:
		return ErrStopped
	default:
	}
	if !n.leaderFlag.Load() {
		return ErrNotLeader
	}
	req := &readReq{reply: make(chan readReply, 1)}
	select {
	case n.readC <- req:
	case <-n.stopC:
		return ErrStopped
	case <-ctx.Done():
		return ctx.Err()
	}
	var rep readReply
	select {
	case rep = <-req.reply:
	case <-n.stopC:
		return ErrStopped
	case <-ctx.Done():
		return ctx.Err()
	}
	if rep.err != nil {
		return rep.err
	}
	return n.waitApplied(ctx, rep.index)
}

// waitApplied blocks until lastApplied >= index.
func (n *Node) waitApplied(ctx context.Context, index uint64) error {
	if n.lastApplied.Load() >= index {
		return nil
	}
	w := &appliedWaiter{index: index, ch: make(chan struct{})}
	n.appliedMu.Lock()
	if n.lastApplied.Load() >= index {
		n.appliedMu.Unlock()
		return nil
	}
	n.appliedWait = append(n.appliedWait, w)
	n.appliedMu.Unlock()
	select {
	case <-w.ch:
		return nil
	case <-n.stopC:
		return ErrStopped
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Status returns a snapshot of the node's state. It is safe to call from any
// goroutine.
func (n *Node) Status() Status {
	n.statusMu.RLock()
	s := n.status
	n.statusMu.RUnlock()
	s.LastApplied = n.lastApplied.Load()
	s.Snapshots = n.snapshots.Load()
	return s
}

// LeaderChanges returns a channel that receives true when this node becomes
// leader and false when it stops being leader. The channel is buffered;
// slow readers may miss intermediate values but always see the latest one
// eventually. Every call returns the same channel.
func (n *Node) LeaderChanges() <-chan bool { return n.lcC }

// HandleRequestVote handles an inbound RequestVote RPC.
func (n *Node) HandleRequestVote(ctx context.Context, req *RequestVoteRequest) (*RequestVoteResponse, error) {
	r, err := n.call(ctx, &rpcMsg{kind: rpcVote, vote: req})
	return r.vote, err
}

// HandleAppendEntries handles an inbound AppendEntries RPC.
func (n *Node) HandleAppendEntries(ctx context.Context, req *AppendEntriesRequest) (*AppendEntriesResponse, error) {
	r, err := n.call(ctx, &rpcMsg{kind: rpcAppend, app: req})
	return r.app, err
}

// HandleInstallSnapshot handles an inbound InstallSnapshot RPC.
func (n *Node) HandleInstallSnapshot(ctx context.Context, req *InstallSnapshotRequest) (*InstallSnapshotResponse, error) {
	r, err := n.call(ctx, &rpcMsg{kind: rpcSnapshot, snap: req})
	return r.snap, err
}

func (n *Node) call(ctx context.Context, m *rpcMsg) (rpcReply, error) {
	if !n.started.Load() {
		return rpcReply{}, errors.New("raft: node not started")
	}
	m.reply = make(chan rpcReply, 1)
	select {
	case n.rpcC <- m:
	case <-n.stopC:
		return rpcReply{}, ErrStopped
	case <-ctx.Done():
		return rpcReply{}, ctx.Err()
	}
	select {
	case r := <-m.reply:
		return r, r.err
	case <-n.stopC:
		return rpcReply{}, ErrStopped
	case <-ctx.Done():
		return rpcReply{}, ctx.Err()
	}
}

// ---------------------------------------------------------------------------
// Event loop
// ---------------------------------------------------------------------------

func (n *Node) run() {
	defer n.wg.Done()
	defer n.shutdown()
	ticker := time.NewTicker(n.tickEvery)
	defer ticker.Stop()
	n.resetElectionTimer()
	for {
		select {
		case <-n.stopC:
			return
		case <-ticker.C:
			n.tick()
		case m := <-n.rpcC:
			n.handleRPC(m)
		case r := <-n.respC:
			n.handleResp(r)
		case p := <-n.proposeC:
			n.handleProposals(p)
		case r := <-n.readC:
			n.handleRead(r)
		case s := <-n.snapC:
			n.handleSnapDone(s)
		}
		if err := n.flushHS(); err != nil {
			return
		}
		n.publishStatus()
	}
}

// shutdown fails everything still waiting. It runs on the loop goroutine as it
// exits.
func (n *Node) shutdown() {
	n.signalStop()
	n.leaderFlag.Store(false)
	n.wmu.Lock()
	for idx, w := range n.waiters {
		w.ch <- proposeResult{err: ErrStopped}
		delete(n.waiters, idx)
	}
	n.wmu.Unlock()
	for _, r := range n.reads {
		r.reply <- readReply{err: ErrStopped}
	}
	n.reads = nil
}

func (n *Node) publishStatus() {
	s := Status{
		ID:            n.id,
		Role:          n.role,
		Term:          n.term,
		LeaderID:      n.leaderID,
		CommitIndex:   n.commitIndex,
		LastLogIndex:  n.lastIndex(),
		SnapshotIndex: n.snapIndex,
		LeaderChanges: n.leaderChanges,
	}
	n.statusMu.Lock()
	n.status = s
	n.statusMu.Unlock()
}

// flushHS persists the hard state if it changed. Every code path that is about
// to reply to an RPC or send a vote calls it first.
func (n *Node) flushHS() error {
	if !n.hsDirty {
		return nil
	}
	if err := n.st.SaveHardState(HardState{Term: n.term, VotedFor: n.votedFor}); err != nil {
		n.fatal(fmt.Errorf("save hard state: %w", err))
		return err
	}
	n.hsDirty = false
	return nil
}

func (n *Node) handleRPC(m *rpcMsg) {
	var rep rpcReply
	switch m.kind {
	case rpcVote:
		rep.vote, rep.err = n.onRequestVote(m.vote)
	case rpcAppend:
		rep.app, rep.err = n.onAppendEntries(m.app)
	case rpcSnapshot:
		rep.snap, rep.err = n.onInstallSnapshot(m.snap)
	}
	m.reply <- rep
}

func (n *Node) tick() {
	now := time.Now()
	if n.role == Leader {
		n.checkQuorum(now)
		if n.role != Leader {
			return
		}
		for _, p := range n.others {
			if n.inflight[p] {
				continue
			}
			if now.Sub(n.lastSent[p]) >= n.cfg.HeartbeatInterval || n.nextIndex[p] <= n.lastIndex() {
				n.sendAppend(p)
			}
		}
		return
	}
	if !now.Before(n.electionDeadline) {
		n.startPreVote()
	}
}

// checkQuorum makes a leader that has not heard from a majority within an
// election timeout step down. This is what lets a partitioned leader notice it
// is deposed (and fail its ReadIndex calls) without receiving a higher term.
func (n *Node) checkQuorum(now time.Time) {
	active := 1
	for _, p := range n.others {
		if now.Sub(n.lastAck[p]) < n.cfg.ElectionTimeoutMax {
			active++
		}
	}
	if active < n.quorum {
		n.lg.Info("leader lost contact with a quorum, stepping down", "term", n.term)
		n.becomeFollower(n.term, "")
	}
}

// ---------------------------------------------------------------------------
// Log helpers (loop only)
// ---------------------------------------------------------------------------

func (n *Node) lastIndex() uint64 { return n.snapIndex + uint64(len(n.log)) }

func (n *Node) lastTerm() uint64 {
	t, _ := n.termAt(n.lastIndex())
	return t
}

// termAt returns the term of the entry at i. Index snapIndex answers with the
// snapshot's term; anything older than that or past the end is unknown.
func (n *Node) termAt(i uint64) (uint64, bool) {
	if i == n.snapIndex {
		return n.snapTerm, true
	}
	if i < n.snapIndex || i > n.lastIndex() {
		return 0, false
	}
	return n.log[i-n.snapIndex-1].Term, true
}

func (n *Node) logUpToDate(lastIdx, lastTerm uint64) bool {
	myTerm := n.lastTerm()
	if lastTerm != myTerm {
		return lastTerm > myTerm
	}
	return lastIdx >= n.lastIndex()
}

func (n *Node) resetElectionTimer() {
	span := n.cfg.ElectionTimeoutMax - n.cfg.ElectionTimeoutMin
	d := n.cfg.ElectionTimeoutMin + time.Duration(n.rng.Int63n(int64(span)))
	n.electionDeadline = time.Now().Add(d)
}

// hasLeaderContact reports whether this node recently heard from a legitimate
// leader (or is one). Used for leader stickiness: such a node refuses to help
// elect anybody else.
func (n *Node) hasLeaderContact(now time.Time) bool {
	if n.role == Leader {
		return true
	}
	return !n.lastLeaderContact.IsZero() && now.Sub(n.lastLeaderContact) < n.cfg.ElectionTimeoutMin
}

func (n *Node) setLeader(id string) {
	if id != n.leaderID {
		if id != "" {
			n.leaderChanges++
		}
		n.leaderID = id
	}
}

func (n *Node) notifyLeader(isLeader bool) {
	select {
	case n.lcC <- isLeader:
		return
	default:
	}
	// Full: drop the oldest value. Only this goroutine sends, so the second
	// attempt always has room.
	select {
	case <-n.lcC:
	default:
	}
	select {
	case n.lcC <- isLeader:
	default:
	}
}

// ---------------------------------------------------------------------------
// Role transitions
// ---------------------------------------------------------------------------

func (n *Node) becomeFollower(term uint64, leader string) {
	wasLeader := n.role == Leader
	if term > n.term {
		n.term = term
		n.votedFor = ""
		n.hsDirty = true
	}
	n.role = Follower
	n.preCandidate = false
	n.round++
	n.setLeader(leader)
	if wasLeader {
		n.lg.Info("stepped down", "term", n.term, "new_leader", leader)
		n.leaderFlag.Store(false)
		n.failLeaderState()
		n.notifyLeader(false)
	}
	n.resetElectionTimer()
}

// failLeaderState resolves everything a leader was holding. Proposals at or
// below commitIndex are certain to commit and are resolved by the apply
// goroutine; the rest have an unknown fate.
func (n *Node) failLeaderState() {
	n.wmu.Lock()
	for idx, w := range n.waiters {
		if idx > n.commitIndex {
			w.ch <- proposeResult{err: ErrLeadershipLost}
			delete(n.waiters, idx)
		}
	}
	n.wmu.Unlock()
	for _, r := range n.reads {
		r.reply <- readReply{err: ErrLeadershipLost}
	}
	n.reads = nil
}

// startPreVote begins a pre-vote round at term+1 without changing our term.
func (n *Node) startPreVote() {
	n.resetElectionTimer()
	n.preCandidate = true
	n.round++
	n.preVotes = map[string]bool{n.id: true}
	if len(n.preVotes) >= n.quorum {
		n.startElection()
		return
	}
	req := RequestVoteRequest{
		Term:         n.term + 1,
		CandidateID:  n.id,
		LastLogIndex: n.lastIndex(),
		LastLogTerm:  n.lastTerm(),
		PreVote:      true,
	}
	for _, p := range n.others {
		go n.sendVote(p, req, n.round)
	}
}

// startElection increments the term, votes for itself and asks for votes.
func (n *Node) startElection() {
	n.preCandidate = false
	n.round++
	n.term++
	n.votedFor = n.id
	n.hsDirty = true
	n.role = Candidate
	n.setLeader("")
	n.votes = map[string]bool{n.id: true}
	n.resetElectionTimer()
	if err := n.flushHS(); err != nil {
		return
	}
	if len(n.votes) >= n.quorum {
		n.becomeLeader()
		return
	}
	req := RequestVoteRequest{
		Term:         n.term,
		CandidateID:  n.id,
		LastLogIndex: n.lastIndex(),
		LastLogTerm:  n.lastTerm(),
	}
	for _, p := range n.others {
		go n.sendVote(p, req, n.round)
	}
}

func (n *Node) becomeLeader() {
	n.role = Leader
	n.preCandidate = false
	n.setLeader(n.id)
	now := time.Now()
	for _, p := range n.others {
		n.nextIndex[p] = n.lastIndex() + 1
		n.matchIndex[p] = 0
		n.inflight[p] = false
		n.lastSent[p] = time.Time{}
		n.lastAck[p] = now
		n.ackEpoch[p] = 0
	}
	n.reads = nil
	n.lg.Info("became leader", "term", n.term, "last_index", n.lastIndex())
	// A new leader cannot commit entries from earlier terms directly (Figure 8),
	// so it appends a no-op in its own term; committing it commits everything
	// before it and lets ReadIndex know the leader's commit index is current.
	noop := LogEntry{Index: n.lastIndex() + 1, Term: n.term, Type: EntryNoop}
	if err := n.appendLocal([]LogEntry{noop}); err != nil {
		n.fatal(err)
		return
	}
	n.noopIndex = noop.Index
	n.leaderFlag.Store(true)
	n.notifyLeader(true)
	n.advanceCommit()
	n.broadcastAppend()
}

// appendLocal durably appends entries to the leader's own log.
func (n *Node) appendLocal(entries []LogEntry) error {
	if err := n.st.Append(entries); err != nil {
		return fmt.Errorf("append: %w", err)
	}
	n.log = append(n.log, entries...)
	return nil
}

// ---------------------------------------------------------------------------
// Inbound RPCs
// ---------------------------------------------------------------------------

func (n *Node) onRequestVote(req *RequestVoteRequest) (*RequestVoteResponse, error) {
	now := time.Now()
	if req.PreVote {
		// A pre-vote changes no state at all. Grant only if we would plausibly
		// vote for this candidate in a real election at req.Term, and we have
		// not heard from a live leader recently.
		grant := req.Term > n.term &&
			!n.hasLeaderContact(now) &&
			n.logUpToDate(req.LastLogIndex, req.LastLogTerm)
		if grant {
			return &RequestVoteResponse{Term: req.Term, VoteGranted: true}, nil
		}
		return &RequestVoteResponse{Term: n.term}, nil
	}
	if req.Term < n.term {
		return &RequestVoteResponse{Term: n.term}, nil
	}
	if req.Term > n.term {
		if n.hasLeaderContact(now) {
			// Leader stickiness: ignore disruptive candidates entirely.
			return &RequestVoteResponse{Term: n.term}, nil
		}
		n.becomeFollower(req.Term, "")
	}
	grant := (n.votedFor == "" || n.votedFor == req.CandidateID) &&
		n.logUpToDate(req.LastLogIndex, req.LastLogTerm)
	if grant {
		n.votedFor = req.CandidateID
		n.hsDirty = true
		n.resetElectionTimer()
	}
	if err := n.flushHS(); err != nil {
		return nil, err
	}
	return &RequestVoteResponse{Term: n.term, VoteGranted: grant}, nil
}

func (n *Node) onAppendEntries(req *AppendEntriesRequest) (*AppendEntriesResponse, error) {
	if req.Term < n.term {
		return &AppendEntriesResponse{Term: n.term}, nil
	}
	n.becomeFollower(req.Term, req.LeaderID)
	n.lastLeaderContact = time.Now()
	if err := n.flushHS(); err != nil {
		return nil, err
	}
	resp := &AppendEntriesResponse{Term: n.term}

	prevIdx := req.PrevLogIndex
	entries := req.Entries
	if prevIdx > n.lastIndex() {
		// Our log is too short.
		resp.ConflictIndex = n.lastIndex() + 1
		return resp, nil
	}
	if prevIdx < n.snapIndex {
		// Everything up to our snapshot is committed here and so matches the
		// leader's log; skip the part of the request we already hold.
		skip := n.snapIndex - prevIdx
		if skip >= uint64(len(entries)) {
			entries = nil
		} else {
			entries = entries[skip:]
		}
		prevIdx = n.snapIndex
	} else if t, _ := n.termAt(prevIdx); t != req.PrevLogTerm {
		// Consistency check failed. Tell the leader the term of the conflicting
		// entry and where that term starts in our log.
		resp.ConflictTerm = t
		ci := prevIdx
		for ci > n.snapIndex {
			if tt, _ := n.termAt(ci - 1); tt != t {
				break
			}
			ci--
		}
		resp.ConflictIndex = ci
		return resp, nil
	}

	for k, e := range entries {
		if e.Index != prevIdx+1+uint64(k) {
			return nil, fmt.Errorf("raft: malformed AppendEntries: entry %d has index %d", k, e.Index)
		}
	}

	// Find the first entry that is new or conflicts; keep everything that
	// already matches (a delayed duplicate must not truncate a longer log).
	var toAppend []LogEntry
	for k, e := range entries {
		if e.Index <= n.lastIndex() {
			if t, _ := n.termAt(e.Index); t == e.Term {
				continue
			}
			if e.Index <= n.commitIndex {
				n.fatal(fmt.Errorf("leader asks to overwrite committed entry %d", e.Index))
				return nil, ErrStopped
			}
		}
		toAppend = entries[k:]
		break
	}
	if len(toAppend) > 0 {
		// Storage.Append replaces any overlapping suffix and is durable.
		if err := n.st.Append(toAppend); err != nil {
			n.fatal(fmt.Errorf("append: %w", err))
			return nil, err
		}
		n.log = append(n.log[:toAppend[0].Index-n.snapIndex-1], toAppend...)
	}

	lastNew := prevIdx + uint64(len(entries))
	if req.LeaderCommit > n.commitIndex {
		if nc := min(req.LeaderCommit, lastNew); nc > n.commitIndex {
			n.commitIndex = nc
			n.queueApply()
		}
	}
	resp.Success = true
	return resp, nil
}

func (n *Node) onInstallSnapshot(req *InstallSnapshotRequest) (*InstallSnapshotResponse, error) {
	if req.Term < n.term {
		return &InstallSnapshotResponse{Term: n.term}, nil
	}
	n.becomeFollower(req.Term, req.LeaderID)
	n.lastLeaderContact = time.Now()
	if err := n.flushHS(); err != nil {
		return nil, err
	}
	resp := &InstallSnapshotResponse{Term: n.term}
	s := req.Snapshot
	if s.LastIndex <= n.commitIndex {
		return resp, nil // we already have all of this committed
	}
	if err := n.st.SaveSnapshot(s); err != nil {
		n.fatal(fmt.Errorf("save snapshot: %w", err))
		return nil, err
	}
	// Keep the log suffix if it agrees with the snapshot, else discard it all.
	if t, ok := n.termAt(s.LastIndex); ok && t == s.LastTerm {
		n.log = append([]LogEntry(nil), n.log[s.LastIndex-n.snapIndex:]...)
	} else {
		n.log = nil
	}
	n.snapIndex, n.snapTerm = s.LastIndex, s.LastTerm
	n.commitIndex = s.LastIndex
	n.applyQueued = s.LastIndex
	n.snapshots.Add(1)
	snap := s
	n.pushApply(applyItem{snap: &snap})
	n.lg.Info("installed snapshot", "last_index", s.LastIndex, "term", s.LastTerm)
	return resp, nil
}

// ---------------------------------------------------------------------------
// Outbound RPC results
// ---------------------------------------------------------------------------

func (n *Node) handleResp(r *respMsg) {
	switch r.kind {
	case rpcVote:
		n.onVoteResp(r)
	case rpcAppend:
		n.onAppendResp(r)
	case rpcSnapshot:
		n.onSnapshotResp(r)
	}
}

func (n *Node) onVoteResp(r *respMsg) {
	if r.err != nil || r.vote == nil {
		return
	}
	if r.preVote {
		if !n.preCandidate || r.round != n.round {
			return
		}
		if r.vote.VoteGranted {
			n.preVotes[r.from] = true
			if len(n.preVotes) >= n.quorum {
				n.startElection()
			}
		} else if r.vote.Term > n.term {
			n.becomeFollower(r.vote.Term, "")
		}
		return
	}
	if r.vote.Term > n.term {
		n.becomeFollower(r.vote.Term, "")
		return
	}
	if n.role != Candidate || n.preCandidate || r.term != n.term {
		return
	}
	if r.vote.VoteGranted {
		n.votes[r.from] = true
		if len(n.votes) >= n.quorum {
			n.becomeLeader()
		}
	}
}

func (n *Node) onAppendResp(r *respMsg) {
	if r.err == nil && r.app != nil && r.app.Term > n.term {
		n.becomeFollower(r.app.Term, "")
		return
	}
	if n.role != Leader || r.term != n.term {
		return
	}
	n.inflight[r.from] = false
	if r.err != nil || r.app == nil {
		return // retried on a later tick
	}
	p := r.from
	n.lastAck[p] = time.Now()
	n.ackEpoch[p] = max(n.ackEpoch[p], r.tag)
	resp := r.app
	if resp.Success {
		match := r.prevIdx + uint64(r.nEntries)
		if match > n.matchIndex[p] {
			n.matchIndex[p] = match
		}
		n.nextIndex[p] = max(n.nextIndex[p], n.matchIndex[p]+1)
		n.advanceCommit()
	} else {
		n.nextIndex[p] = n.backtrack(resp, r.prevIdx)
	}
	n.checkReads()
	if n.role != Leader {
		return
	}
	if n.nextIndex[p] <= n.lastIndex() || (len(n.reads) > 0 && n.ackEpoch[p] < n.readEpoch) {
		n.sendAppend(p)
	}
}

// backtrack computes the follower's next index from a rejection using the
// ConflictTerm / ConflictIndex hints, skipping a whole term at a time.
func (n *Node) backtrack(resp *AppendEntriesResponse, prevIdx uint64) uint64 {
	next := resp.ConflictIndex
	if resp.ConflictTerm != 0 {
		// If we also have entries of the conflicting term, both logs agree up to
		// our last entry of that term; resume right after it.
		for i := min(prevIdx, n.lastIndex()); i > n.snapIndex; i-- {
			t := n.log[i-n.snapIndex-1].Term
			if t == resp.ConflictTerm {
				next = i + 1
				break
			}
			if t < resp.ConflictTerm {
				break
			}
		}
	}
	if next < 1 {
		next = 1
	}
	if next > prevIdx { // always make progress
		next = prevIdx
	}
	if next < 1 {
		next = 1
	}
	return next
}

func (n *Node) onSnapshotResp(r *respMsg) {
	if r.err == nil && r.snap != nil && r.snap.Term > n.term {
		n.becomeFollower(r.snap.Term, "")
		return
	}
	if n.role != Leader || r.term != n.term {
		return
	}
	n.inflight[r.from] = false
	if r.err != nil || r.snap == nil {
		return
	}
	p := r.from
	n.lastAck[p] = time.Now()
	n.ackEpoch[p] = max(n.ackEpoch[p], r.tag)
	if r.snapIdx > n.matchIndex[p] {
		n.matchIndex[p] = r.snapIdx
	}
	n.nextIndex[p] = max(n.nextIndex[p], n.matchIndex[p]+1)
	n.advanceCommit()
	n.checkReads()
	if n.role == Leader && n.nextIndex[p] <= n.lastIndex() {
		n.sendAppend(p)
	}
}

// ---------------------------------------------------------------------------
// Leader replication
// ---------------------------------------------------------------------------

func (n *Node) broadcastAppend() {
	for _, p := range n.others {
		if !n.inflight[p] {
			n.sendAppend(p)
		}
	}
}

func (n *Node) sendAppend(p string) {
	next := n.nextIndex[p]
	if next <= n.snapIndex {
		n.sendSnapshot(p)
		return
	}
	prevIdx := next - 1
	prevTerm, _ := n.termAt(prevIdx)
	var entries []LogEntry
	if last := n.lastIndex(); next <= last {
		hi := min(last, next+uint64(n.cfg.MaxEntriesPerAppend)-1)
		// Copy: the backing array of n.log is rewritten by later truncations.
		entries = make([]LogEntry, hi-next+1)
		copy(entries, n.log[next-n.snapIndex-1:hi-n.snapIndex])
	}
	req := &AppendEntriesRequest{
		Term:         n.term,
		LeaderID:     n.id,
		PrevLogIndex: prevIdx,
		PrevLogTerm:  prevTerm,
		Entries:      entries,
		LeaderCommit: n.commitIndex,
	}
	n.inflight[p] = true
	n.lastSent[p] = time.Now()
	go n.callAppend(p, req, n.readEpoch)
}

func (n *Node) callAppend(p string, req *AppendEntriesRequest, tag uint64) {
	ctx, cancel := context.WithTimeout(n.ctx, n.rpcTimeout)
	resp, err := n.tr.AppendEntries(ctx, p, req)
	cancel()
	n.post(&respMsg{
		kind: rpcAppend, from: p, term: req.Term, err: err, app: resp,
		prevIdx: req.PrevLogIndex, nEntries: len(req.Entries), tag: tag,
	})
}

func (n *Node) sendSnapshot(p string) {
	n.inflight[p] = true
	n.lastSent[p] = time.Now()
	term, tag := n.term, n.readEpoch
	go func() {
		snap, err := n.st.LoadSnapshot()
		if err == nil && snap.LastIndex == 0 {
			err = errors.New("no snapshot available")
		}
		if err != nil {
			n.post(&respMsg{kind: rpcSnapshot, from: p, term: term, err: err})
			return
		}
		ctx, cancel := context.WithTimeout(n.ctx, 10*n.rpcTimeout)
		resp, err := n.tr.InstallSnapshot(ctx, p, &InstallSnapshotRequest{Term: term, LeaderID: n.id, Snapshot: snap})
		cancel()
		n.post(&respMsg{kind: rpcSnapshot, from: p, term: term, err: err, snap: resp, tag: tag, snapIdx: snap.LastIndex})
	}()
}

func (n *Node) sendVote(p string, req RequestVoteRequest, round uint64) {
	ctx, cancel := context.WithTimeout(n.ctx, n.rpcTimeout)
	resp, err := n.tr.RequestVote(ctx, p, &req)
	cancel()
	n.post(&respMsg{kind: rpcVote, from: p, term: req.Term, err: err, vote: resp, preVote: req.PreVote, round: round})
}

func (n *Node) post(r *respMsg) {
	select {
	case n.respC <- r:
	case <-n.stopC:
	}
}

// advanceCommit moves commitIndex to the highest index stored on a majority,
// but only if that entry is from the current term (Raft paper, Figure 8):
// older-term entries become committed indirectly when a newer one does.
func (n *Node) advanceCommit() {
	if n.role != Leader {
		return
	}
	match := make([]uint64, 0, len(n.others)+1)
	match = append(match, n.lastIndex())
	for _, p := range n.others {
		match = append(match, n.matchIndex[p])
	}
	sort.Slice(match, func(i, j int) bool { return match[i] > match[j] })
	q := match[n.quorum-1]
	if q <= n.commitIndex {
		return
	}
	if t, ok := n.termAt(q); !ok || t != n.term {
		return
	}
	n.commitIndex = q
	n.queueApply()
	n.checkReads()
}

// ---------------------------------------------------------------------------
// Proposals and reads
// ---------------------------------------------------------------------------

func (n *Node) handleProposals(first *proposeReq) {
	batch := []*proposeReq{first}
drain:
	for len(batch) < maxProposalBatch {
		select {
		case p := <-n.proposeC:
			batch = append(batch, p)
		default:
			break drain
		}
	}
	if n.role != Leader {
		for _, p := range batch {
			p.reply <- proposeReply{err: ErrNotLeader}
		}
		return
	}
	entries := make([]LogEntry, len(batch))
	idx := n.lastIndex()
	for i, p := range batch {
		idx++
		entries[i] = LogEntry{Index: idx, Term: n.term, Type: EntryCommand, Data: p.data}
	}
	if err := n.appendLocal(entries); err != nil {
		n.fatal(err)
		for _, p := range batch {
			p.reply <- proposeReply{err: ErrStopped}
		}
		return
	}
	waiters := make([]*waiter, len(batch))
	n.wmu.Lock()
	for i := range batch {
		waiters[i] = &waiter{term: n.term, ch: make(chan proposeResult, 1)}
		n.waiters[entries[i].Index] = waiters[i]
	}
	n.wmu.Unlock()
	for i, p := range batch {
		p.reply <- proposeReply{w: waiters[i], index: entries[i].Index}
	}
	n.advanceCommit()
	n.broadcastAppend()
}

func (n *Node) handleRead(r *readReq) {
	if n.role != Leader {
		r.reply <- readReply{err: ErrNotLeader}
		return
	}
	// ReadIndex (Raft thesis 6.4). Bump the epoch so only heartbeats sent from
	// now on count as proof that we are still leader, then ask for them.
	n.readEpoch++
	n.reads = append(n.reads, &pendingRead{epoch: n.readEpoch, reply: r.reply})
	n.broadcastAppend()
	n.checkReads()
}

// checkReads answers reads that have (a) a committed entry from the current
// term and (b) a majority that acknowledged a heartbeat sent after the read
// was requested. The read index is the commit index at that moment.
func (n *Node) checkReads() {
	if n.role != Leader || len(n.reads) == 0 || n.commitIndex < n.noopIndex {
		return
	}
	keep := n.reads[:0]
	for _, r := range n.reads {
		acks := 1
		for _, p := range n.others {
			if n.ackEpoch[p] >= r.epoch {
				acks++
			}
		}
		if acks >= n.quorum {
			r.reply <- readReply{index: n.commitIndex}
		} else {
			keep = append(keep, r)
		}
	}
	for i := len(keep); i < len(n.reads); i++ {
		n.reads[i] = nil
	}
	n.reads = keep
}

// ---------------------------------------------------------------------------
// Handoff to the apply goroutine
// ---------------------------------------------------------------------------

// queueApply hands newly committed entries to the apply goroutine.
func (n *Node) queueApply() {
	if n.commitIndex <= n.applyQueued {
		return
	}
	lo, hi := n.applyQueued+1, n.commitIndex
	ents := make([]LogEntry, hi-lo+1)
	copy(ents, n.log[lo-n.snapIndex-1:hi-n.snapIndex])
	n.applyQueued = hi
	n.pushApply(applyItem{entries: ents})
}

func (n *Node) pushApply(it applyItem) {
	n.aqMu.Lock()
	n.aq = append(n.aq, it)
	n.aqMu.Unlock()
	select {
	case n.aqSig <- struct{}{}:
	default:
	}
}

// handleSnapDone compacts the in-memory log after the apply goroutine saved a
// snapshot (Storage already dropped the prefix).
func (n *Node) handleSnapDone(s snapDone) {
	if s.index <= n.snapIndex || s.index > n.lastIndex() {
		return
	}
	n.log = append([]LogEntry(nil), n.log[s.index-n.snapIndex:]...)
	n.snapIndex, n.snapTerm = s.index, s.term
}
