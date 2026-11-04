package raft

import (
	"errors"
	"fmt"
	"sync"
)

var (
	// ErrCompacted is returned by Storage reads of indexes that were discarded
	// by a snapshot.
	ErrCompacted = errors.New("raft: requested index is unavailable due to compaction")
	// ErrUnavailable is returned by Storage reads past the end of the log.
	ErrUnavailable = errors.New("raft: requested index is beyond the end of the log")
)

// MemStorage is an in-memory Storage. It is safe for concurrent use. Tests
// reuse one MemStorage across a simulated crash: stop the old Node, build a
// new Node over the same MemStorage.
type MemStorage struct {
	mu   sync.Mutex
	hs   HardState
	snap Snapshot
	ents []LogEntry // ents[0].Index == snap.LastIndex+1
}

// NewMemStorage returns an empty MemStorage.
func NewMemStorage() *MemStorage { return &MemStorage{} }

func (m *MemStorage) LoadHardState() (HardState, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.hs, nil
}

func (m *MemStorage) SaveHardState(hs HardState) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.hs = hs
	return nil
}

func (m *MemStorage) FirstIndex() (uint64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.snap.LastIndex + 1, nil
}

func (m *MemStorage) LastIndex() (uint64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.lastLocked(), nil
}

func (m *MemStorage) lastLocked() uint64 { return m.snap.LastIndex + uint64(len(m.ents)) }

func (m *MemStorage) Entries(lo, hi uint64) ([]LogEntry, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if lo > hi {
		return nil, fmt.Errorf("raft: invalid range [%d,%d)", lo, hi)
	}
	if lo <= m.snap.LastIndex {
		return nil, ErrCompacted
	}
	if hi > m.lastLocked()+1 {
		return nil, ErrUnavailable
	}
	out := make([]LogEntry, hi-lo)
	copy(out, m.ents[lo-m.snap.LastIndex-1:hi-m.snap.LastIndex-1])
	return out, nil
}

func (m *MemStorage) Term(index uint64) (uint64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if index == m.snap.LastIndex {
		return m.snap.LastTerm, nil
	}
	if index < m.snap.LastIndex {
		return 0, ErrCompacted
	}
	if index > m.lastLocked() {
		return 0, ErrUnavailable
	}
	return m.ents[index-m.snap.LastIndex-1].Term, nil
}

// checkAppendLocked validates that entries are contiguous and attach to the
// existing log without a gap.
func (m *MemStorage) checkAppendLocked(entries []LogEntry) error {
	if len(entries) == 0 {
		return nil
	}
	first := entries[0].Index
	if first <= m.snap.LastIndex {
		return fmt.Errorf("raft: append at index %d overlaps snapshot at %d", first, m.snap.LastIndex)
	}
	if first > m.lastLocked()+1 {
		return fmt.Errorf("raft: append at index %d leaves a gap after %d", first, m.lastLocked())
	}
	for i, e := range entries {
		if e.Index != first+uint64(i) {
			return fmt.Errorf("raft: non-contiguous append: entry %d has index %d, want %d", i, e.Index, first+uint64(i))
		}
	}
	return nil
}

func (m *MemStorage) Append(entries []LogEntry) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.checkAppendLocked(entries); err != nil {
		return err
	}
	if len(entries) == 0 {
		return nil
	}
	keep := entries[0].Index - m.snap.LastIndex - 1
	m.ents = append(m.ents[:keep:keep], entries...)
	return nil
}

func (m *MemStorage) LoadSnapshot() (Snapshot, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.snap, nil
}

// SaveSnapshot stores snap and discards the log up to snap.LastIndex. If the
// log holds an entry at snap.LastIndex with snap.LastTerm, the suffix after it
// is kept; otherwise the whole log is dropped. A snapshot that is not newer
// than the current one is ignored.
func (m *MemStorage) SaveSnapshot(snap Snapshot) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.applySnapshotLocked(snap)
	return nil
}

func (m *MemStorage) applySnapshotLocked(snap Snapshot) bool {
	if snap.LastIndex <= m.snap.LastIndex {
		return false
	}
	var rest []LogEntry
	if snap.LastIndex <= m.lastLocked() {
		pos := snap.LastIndex - m.snap.LastIndex - 1
		if m.ents[pos].Term == snap.LastTerm {
			rest = append([]LogEntry(nil), m.ents[pos+1:]...)
		}
	}
	m.snap = snap
	m.ents = rest
	return true
}

func (m *MemStorage) Close() error { return nil }

var _ Storage = (*MemStorage)(nil)
