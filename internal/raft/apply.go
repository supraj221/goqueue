package raft

// The apply goroutine is the only caller of StateMachine.Apply, Snapshot and
// Restore after NewNode, so those never run concurrently with each other.

func (n *Node) takeApply() []applyItem {
	n.aqMu.Lock()
	items := n.aq
	n.aq = nil
	n.aqMu.Unlock()
	return items
}

func (n *Node) applyLoop() {
	defer n.wg.Done()
	for {
		select {
		case <-n.stopC:
			return
		case <-n.aqSig:
		}
		for {
			items := n.takeApply()
			if len(items) == 0 {
				break
			}
			for _, it := range items {
				select {
				case <-n.stopC:
					return
				default:
				}
				if it.snap != nil {
					n.applySnapshot(it.snap)
				} else {
					n.applyEntries(it.entries)
				}
			}
			n.maybeSnapshot()
		}
	}
}

func (n *Node) applyEntries(entries []LogEntry) {
	for _, e := range entries {
		if e.Index <= n.lastApplied.Load() {
			continue // covered by a snapshot restored in the meantime
		}
		var res any
		if e.Type == EntryCommand {
			res = n.sm.Apply(e)
		}
		n.aLastTerm = e.Term
		n.lastApplied.Store(e.Index)
		if e.Type == EntryCommand {
			n.resolveWaiter(e, res)
		}
	}
	n.wakeApplied()
}

// resolveWaiter hands the Apply result to the Propose call waiting on this
// index. If the entry that got committed there is from a different term than
// the one that was proposed, the proposal was overwritten.
func (n *Node) resolveWaiter(e LogEntry, res any) {
	n.wmu.Lock()
	w := n.waiters[e.Index]
	delete(n.waiters, e.Index)
	n.wmu.Unlock()
	if w == nil {
		return
	}
	if w.term != e.Term {
		w.ch <- proposeResult{err: ErrLeadershipLost}
		return
	}
	w.ch <- proposeResult{res: res}
}

func (n *Node) applySnapshot(s *Snapshot) {
	if s.LastIndex <= n.lastApplied.Load() {
		return
	}
	if err := n.sm.Restore(s.Data); err != nil {
		n.fatal(err)
		return
	}
	n.aLastTerm = s.LastTerm
	n.aLastSnap = s.LastIndex
	n.lastApplied.Store(s.LastIndex)
	n.wakeApplied()
}

func (n *Node) wakeApplied() {
	la := n.lastApplied.Load()
	n.appliedMu.Lock()
	keep := n.appliedWait[:0]
	for _, w := range n.appliedWait {
		if w.index <= la {
			close(w.ch)
		} else {
			keep = append(keep, w)
		}
	}
	for i := len(keep); i < len(n.appliedWait); i++ {
		n.appliedWait[i] = nil
	}
	n.appliedWait = keep
	n.appliedMu.Unlock()
}

// maybeSnapshot takes a snapshot once SnapshotThreshold entries have been
// applied since the last one. It runs on the apply goroutine, so the state
// machine is quiescent. Saving the snapshot lets Storage drop the log prefix;
// the event loop is told so it can drop its in-memory copy too.
func (n *Node) maybeSnapshot() {
	la := n.lastApplied.Load()
	if la-n.aLastSnap < n.cfg.SnapshotThreshold {
		return
	}
	data, err := n.sm.Snapshot()
	if err != nil {
		n.lg.Warn("state machine snapshot failed", "err", err)
		return
	}
	if err := n.st.SaveSnapshot(Snapshot{LastIndex: la, LastTerm: n.aLastTerm, Data: data}); err != nil {
		n.lg.Warn("saving snapshot failed", "err", err)
		return
	}
	n.aLastSnap = la
	n.snapshots.Add(1)
	select {
	case n.snapC <- snapDone{index: la, term: n.aLastTerm}:
	case <-n.stopC:
	}
}
