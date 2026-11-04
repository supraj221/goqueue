package raft_test

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/supraj221/goqueue/internal/raft"
)

func TestInitialElection(t *testing.T) {
	c := newCluster(t, 3)
	l := c.checkOneLeader()
	term1 := c.status(l).Term
	if term1 < 1 {
		t.Fatalf("leader term is %d, want >= 1", term1)
	}
	time.Sleep(300 * time.Millisecond)
	l2 := c.checkOneLeader()
	if term2 := c.status(l2).Term; term2 != term1 || l2 != l {
		t.Fatalf("leadership changed without failures: node %d term %d -> node %d term %d", l, term1, l2, term2)
	}
	for i := range c.ids {
		if got := c.status(i).LeaderID; got != c.ids[l] {
			t.Errorf("node %d thinks leader is %q, want %q", i, got, c.ids[l])
		}
	}
}

func TestReElection(t *testing.T) {
	c := newCluster(t, 3)
	l1 := c.checkOneLeader()

	c.disconnect(l1)
	l2 := c.checkOneLeader()
	if l2 == l1 {
		t.Fatal("same leader after disconnect")
	}

	// The old leader rejoins; it must not displace the new one.
	c.connect(l1)
	if l3 := c.checkOneLeader(); l3 != l2 {
		t.Fatalf("old leader %d rejoining displaced leader %d (now %d)", l1, l2, l3)
	}
	if s := c.status(l1); s.Role != raft.Follower || s.LeaderID != c.ids[l2] {
		t.Fatalf("old leader should follow %s, status %+v", c.ids[l2], s)
	}

	// No quorum: no leader.
	c.disconnect(l2)
	c.disconnect((l2 + 1) % 3)
	time.Sleep(400 * time.Millisecond)
	c.checkNoLeader()

	// Quorum restored.
	c.connect((l2 + 1) % 3)
	c.checkOneLeader()
	c.connect(l2)
	c.checkOneLeader()
}

func TestNoLeaderWithoutQuorum(t *testing.T) {
	c := newCluster(t, 5)
	l := c.checkOneLeader()
	termBefore := c.status(l).Term

	// Cut off three nodes, leaving a minority of two (maybe including the leader).
	for _, i := range []int{(l + 1) % 5, (l + 2) % 5, (l + 3) % 5} {
		c.disconnect(i)
	}
	time.Sleep(600 * time.Millisecond) // many election timeouts
	c.checkNoLeader()
	// PreVote: the isolated minority must not have run its term up.
	for _, i := range []int{l, (l + 4) % 5} {
		if term := c.status(i).Term; term != termBefore {
			t.Errorf("minority node %d term %d, want unchanged %d", i, term, termBefore)
		}
	}

	c.connect((l + 1) % 5)
	c.checkOneLeader()
}

func TestBasicAgree(t *testing.T) {
	c := newCluster(t, 3)
	c.checkOneLeader()
	var prev uint64
	for i := 1; i <= 5; i++ {
		idx := c.one(fmt.Sprintf("cmd-%d", i), 3, false)
		if idx <= prev {
			t.Fatalf("index %d not greater than previous %d", idx, prev)
		}
		prev = idx
	}
}

func TestFailAgree(t *testing.T) {
	c := newCluster(t, 3)
	c.one("101", 3, false)
	l := c.checkOneLeader()
	follower := (l + 1) % 3
	c.disconnect(follower)

	i2 := c.one("102", 2, false)
	c.one("103", 2, false)
	time.Sleep(100 * time.Millisecond)
	c.one("104", 2, false)
	i5 := c.one("105", 2, false)

	c.connect(follower)
	c.one("106", 3, true)
	if !c.waitCommitted(i2, 3, "102", 2*time.Second) || !c.waitCommitted(i5, 3, "105", 2*time.Second) {
		t.Fatal("rejoined follower did not catch up")
	}
}

func TestFailNoAgree(t *testing.T) {
	c := newCluster(t, 5)
	c.one("10", 5, false)
	l := c.checkOneLeader()
	for i := 1; i <= 3; i++ {
		c.disconnect((l + i) % 5)
	}
	commitBefore := c.status(l).CommitIndex

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	_, _, err := c.node(l).Propose(ctx, []byte("20"))
	cancel()
	if err == nil {
		t.Fatal("Propose succeeded without a quorum")
	}
	if !isOneOf(err, context.DeadlineExceeded, raft.ErrLeadershipLost, raft.ErrNotLeader) {
		t.Fatalf("unexpected error %v", err)
	}
	if got := c.status(l).CommitIndex; got != commitBefore {
		t.Fatalf("commit index advanced from %d to %d without a quorum", commitBefore, got)
	}

	for i := 1; i <= 3; i++ {
		c.connect((l + i) % 5)
	}
	c.one("30", 5, true)
}

func TestConcurrentProposals(t *testing.T) {
	c := newCluster(t, 3)
	l := c.checkOneLeader()
	node := c.node(l)

	const n = 40
	var wg sync.WaitGroup
	idxs := make([]uint64, n)
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			cmd := fmt.Sprintf("v%d", i)
			res, idx, err := node.Propose(ctx, []byte(cmd))
			if err == nil && res != cmd {
				err = fmt.Errorf("result %v, want %q", res, cmd)
			}
			idxs[i], errs[i] = idx, err
		}(i)
	}
	wg.Wait()
	seen := map[uint64]bool{}
	for i := 0; i < n; i++ {
		if errs[i] != nil {
			t.Fatalf("proposal %d: %v", i, errs[i])
		}
		if seen[idxs[i]] {
			t.Fatalf("index %d assigned twice", idxs[i])
		}
		seen[idxs[i]] = true
	}
	for i := 0; i < n; i++ {
		if !c.waitCommitted(idxs[i], 3, fmt.Sprintf("v%d", i), 2*time.Second) {
			t.Fatalf("entry v%d at %d not applied on all nodes", i, idxs[i])
		}
	}
}

func TestRejoin(t *testing.T) {
	c := newCluster(t, 3)
	c.one("101", 3, true)

	l1 := c.checkOneLeader()
	c.disconnect(l1)

	// The old leader accepts entries it can never commit.
	base := c.status(l1).LastLogIndex
	for i := 0; i < 4; i++ {
		c.proposeAsync(l1, fmt.Sprintf("stale-%d", i), 150*time.Millisecond)
	}
	if !waitFor(time.Second, func() bool { return c.status(l1).LastLogIndex >= base+4 }) {
		t.Fatal("old leader did not append the uncommitted entries")
	}

	// The new leader commits others.
	c.one("102", 2, true)
	c.one("103", 2, true)

	// Swap who is reachable: old leader returns, new leader leaves.
	l2 := c.checkOneLeader()
	c.disconnect(l2)
	c.connect(l1)
	c.one("104", 2, true)

	c.connect(l2)
	c.one("105", 3, true)

	waitFor(2*time.Second, func() bool {
		for i := range c.ids {
			if len(dedupe(c.sm(i).values())) != 5 {
				return false
			}
		}
		return true
	})
	for i := range c.ids {
		got := dedupe(c.sm(i).values())
		want := []string{"101", "102", "103", "104", "105"}
		if fmt.Sprint(got) != fmt.Sprint(want) {
			t.Fatalf("node %d applied %v, want %v (stale entries must be discarded)", i, got, want)
		}
	}
}

func TestBackup(t *testing.T) {
	const nBig = 60
	c := newCluster(t, 5)
	c.one("first", 5, true)

	// Leader 1 and one follower get cut off from the other three, then leader 1
	// accepts many entries that will never commit.
	l1 := c.checkOneLeader()
	for _, d := range []int{2, 3, 4} {
		c.disconnect((l1 + d) % 5)
	}
	base := c.status(l1).LastLogIndex
	for i := 0; i < nBig; i++ {
		c.proposeAsync(l1, fmt.Sprintf("lost1-%d", i), 200*time.Millisecond)
	}
	if !waitFor(2*time.Second, func() bool { return c.status(l1).LastLogIndex >= base+nBig }) {
		t.Fatal("leader 1 did not append the divergent entries")
	}

	// The other three form a new majority and commit many entries.
	c.disconnect(l1)
	c.disconnect((l1 + 1) % 5)
	for _, d := range []int{2, 3, 4} {
		c.connect((l1 + d) % 5)
	}
	for i := 0; i < nBig; i++ {
		c.one(fmt.Sprintf("maj-%d", i), 3, true)
	}

	// Their leader then loses one more node and appends another divergent tail.
	l2 := c.checkOneLeader()
	other := (l1 + 2) % 5
	if other == l2 {
		other = (l1 + 3) % 5
	}
	c.disconnect(other)
	base2 := c.status(l2).LastLogIndex
	for i := 0; i < nBig; i++ {
		c.proposeAsync(l2, fmt.Sprintf("lost2-%d", i), 200*time.Millisecond)
	}
	if !waitFor(2*time.Second, func() bool { return c.status(l2).LastLogIndex >= base2+nBig }) {
		t.Fatal("leader 2 did not append the divergent entries")
	}

	// Now bring back the two stale-leader nodes plus "other" (up to date up to
	// the majority entries) and drop leader 2 and its peer.
	for i := range c.ids {
		c.disconnect(i)
	}
	for _, i := range []int{l1, (l1 + 1) % 5, other} {
		c.connect(i)
	}
	rejBefore := c.cnt.rejections.Load()
	for i := 0; i < nBig; i++ {
		c.one(fmt.Sprintf("final-%d", i), 3, true)
	}
	rejections := c.cnt.rejections.Load() - rejBefore

	for i := range c.ids {
		c.connect(i)
	}
	c.one("last", 5, true)

	// Fast backtracking skips a whole term per round trip. Without it, each of
	// the ~60 divergent entries costs one rejected AppendEntries per follower.
	t.Logf("rejected AppendEntries during catch-up: %d", rejections)
	if rejections > 30 {
		t.Fatalf("%d rejected AppendEntries while backing up %d divergent entries; fast backtracking is not working", rejections, nBig)
	}
}

func TestPersistenceRestartAll(t *testing.T) {
	c := newCluster(t, 3)
	i1 := c.one("11", 3, true)
	i2 := c.one("12", 3, true)

	c.crashAll()
	for i := range c.ids {
		c.restart(i)
	}
	c.checkOneLeader()
	i3 := c.one("13", 3, true)
	if i3 <= i2 {
		t.Fatalf("new entry index %d not after %d", i3, i2)
	}
	for idx, want := range map[uint64]string{i1: "11", i2: "12", i3: "13"} {
		if !c.waitCommitted(idx, 3, want, 3*time.Second) {
			t.Fatalf("entry %q at index %d missing after restart", want, idx)
		}
	}

	// Crash and restart each node in turn, including whoever leads.
	for round := 0; round < 3; round++ {
		l := c.checkOneLeader()
		c.crash(l)
		c.one(fmt.Sprintf("r%d", round), 2, true)
		c.restart(l)
		c.one(fmt.Sprintf("s%d", round), 3, true)
	}
	if !c.waitCommitted(i1, 3, "11", 3*time.Second) {
		t.Fatal("first entry lost")
	}
}

func TestFigure8Unreliable(t *testing.T) {
	c := newCluster(t, 5)
	c.one(fmt.Sprint(rand.Int()), 1, true)

	iterations := 150
	if testing.Short() {
		iterations = 50
	}
	c.net.SetDropRate(0.1)
	c.net.SetDelay(0, 8*time.Millisecond)

	nup := 5
	for it := 0; it < iterations; it++ {
		leader := -1
		for i := range c.ids {
			if c.isUp(i) && c.status(i).Role == raft.Leader {
				c.proposeAsync(i, fmt.Sprint(rand.Int()), 300*time.Millisecond)
				leader = i
			}
		}
		if rand.Intn(1000) < 100 {
			time.Sleep(time.Duration(rand.Intn(60)) * time.Millisecond)
		} else {
			time.Sleep(time.Duration(rand.Intn(13)) * time.Millisecond)
		}
		if leader != -1 && rand.Intn(3) == 0 {
			c.crash(leader)
			nup--
		}
		if nup < 3 {
			s := rand.Intn(5)
			if !c.isUp(s) {
				c.restart(s)
				nup++
			}
		}
	}

	c.net.SetDropRate(0)
	c.net.SetDelay(0, 0)
	for i := range c.ids {
		if !c.isUp(i) {
			c.restart(i)
		}
	}
	c.one(fmt.Sprint(rand.Int()), 5, true)
}

func TestSnapshotInstall(t *testing.T) {
	c := newClusterSnap(t, 3, 10)
	c.one("start", 3, true)
	l := c.checkOneLeader()
	lag := (l + 1) % 3
	c.disconnect(lag)

	var last uint64
	for i := 0; i < 45; i++ {
		last = c.one(fmt.Sprintf("e%d", i), 2, true)
	}
	if !waitFor(2*time.Second, func() bool { return c.status(c.checkOneLeader()).SnapshotIndex > 0 }) {
		t.Fatal("leader never compacted its log")
	}

	c.connect(lag)
	if !c.waitCommitted(last, 3, "e44", 5*time.Second) {
		t.Fatal("lagging follower did not catch up")
	}
	if c.cnt.snapshots.Load() == 0 {
		t.Fatal("follower caught up without InstallSnapshot")
	}
	s := c.status(lag)
	if s.SnapshotIndex == 0 || s.Snapshots == 0 {
		t.Fatalf("follower status shows no snapshot: %+v", s)
	}
	// Whole history must match, including the part that arrived as a snapshot.
	c.one("after", 3, true)
}

func TestSnapshotRestart(t *testing.T) {
	c := newClusterSnap(t, 3, 10)
	var want []string
	for i := 0; i < 35; i++ {
		v := fmt.Sprintf("e%d", i)
		c.one(v, 3, true)
		want = append(want, v)
	}
	for i := range c.ids {
		if !waitFor(2*time.Second, func() bool {
			s, _ := c.stores[i].LoadSnapshot()
			return s.LastIndex > 0
		}) {
			t.Fatalf("node %d never saved a snapshot", i)
		}
	}

	c.crashAll()
	for i := range c.ids {
		c.restart(i)
		// NewNode already restored the snapshot into the fresh state machine.
		if len(c.sm(i).values()) == 0 {
			t.Fatalf("node %d state machine empty after restart despite snapshot", i)
		}
	}
	c.checkOneLeader()
	c.one("after", 3, true)
	want = append(want, "after")
	for i := range c.ids {
		if !waitFor(3*time.Second, func() bool { return len(dedupe(c.sm(i).values())) == len(want) }) {
			t.Fatalf("node %d applied %d entries, want %d", i, len(c.sm(i).values()), len(want))
		}
		if got := dedupe(c.sm(i).values()); fmt.Sprint(got) != fmt.Sprint(want) {
			t.Fatalf("node %d applied %v, want %v", i, got, want)
		}
	}
}

func TestReadIndexDeposedLeader(t *testing.T) {
	c := newCluster(t, 3)
	c.one("a", 3, true)
	l1 := c.checkOneLeader()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	if err := c.node(l1).ReadIndex(ctx); err != nil {
		t.Fatalf("ReadIndex on healthy leader: %v", err)
	}
	cancel()

	// A follower refuses.
	if err := c.node((l1 + 1) % 3).ReadIndex(context.Background()); err != raft.ErrNotLeader {
		t.Fatalf("follower ReadIndex = %v, want ErrNotLeader", err)
	}

	// Partition the leader away. It may still think it leads for a moment, but
	// its ReadIndex must never succeed.
	c.disconnect(l1)
	readErr := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		readErr <- c.node(l1).ReadIndex(ctx)
	}()
	l2 := c.checkOneLeader()
	c.one("b", 2, true) // a write the deposed leader has not seen

	if err := <-readErr; err == nil {
		t.Fatal("deposed leader's ReadIndex succeeded")
	}
	if err := c.node(l1).ReadIndex(context.Background()); err == nil {
		t.Fatal("deposed leader's ReadIndex succeeded on retry")
	}

	// The real leader serves a linearizable read: after ReadIndex, its state
	// machine already holds the committed write.
	if err := c.node(l2).ReadIndex(context.Background()); err != nil {
		t.Fatalf("ReadIndex on new leader: %v", err)
	}
	vals := c.sm(l2).values()
	if len(vals) < 2 || vals[len(vals)-1] != "b" {
		t.Fatalf("new leader state after ReadIndex = %v, want to end with b", vals)
	}

	c.connect(l1)
	waitFor(2*time.Second, func() bool { return c.status(l1).Role == raft.Follower })
	if err := c.node(l1).ReadIndex(context.Background()); err != raft.ErrNotLeader {
		t.Fatalf("ex-leader ReadIndex = %v, want ErrNotLeader", err)
	}
}

func TestPreVoteNoTermInflation(t *testing.T) {
	c := newCluster(t, 3)
	l := c.checkOneLeader()
	term := c.status(l).Term
	victim := (l + 1) % 3

	c.disconnect(victim)
	time.Sleep(800 * time.Millisecond) // roughly ten election timeouts
	if got := c.status(victim).Term; got != term {
		t.Fatalf("isolated node's term grew from %d to %d", term, got)
	}

	c.connect(victim)
	time.Sleep(400 * time.Millisecond)
	for i := range c.ids {
		if got := c.status(i).Term; got != term {
			t.Fatalf("node %d term %d after rejoin, want unchanged %d", i, got, term)
		}
	}
	if l2 := c.checkOneLeader(); l2 != l {
		t.Fatalf("leadership moved from %d to %d on rejoin", l, l2)
	}
	c.one("x", 3, true)
}

func TestProposeOnFollower(t *testing.T) {
	c := newCluster(t, 3)
	l := c.checkOneLeader()
	f := (l + 1) % 3
	start := time.Now()
	_, _, err := c.node(f).Propose(context.Background(), []byte("x"))
	if err != raft.ErrNotLeader {
		t.Fatalf("follower Propose = %v, want ErrNotLeader", err)
	}
	if time.Since(start) > 100*time.Millisecond {
		t.Fatal("ErrNotLeader was not immediate")
	}
}

func TestProposeLeadershipLost(t *testing.T) {
	c := newCluster(t, 3)
	c.one("a", 3, true)
	l := c.checkOneLeader()
	c.disconnect(l)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, _, err := c.node(l).Propose(ctx, []byte("doomed"))
	if err != raft.ErrLeadershipLost {
		t.Fatalf("Propose on partitioned leader = %v, want ErrLeadershipLost", err)
	}
}

func TestStopUnblocksWaiters(t *testing.T) {
	c := newCluster(t, 3)
	c.one("a", 3, true)
	l := c.checkOneLeader()
	c.disconnect(l)
	errc := make(chan error, 1)
	go func() {
		_, _, err := c.node(l).Propose(context.Background(), []byte("stuck"))
		errc <- err
	}()
	time.Sleep(10 * time.Millisecond)
	c.node(l).Stop()
	select {
	case err := <-errc:
		if err != raft.ErrStopped && err != raft.ErrLeadershipLost {
			t.Fatalf("Propose after Stop = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Propose did not return after Stop")
	}
	if _, _, err := c.node(l).Propose(context.Background(), []byte("x")); err != raft.ErrStopped {
		t.Fatalf("Propose on stopped node = %v, want ErrStopped", err)
	}
	if err := c.node(l).ReadIndex(context.Background()); err != raft.ErrStopped {
		t.Fatalf("ReadIndex on stopped node = %v, want ErrStopped", err)
	}
}

func TestLeaderChangesChannel(t *testing.T) {
	c := newCluster(t, 3)
	l := c.checkOneLeader()
	ch := c.node(l).LeaderChanges()
	if ch != c.node(l).LeaderChanges() {
		t.Fatal("LeaderChanges must return the same channel")
	}
	// The leader's buffer holds at least one true.
	sawTrue := false
	for len(ch) > 0 {
		sawTrue = <-ch
	}
	if !sawTrue {
		t.Fatal("no true on LeaderChanges after becoming leader")
	}
	c.disconnect(l)
	select {
	case v := <-ch:
		if v {
			t.Fatal("got true, want false after losing leadership")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no false on LeaderChanges after losing leadership")
	}
}

func TestSingleNode(t *testing.T) {
	c := newCluster(t, 1)
	l := c.checkOneLeader()
	for i := 0; i < 5; i++ {
		c.one(fmt.Sprintf("s%d", i), 1, true)
	}
	if err := c.node(l).ReadIndex(context.Background()); err != nil {
		t.Fatalf("single node ReadIndex: %v", err)
	}
	c.crash(0)
	c.restart(0)
	c.checkOneLeader()
	c.one("again", 1, true)
	if got := c.sm(0).values(); len(got) != 6 {
		t.Fatalf("applied %v", got)
	}
}

func BenchmarkPropose3Nodes(b *testing.B) {
	// Default-ish timeouts: a saturated benchmark must not look like a failure.
	c := newClusterTimes(b, 3, 0, 150*time.Millisecond, 300*time.Millisecond, 50*time.Millisecond)
	if os.Getenv("RAFT_BENCH_TIGHT") != "" {
		c = newCluster(b, 3)
	}
	l := c.checkOneLeader()
	node := c.node(l)
	payload := []byte("benchmark-payload")
	ctx := context.Background()
	b.ResetTimer()
	b.SetParallelism(16)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, _, err := node.Propose(ctx, payload); err != nil {
				b.Error(err)
				return
			}
		}
	})
	b.StopTimer()
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "commits/s")
}

// TestFileStorageCluster runs the persistence and snapshot paths against the
// real on-disk storage instead of MemStorage.
func TestFileStorageCluster(t *testing.T) {
	c := newFileCluster(t, 3, 10)
	var want []string
	for i := 0; i < 30; i++ {
		v := fmt.Sprintf("e%d", i)
		c.one(v, 3, true)
		want = append(want, v)
	}
	c.crashAll()
	for i := range c.ids {
		c.restart(i)
	}
	c.checkOneLeader()
	c.one("after", 3, true)
	want = append(want, "after")
	for i := range c.ids {
		if !waitFor(3*time.Second, func() bool { return len(dedupe(c.sm(i).values())) == len(want) }) {
			t.Fatalf("node %d applied %v, want %v", i, c.sm(i).values(), want)
		}
		if got := dedupe(c.sm(i).values()); fmt.Sprint(got) != fmt.Sprint(want) {
			t.Fatalf("node %d applied %v, want %v", i, got, want)
		}
	}
	// Lagging follower on disk storage catches up via InstallSnapshot.
	l := c.checkOneLeader()
	lag := (l + 1) % 3
	c.disconnect(lag)
	var last uint64
	for i := 0; i < 30; i++ {
		last = c.one(fmt.Sprintf("g%d", i), 2, true)
	}
	c.connect(lag)
	if !c.waitCommitted(last, 3, "g29", 5*time.Second) {
		t.Fatal("lagging follower did not catch up")
	}
}
