package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

type nodeProc struct {
	id, addr, dir, logPath string
	cmd                    *exec.Cmd
	exited                 chan struct{}
	running                bool
	paused                 bool
}

type cluster struct {
	binary    string
	nodes     []*nodeProc
	peers     string
	snapshot  uint64
	hc        *http.Client
	mu        sync.Mutex
	extra     []string
	startedAt map[string]int // restarts per node, for log annotation
}

func newCluster(binary, root string, n, basePort int, snapshot uint64, extra []string) (*cluster, error) {
	abs, err := filepath.Abs(binary)
	if err != nil {
		return nil, err
	}
	if _, err := os.Stat(abs); err != nil {
		return nil, fmt.Errorf("goqueued binary not found at %s (run `make build`): %w", abs, err)
	}
	c := &cluster{extra: extra, binary: abs, snapshot: snapshot, hc: &http.Client{}, startedAt: map[string]int{}}
	var peers []string
	for i := 1; i <= n; i++ {
		id := fmt.Sprintf("n%d", i)
		addr := fmt.Sprintf("127.0.0.1:%d", basePort+i-1)
		dir := filepath.Join(root, id)
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, err
		}
		c.nodes = append(c.nodes, &nodeProc{id: id, addr: addr, dir: dir, logPath: filepath.Join(root, id+".log")})
		peers = append(peers, fmt.Sprintf("%s=http://%s", id, addr))
	}
	c.peers = strings.Join(peers, ",")
	return c, nil
}

func (n *nodeProc) url() string { return "http://" + n.addr }

func (c *cluster) urls() []string {
	var u []string
	for _, n := range c.nodes {
		u = append(u, n.url())
	}
	return u
}

func (c *cluster) start(n *nodeProc) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if n.running {
		return nil
	}
	lf, err := os.OpenFile(n.logPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	c.startedAt[n.id]++
	fmt.Fprintf(lf, "---- start #%d at %s ----\n", c.startedAt[n.id], time.Now().Format(time.RFC3339Nano))
	args := []string{"--backend=raft", "--http=" + n.addr, "--node-id=" + n.id,
		"--peers=" + c.peers, "--data-dir=" + n.dir, fmt.Sprintf("--snapshot-threshold=%d", c.snapshot), "--log-level=warn"}
	cmd := exec.Command(c.binary, append(args, c.extra...)...)
	cmd.Stdout, cmd.Stderr = lf, lf
	if err := cmd.Start(); err != nil {
		lf.Close()
		return err
	}
	n.cmd, n.running, n.paused = cmd, true, false
	n.exited = make(chan struct{})
	go func() {
		_ = cmd.Wait()
		lf.Close()
		close(n.exited)
	}()
	return nil
}

func (c *cluster) kill(n *nodeProc) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !n.running {
		return
	}
	if n.paused {
		_ = resumeProcess(n.cmd.Process.Pid)
	}
	_ = n.cmd.Process.Kill()
	<-n.exited
	n.running, n.paused = false, false
}

func (c *cluster) killAll() {
	for _, n := range c.nodes {
		c.kill(n)
	}
}

// leader returns the running node that reports itself leader in the highest term.
func (c *cluster) leader(ctx context.Context) *nodeProc {
	var best *nodeProc
	var bestTerm uint64
	for _, n := range c.nodes {
		c.mu.Lock()
		ok := n.running && !n.paused
		c.mu.Unlock()
		if !ok {
			continue
		}
		st, err := clusterStatusAt(ctx, c.hc, n.url())
		if err == nil && st.Role == "leader" && (best == nil || st.Term > bestTerm) {
			best, bestTerm = n, st.Term
		}
	}
	return best
}

func (c *cluster) waitLeader(ctx context.Context, timeout time.Duration) (*nodeProc, error) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) && ctx.Err() == nil {
		if l := c.leader(ctx); l != nil {
			return l, nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	return nil, errors.New("no leader elected in time")
}

func cmdChaos(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("chaos", flag.ExitOnError)
	var c loadCfg
	addLoadFlags(fs, &c)
	c.duration, c.lease = 120*time.Second, 5*time.Second
	nodes := fs.Int("nodes", 3, "cluster size")
	binary := fs.String("binary", "./bin/goqueued.exe", "path to the goqueued binary")
	killEvery := fs.Duration("kill-every", 10*time.Second, "inject a fault into the leader this often")
	restartDelay := fs.Duration("restart-delay", 2*time.Second, "how long a killed node stays down (or a partitioned node stays frozen)")
	partition := fs.Bool("partition", false, "freeze the leader (SIGSTOP / NtSuspendProcess) instead of killing it; it resumes after --restart-delay with its state and connections intact, simulating a partitioned leader")
	basePort := fs.Int("base-port", 7101, "first node's HTTP port")
	snap := fs.Uint64("snapshot-threshold", 2000, "raft snapshot threshold passed to every node (small, so snapshots happen during the run)")
	out := fs.String("out", "bench/results", "results directory")
	keep := fs.Bool("keep", false, "keep the temp data directory and node logs")
	retention := fs.Duration("retention", 30*time.Second, "how long nodes keep finished jobs (bounds snapshot size; the server default is 1h)")
	nodeArgs := fs.String("node-args", "", "extra space-separated flags passed to every goqueued")
	label := fs.String("backend-label", "", "label for the report (default raft-N)")
	fs.Parse(args)
	if *label == "" {
		*label = fmt.Sprintf("raft-%d", *nodes)
	}
	fault := "kill"
	if *partition {
		fault = "freeze"
		if *restartDelay == 2*time.Second {
			*restartDelay = 5 * time.Second
		}
	}

	root, err := os.MkdirTemp("", "goqueue-chaos-")
	if err != nil {
		return err
	}
	cl, err := newCluster(*binary, root, *nodes, *basePort, *snap, append([]string{"--retention=" + retention.String()}, strings.Fields(*nodeArgs)...))
	if err != nil {
		return err
	}
	defer func() {
		cl.killAll()
		if !*keep {
			os.RemoveAll(root)
		} else {
			fmt.Println("kept data and logs in", root)
		}
	}()
	for _, n := range cl.nodes {
		if err := cl.start(n); err != nil {
			return fmt.Errorf("start %s: %w", n.id, err)
		}
	}
	if _, err := cl.waitLeader(ctx, 30*time.Second); err != nil {
		return fmt.Errorf("%w (logs in %s)", err, root)
	}
	fmt.Printf("cluster of %d up (logs: %s). load for %s, %s the leader every %s\n", *nodes, root, c.duration, fault, *killEvery)

	c.urls = cl.urls()
	s := newLoadState(c)
	s.cl.enqAttempt = 3 * time.Second

	var (
		faults   []faultRec
		faultsMu sync.Mutex
		restarts int
		fwg      sync.WaitGroup
	)
	loadCtx, cancelLoad := context.WithTimeout(ctx, c.duration) // bounds the fault injector only
	defer cancelLoad()
	begin := time.Now()
	fwg.Add(1)
	go func() {
		defer fwg.Done()
		// Let the load ramp up before the first fault.
		t := time.NewTicker(*killEvery)
		defer t.Stop()
		stopAt := time.Now().Add(c.duration)
		for {
			select {
			case <-loadCtx.Done():
				return
			case <-t.C:
			}
			if time.Now().Add(*restartDelay).After(stopAt) {
				return
			}
			ldr, err := cl.waitLeader(loadCtx, 15*time.Second)
			if err != nil {
				fmt.Println("chaos: no leader to fault:", err)
				continue
			}
			at := time.Now()
			if *partition {
				err = pauseProcess(ldr.cmd.Process.Pid)
				cl.mu.Lock()
				ldr.paused = err == nil
				cl.mu.Unlock()
			} else {
				cl.kill(ldr)
			}
			if err != nil {
				fmt.Println("chaos: fault failed:", err)
				continue
			}
			faultsMu.Lock()
			faults = append(faults, faultRec{node: ldr.id, at: at})
			n := len(faults)
			faultsMu.Unlock()
			fmt.Printf("chaos: %s leader %s (#%d) at +%.1fs\n", fault, ldr.id, n, at.Sub(begin).Seconds())
			time.Sleep(*restartDelay)
			if *partition {
				_ = resumeProcess(ldr.cmd.Process.Pid)
				cl.mu.Lock()
				ldr.paused = false
				cl.mu.Unlock()
			} else if err := cl.start(ldr); err != nil {
				fmt.Println("chaos: restart failed:", err)
			}
			restarts++
		}
	}()

	s.run(ctx) // producers stop after c.duration; workers then drain
	cancelLoad()
	fwg.Wait()
	for _, n := range cl.nodes { // bring everything back for the final accounting
		if !n.running {
			_ = cl.start(n)
		}
	}
	fmt.Println("verifying acknowledged enqueues...")
	vctx, vcancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer vcancel()
	if _, err := cl.waitLeader(vctx, 30*time.Second); err != nil {
		fmt.Println("warning:", err)
	}
	v := s.verify(vctx)

	rep := newReport(fmt.Sprintf("chaos-%s", *label), *label, commandLine(), map[string]any{
		"nodes": *nodes, "producers": c.producers, "workers": c.workers, "duration": c.duration.String(),
		"payload_bytes": c.payload, "batch": c.batch, "lease": c.lease.String(), "fault": fault,
		"fault_every": killEvery.String(), "retention": retention.String(), "restart_delay": restartDelay.String(), "snapshot_threshold": *snap,
	})
	rep.fill(s, v)
	rep.Chaos = chaosReport(s, faults, *nodes, killEvery.String(), restarts, *partition)

	var notes []string
	for _, n := range cl.nodes {
		if st, err := clusterStatusAt(vctx, cl.hc, n.url()); err == nil {
			notes = append(notes, fmt.Sprintf("%s role=%s term=%d commit=%d applied=%d", n.id, st.Role, st.Term, st.Commit, st.Applied))
		} else {
			notes = append(notes, fmt.Sprintf("%s unreachable: %v", n.id, err))
		}
	}
	rep.ClusterNotes = "Final cluster status:\n\n- " + strings.Join(notes, "\n- ")
	base, err := rep.write(*out)
	if err != nil {
		return err
	}
	fmt.Print(rep.markdown())
	fmt.Println("results:", base+".json", base+".md")
	if rep.Verify.Lost > 0 {
		*keep = true
		return fmt.Errorf("%d acknowledged jobs were lost (logs kept in %s)", rep.Verify.Lost, root)
	}
	return nil
}

type faultRec struct {
	node string
	at   time.Time
}

func chaosReport(s *loadState, faults []faultRec, nodes int, every string, restarts int, partition bool) *Chaos {
	times := append([]int64(nil), s.successTimes...)
	sort.Slice(times, func(i, j int) bool { return times[i] < times[j] })
	ch := &Chaos{Nodes: nodes, KillEvery: every, Kills: len(faults), Restarts: restarts}
	if partition {
		ch.PartitionSupported = true
		ch.PartitionNote = "leader frozen with NtSuspendProcess/SIGSTOP for the restart delay, then resumed (counted under kills)"
	} else {
		ch.PartitionNote = "not run (re-run with --partition to freeze the leader instead of killing it)"
	}
	var durs []time.Duration
	for _, f := range faults {
		i := sort.Search(len(times), func(i int) bool { return times[i] > f.at.UnixNano() })
		rec := Failover{KilledNode: f.node, KilledAt: f.at.Format("15:04:05.000"), FailoverMs: -1}
		if i < len(times) {
			d := time.Duration(times[i] - f.at.UnixNano())
			rec.FailoverMs = ms(d)
			durs = append(durs, d)
		} else {
			ch.Unrecovered++
		}
		ch.Failovers = append(ch.Failovers, rec)
	}
	if len(durs) > 0 {
		sum := summarize(durs)
		ch.FailoverP50Ms, ch.FailoverMaxMs = sum.P50Ms, sum.MaxMs
	}
	var gap int64
	for i := 1; i < len(times); i++ {
		gap = max(gap, times[i]-times[i-1])
	}
	ch.LongestEnqueueGap = ms(time.Duration(gap))
	return ch
}
