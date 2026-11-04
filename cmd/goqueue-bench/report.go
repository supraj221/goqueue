package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"
)

// Report is the JSON written for every benchmark run.
type Report struct {
	Scenario  string         `json:"scenario"`
	Backend   string         `json:"backend"`
	Timestamp string         `json:"timestamp"`
	GOOS      string         `json:"goos"`
	GOARCH    string         `json:"goarch"`
	NumCPU    int            `json:"num_cpu"`
	GoVersion string         `json:"go_version"`
	Command   string         `json:"command"`
	Params    map[string]any `json:"params"`

	LoadSeconds  float64 `json:"load_seconds"`
	DrainSeconds float64 `json:"drain_seconds"`

	Enqueued               int     `json:"enqueued"`
	EnqueueErrors          int     `json:"enqueue_errors"`
	EnqueuePerSec          float64 `json:"enqueue_per_sec"`
	ProcessedTotal         int     `json:"processed_total"`
	ProcessedDuringLoad    int     `json:"processed_during_load"`
	ProcessedPerSecDuring  float64 `json:"processed_per_sec_during_load"`
	ProcessedPerSecOverall float64 `json:"processed_per_sec_overall"`
	DequeueErrors          int64   `json:"dequeue_errors"`
	AckLeaseLost           int64   `json:"ack_lease_lost"`
	EnqueueLatency         Latency `json:"enqueue_latency"`
	QueueWait              Latency `json:"queue_wait"`
	EndToEnd               Latency `json:"end_to_end"`
	Verify                 Verify  `json:"verify"`
	Chaos                  *Chaos  `json:"chaos,omitempty"`
	ClusterNotes           string  `json:"notes,omitempty"`
}

// Verify is the post-run accounting of acknowledged enqueues.
type Verify struct {
	Acknowledged     int            `json:"acknowledged_enqueues"`
	Processed        int            `json:"processed_by_workers"`
	RecoveredByGet   int            `json:"recovered_by_get"`
	Lost             int            `json:"lost_jobs"`
	LostStates       map[string]int `json:"lost_states,omitempty"`
	LostIDs          []string       `json:"lost_ids,omitempty"`
	DuplicateJobs    int            `json:"duplicate_deliveries_jobs"`
	ExtraDeliveries  int            `json:"duplicate_deliveries_extra"`
	DeliveredUnknown int            `json:"delivered_not_acknowledged"`
}

// Chaos holds the failure-injection results.
type Chaos struct {
	Nodes              int        `json:"nodes"`
	KillEvery          string     `json:"kill_every"`
	Kills              int        `json:"kills"`
	Restarts           int        `json:"restarts"`
	Failovers          []Failover `json:"failovers"`
	FailoverP50Ms      float64    `json:"failover_p50_ms"`
	FailoverMaxMs      float64    `json:"failover_max_ms"`
	LongestEnqueueGap  float64    `json:"longest_enqueue_gap_ms"`
	Unrecovered        int        `json:"unrecovered_kills"`
	PartitionSupported bool       `json:"partition_supported"`
	PartitionNote      string     `json:"partition_note"`
}

// Failover is one leader kill and how long writes were unavailable.
type Failover struct {
	KilledNode string  `json:"killed_node"`
	KilledAt   string  `json:"killed_at"`
	FailoverMs float64 `json:"failover_ms"` // kill until first successful enqueue; -1 if none
}

func newReport(scenario, backend, command string, params map[string]any) *Report {
	return &Report{
		Scenario: scenario, Backend: backend, Command: command, Params: params,
		Timestamp: time.Now().UTC().Format(time.RFC3339),
		GOOS:      runtime.GOOS, GOARCH: runtime.GOARCH, NumCPU: runtime.NumCPU(), GoVersion: runtime.Version(),
	}
}

// fill copies the load results into r.
func (r *Report) fill(s *loadState, v verifyResult) {
	loadDur := s.loadEnd.Sub(s.start).Seconds()
	r.LoadSeconds = loadDur
	r.DrainSeconds = s.drainEnd.Sub(s.loadEnd).Seconds()
	r.Enqueued = int(s.enqOK.Load())
	r.EnqueueErrors = int(s.enqErr.Load())
	r.EnqueuePerSec = float64(r.Enqueued) / loadDur
	r.ProcessedTotal = int(s.procCount.Load())
	r.ProcessedDuringLoad = int(s.processedAtLoadEnd)
	r.ProcessedPerSecDuring = float64(s.processedAtLoadEnd) / loadDur
	r.ProcessedPerSecOverall = float64(r.ProcessedTotal) / s.drainEnd.Sub(s.start).Seconds()
	r.DequeueErrors = s.dequeueErr.Load()
	r.AckLeaseLost = s.ackLost.Load()
	r.EnqueueLatency = summarize(s.enqLat.all())
	r.QueueWait = summarize(s.qwait.all())
	r.EndToEnd = summarize(s.e2e.all())
	r.Verify = Verify(v)
}

// write saves <dir>/<scenario>-<timestamp>.{json,md} and returns the base path.
func (r *Report) write(dir string) (string, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", err
	}
	ts := time.Now().Format("20060102-150405")
	base := filepath.Join(dir, fmt.Sprintf("%s-%s", r.Scenario, ts))
	b, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return "", err
	}
	if err := os.WriteFile(base+".json", b, 0o644); err != nil {
		return "", err
	}
	if err := os.WriteFile(base+".md", []byte(r.markdown()), 0o644); err != nil {
		return "", err
	}
	return base, nil
}

func (r *Report) markdown() string {
	var b strings.Builder
	fmt.Fprintf(&b, "# %s (%s)\n\n", r.Scenario, r.Backend)
	fmt.Fprintf(&b, "- Run at: %s\n- Machine: %s/%s, %d logical CPUs, %s\n- Command: `%s`\n\n",
		r.Timestamp, r.GOOS, r.GOARCH, r.NumCPU, r.GoVersion, r.Command)
	b.WriteString("## Throughput\n\n| Metric | Value |\n|---|---|\n")
	fmt.Fprintf(&b, "| Load duration | %.1f s |\n| Drain time | %.1f s |\n", r.LoadSeconds, r.DrainSeconds)
	fmt.Fprintf(&b, "| Enqueued (acknowledged) | %d |\n| Enqueue errors | %d |\n| Enqueue throughput | %.0f jobs/s |\n",
		r.Enqueued, r.EnqueueErrors, r.EnqueuePerSec)
	fmt.Fprintf(&b, "| Processed during load | %d (%.0f jobs/s) |\n| Processed total | %d (%.0f jobs/s over the whole run) |\n\n",
		r.ProcessedDuringLoad, r.ProcessedPerSecDuring, r.ProcessedTotal, r.ProcessedPerSecOverall)
	b.WriteString("## Latency (ms)\n\n| Measure | Count | p50 | p95 | p99 | max | mean |\n|---|---|---|---|---|---|---|\n")
	for _, row := range []struct {
		name string
		l    Latency
	}{{"Enqueue", r.EnqueueLatency}, {"Queue wait (enqueue request to claim)", r.QueueWait}, {"End to end (enqueue request to ack)", r.EndToEnd}} {
		fmt.Fprintf(&b, "| %s | %d | %.2f | %.2f | %.2f | %.2f | %.2f |\n", row.name, row.l.Count, row.l.P50Ms, row.l.P95Ms, row.l.P99Ms, row.l.MaxMs, row.l.MeanMs)
	}
	b.WriteString("\n## Correctness\n\n| Check | Value |\n|---|---|\n")
	v := r.Verify
	fmt.Fprintf(&b, "| Acknowledged enqueues | %d |\n| Processed by workers | %d |\n| Processed but ack response lost (verified via GET) | %d |\n",
		v.Acknowledged, v.Processed, v.RecoveredByGet)
	fmt.Fprintf(&b, "| **Lost jobs** | **%d** |\n| Duplicate deliveries (jobs delivered more than once) | %d (%d extra deliveries) |\n",
		v.Lost, v.DuplicateJobs, v.ExtraDeliveries)
	fmt.Fprintf(&b, "| Worker acks answered lease_lost | %d |\n| Dequeue errors | %d |\n", r.AckLeaseLost, r.DequeueErrors)
	if len(v.LostStates) > 0 {
		fmt.Fprintf(&b, "| Lost job states | %v |\n", v.LostStates)
	}
	if c := r.Chaos; c != nil {
		b.WriteString("\n## Chaos\n\n| Metric | Value |\n|---|---|\n")
		fmt.Fprintf(&b, "| Nodes | %d |\n| Leader kill interval | %s |\n| Leader kills | %d |\n| Restarts | %d |\n", c.Nodes, c.KillEvery, c.Kills, c.Restarts)
		fmt.Fprintf(&b, "| Failover time p50 (kill to first successful enqueue) | %.0f ms |\n| Failover time max | %.0f ms |\n", c.FailoverP50Ms, c.FailoverMaxMs)
		fmt.Fprintf(&b, "| Longest gap between successful enqueues | %.0f ms |\n| Kills with no recovery before the load ended | %d |\n", c.LongestEnqueueGap, c.Unrecovered)
		fmt.Fprintf(&b, "| Partition scenario | %s |\n", c.PartitionNote)
		b.WriteString("\n| # | Killed node | Killed at | Failover |\n|---|---|---|---|\n")
		for i, f := range c.Failovers {
			fmt.Fprintf(&b, "| %d | %s | %s | %.0f ms |\n", i+1, f.KilledNode, f.KilledAt, f.FailoverMs)
		}
	}
	if r.ClusterNotes != "" {
		fmt.Fprintf(&b, "\n%s\n", r.ClusterNotes)
	}
	return b.String()
}
