package raftbroker

import (
	"github.com/prometheus/client_golang/prometheus"
)

type metrics struct {
	c       Consensus
	propose prometheus.Histogram

	term, isLeader, commit, applied, lastIndex *prometheus.Desc
	leaderChanges, snapshots                   *prometheus.Desc
}

func newMetrics(c Consensus) *metrics {
	d := func(name, help string) *prometheus.Desc {
		return prometheus.NewDesc("goqueue_raft_"+name, help, nil, nil)
	}
	return &metrics{
		c: c,
		propose: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "goqueue_raft_propose_seconds",
			Help:    "Latency of Raft proposals, from submit to applied result.",
			Buckets: []float64{.0005, .001, .0025, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10},
		}),
		term:          d("term", "Current Raft term."),
		isLeader:      d("is_leader", "1 if this node is the Raft leader."),
		commit:        d("commit_index", "Raft commit index."),
		applied:       d("applied_index", "Index of the last entry applied to the state machine."),
		lastIndex:     d("last_log_index", "Index of the last entry in the Raft log."),
		leaderChanges: d("leader_changes_total", "Number of leader changes observed by this node."),
		snapshots:     d("snapshot_total", "Snapshots taken or installed on this node."),
	}
}

// Describe implements prometheus.Collector.
func (m *metrics) Describe(ch chan<- *prometheus.Desc) {
	for _, d := range []*prometheus.Desc{m.term, m.isLeader, m.commit, m.applied, m.lastIndex, m.leaderChanges, m.snapshots} {
		ch <- d
	}
}

// Collect implements prometheus.Collector, reading Status() on every scrape.
func (m *metrics) Collect(ch chan<- prometheus.Metric) {
	st := m.c.Status()
	g := prometheus.GaugeValue
	ch <- prometheus.MustNewConstMetric(m.term, g, float64(st.Term))
	leader := 0.0
	if st.Role == "leader" {
		leader = 1
	}
	ch <- prometheus.MustNewConstMetric(m.isLeader, g, leader)
	ch <- prometheus.MustNewConstMetric(m.commit, g, float64(st.CommitIndex))
	ch <- prometheus.MustNewConstMetric(m.applied, g, float64(st.LastApplied))
	ch <- prometheus.MustNewConstMetric(m.lastIndex, g, float64(st.LastLogIndex))
	ch <- prometheus.MustNewConstMetric(m.leaderChanges, prometheus.CounterValue, float64(st.LeaderChanges))
	ch <- prometheus.MustNewConstMetric(m.snapshots, prometheus.CounterValue, float64(st.Snapshots))
}

// RegisterMetrics registers the Raft collector and the propose-latency
// histogram on reg.
func (b *Broker) RegisterMetrics(reg prometheus.Registerer) error {
	if err := reg.Register(b.metrics); err != nil {
		return err
	}
	return reg.Register(b.metrics.propose)
}
