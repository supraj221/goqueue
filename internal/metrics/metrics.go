// Package metrics defines goqueue's Prometheus collectors (docs/PLAN.md
// section 7, queue and HTTP families).
package metrics

import (
	"context"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/supraj221/goqueue/internal/broker"
	"github.com/supraj221/goqueue/internal/job"
)

// Metrics holds the queue and HTTP collectors.
type Metrics struct {
	reg prometheus.Registerer

	enqueued      *prometheus.CounterVec
	claimed       *prometheus.CounterVec
	acked         *prometheus.CounterVec
	nacked        *prometheus.CounterVec
	dead          *prometheus.CounterVec
	leasesExpired *prometheus.CounterVec
	queueWait     *prometheus.HistogramVec
	e2e           *prometheus.HistogramVec
	httpDuration  *prometheus.HistogramVec
}

var latencyBuckets = []float64{.001, .0025, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30}
var waitBuckets = []float64{.001, .01, .05, .1, .5, 1, 5, 15, 60, 300, 1800, 7200}

// New creates the collectors and registers them on reg (a fresh registry is
// used if reg is nil). Registering twice on the same registry panics.
func New(reg prometheus.Registerer) *Metrics {
	if reg == nil {
		reg = prometheus.NewRegistry()
	}
	counter := func(name, help string, labels ...string) *prometheus.CounterVec {
		return prometheus.NewCounterVec(prometheus.CounterOpts{Name: name, Help: help}, labels)
	}
	m := &Metrics{
		reg:           reg,
		enqueued:      counter("goqueue_jobs_enqueued_total", "Jobs enqueued (excluding idempotent duplicates).", "queue"),
		claimed:       counter("goqueue_jobs_claimed_total", "Jobs claimed by workers.", "queue"),
		acked:         counter("goqueue_jobs_acked_total", "Jobs acknowledged as succeeded.", "queue"),
		nacked:        counter("goqueue_jobs_nacked_total", "Jobs negatively acknowledged.", "queue", "retry"),
		dead:          counter("goqueue_jobs_dead_total", "Jobs moved to the dead state.", "queue"),
		leasesExpired: counter("goqueue_leases_expired_total", "Leases that expired before ack.", "queue"),
		queueWait: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "goqueue_job_queue_wait_seconds", Help: "Time from run_at to claim.", Buckets: waitBuckets,
		}, []string{"queue"}),
		e2e: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "goqueue_job_e2e_seconds", Help: "Time from enqueue to ack.", Buckets: waitBuckets,
		}, []string{"queue"}),
		httpDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "goqueue_http_request_duration_seconds", Help: "HTTP request duration.", Buckets: latencyBuckets,
		}, []string{"route", "method", "code"}),
	}
	reg.MustRegister(m.enqueued, m.claimed, m.acked, m.nacked, m.dead, m.leasesExpired, m.queueWait, m.e2e, m.httpDuration)
	return m
}

// JobsEnqueued records one newly created job.
func (m *Metrics) JobsEnqueued(queue string) { m.enqueued.WithLabelValues(queue).Inc() }

// JobsClaimed records n claimed jobs.
func (m *Metrics) JobsClaimed(queue string, n int) { m.claimed.WithLabelValues(queue).Add(float64(n)) }

// JobsAcked records one acked job.
func (m *Metrics) JobsAcked(queue string) { m.acked.WithLabelValues(queue).Inc() }

// JobsNacked records one nack.
func (m *Metrics) JobsNacked(queue string, retry bool) {
	m.nacked.WithLabelValues(queue, strconv.FormatBool(retry)).Inc()
}

// JobsDead records one job moving to dead.
func (m *Metrics) JobsDead(queue string) { m.dead.WithLabelValues(queue).Inc() }

// LeasesExpired records n expired leases (for backends that observe them).
func (m *Metrics) LeasesExpired(queue string, n int) {
	m.leasesExpired.WithLabelValues(queue).Add(float64(n))
}

// ObserveQueueWait records how long a job waited between becoming runnable and
// being claimed.
func (m *Metrics) ObserveQueueWait(queue string, d time.Duration) {
	m.queueWait.WithLabelValues(queue).Observe(max(d, 0).Seconds())
}

// ObserveE2E records enqueue-to-ack time.
func (m *Metrics) ObserveE2E(queue string, d time.Duration) {
	m.e2e.WithLabelValues(queue).Observe(max(d, 0).Seconds())
}

// statusRecorder captures the response status code.
type statusRecorder struct {
	http.ResponseWriter
	code int
}

func (r *statusRecorder) WriteHeader(code int) {
	if r.code == 0 {
		r.code = code
	}
	r.ResponseWriter.WriteHeader(code)
}

func (r *statusRecorder) Write(p []byte) (int, error) {
	if r.code == 0 {
		r.code = http.StatusOK
	}
	return r.ResponseWriter.Write(p)
}

// Unwrap lets http.ResponseController reach the underlying writer.
func (r *statusRecorder) Unwrap() http.ResponseWriter { return r.ResponseWriter }

// Middleware records goqueue_http_request_duration_seconds for h under the
// fixed route label (a pattern, never the raw path, to keep cardinality low).
func (m *Metrics) Middleware(route string, h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w}
		h.ServeHTTP(rec, r)
		code := rec.code
		if code == 0 {
			code = http.StatusOK
		}
		m.httpDuration.WithLabelValues(route, r.Method, strconv.Itoa(code)).Observe(time.Since(start).Seconds())
	})
}

// depthCollector exposes goqueue_queue_depth{queue,state} from broker.Stats,
// calling the broker at most once per ttl.
type depthCollector struct {
	b    broker.Broker
	ttl  time.Duration
	desc *prometheus.Desc

	mu      sync.Mutex
	fetched time.Time
	stats   broker.Stats
}

// WatchDepth registers the queue-depth gauge, backed by b.Stats and cached for
// 5 seconds.
func (m *Metrics) WatchDepth(b broker.Broker) {
	m.watchDepth(b, 5*time.Second)
}

func (m *Metrics) watchDepth(b broker.Broker, ttl time.Duration) {
	m.reg.MustRegister(&depthCollector{
		b:    b,
		ttl:  ttl,
		desc: prometheus.NewDesc("goqueue_queue_depth", "Jobs per queue and state.", []string{"queue", "state"}, nil),
	})
}

func (c *depthCollector) Describe(ch chan<- *prometheus.Desc) { ch <- c.desc }

func (c *depthCollector) Collect(ch chan<- prometheus.Metric) {
	c.mu.Lock()
	if c.fetched.IsZero() || time.Since(c.fetched) >= c.ttl {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		s, err := c.b.Stats(ctx)
		cancel()
		if err == nil {
			c.stats = s
		}
		// On error keep serving the previous value, but still back off for ttl.
		c.fetched = time.Now()
	}
	stats := c.stats
	c.mu.Unlock()

	for q, byState := range stats.Counts {
		for _, st := range job.AllStates {
			ch <- prometheus.MustNewConstMetric(c.desc, prometheus.GaugeValue, float64(byState[st]), q, string(st))
		}
	}
}
