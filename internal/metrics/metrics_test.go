package metrics

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/supraj221/goqueue/internal/broker"
	"github.com/supraj221/goqueue/internal/job"
)

type countingBroker struct {
	broker.Broker
	calls atomic.Int32
}

func (c *countingBroker) Stats(context.Context) (broker.Stats, error) {
	c.calls.Add(1)
	return broker.Stats{Counts: map[string]map[job.State]int{"q": {job.StatePending: 7}}}, nil
}

func TestCountersAndMiddleware(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := New(reg)
	m.JobsEnqueued("q")
	m.JobsClaimed("q", 3)
	m.JobsAcked("q")
	m.JobsNacked("q", true)
	m.JobsDead("q")
	m.ObserveQueueWait("q", 1500*time.Millisecond)

	h := m.Middleware("GET /x", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusTeapot)
	}))
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", "/x", nil))

	mfs, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, mf := range mfs {
		if mf.GetName() == "goqueue_jobs_claimed_total" && mf.GetMetric()[0].GetCounter().GetValue() != 3 {
			t.Errorf("claimed = %v", mf.GetMetric()[0].GetCounter().GetValue())
		}
		if mf.GetName() == "goqueue_http_request_duration_seconds" {
			for _, l := range mf.GetMetric()[0].GetLabel() {
				if l.GetName() == "code" && l.GetValue() == "418" {
					found = true
				}
			}
		}
	}
	if !found {
		t.Error("code=418 label missing")
	}
}

func TestDepthCollectorCaches(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := New(reg)
	cb := &countingBroker{}
	m.WatchDepth(cb)
	for range 3 {
		rec := httptest.NewRecorder()
		mfs, err := reg.Gather()
		if err != nil {
			t.Fatal(err)
		}
		_ = rec
		_ = mfs
	}
	if n := cb.calls.Load(); n != 1 {
		t.Errorf("Stats called %d times, want 1 (cached)", n)
	}
	mfs, _ := reg.Gather()
	var text strings.Builder
	for _, mf := range mfs {
		if mf.GetName() == "goqueue_queue_depth" {
			for _, mm := range mf.GetMetric() {
				if mm.GetGauge().GetValue() == 7 {
					text.WriteString("pending=7")
				}
			}
		}
	}
	if text.String() != "pending=7" {
		t.Errorf("depth gauge missing: %q", text.String())
	}
}
