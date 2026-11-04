package api_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/supraj221/goqueue/internal/api"
	"github.com/supraj221/goqueue/internal/broker"
	"github.com/supraj221/goqueue/internal/broker/brokertest"
	"github.com/supraj221/goqueue/internal/broker/pgbroker"
	"github.com/supraj221/goqueue/internal/job"
)

// failBroker wraps a broker and returns err from every data operation when set.
type failBroker struct {
	broker.Broker
	err error
}

func (f *failBroker) Enqueue(ctx context.Context, r broker.EnqueueRequest) (broker.EnqueueResult, error) {
	if f.err != nil {
		return broker.EnqueueResult{}, f.err
	}
	return f.Broker.Enqueue(ctx, r)
}

func (f *failBroker) Ack(ctx context.Context, id string, tok uint64) error {
	if f.err != nil {
		return f.err
	}
	return f.Broker.Ack(ctx, id, tok)
}

func quiet() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func newTestServer(t *testing.T, b broker.Broker, opts api.Options) *httptest.Server {
	t.Helper()
	if opts.Logger == nil {
		opts.Logger = quiet()
	}
	srv := httptest.NewServer(api.NewServer(b, opts).Handler())
	t.Cleanup(srv.Close)
	return srv
}

func do(t *testing.T, method, url string, body any) (*http.Response, []byte) {
	t.Helper()
	var rd io.Reader
	switch v := body.(type) {
	case nil:
	case string:
		rd = strings.NewReader(v)
	default:
		raw, err := json.Marshal(v)
		if err != nil {
			t.Fatal(err)
		}
		rd = bytes.NewReader(raw)
	}
	req, err := http.NewRequest(method, url, rd)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(resp.Body)
	return resp, data
}

func wantStatus(t *testing.T, resp *http.Response, data []byte, code int) {
	t.Helper()
	if resp.StatusCode != code {
		t.Fatalf("status = %d, want %d; body %s", resp.StatusCode, code, data)
	}
}

func wantErrCode(t *testing.T, data []byte, code string) {
	t.Helper()
	var e api.ErrorBody
	if err := json.Unmarshal(data, &e); err != nil || e.Error != code {
		t.Fatalf("error body %s, want code %q", data, code)
	}
}

func TestRoundTrip(t *testing.T) {
	srv := newTestServer(t, brokertest.NewMemBroker(), api.Options{})
	u := srv.URL

	resp, data := do(t, "POST", u+"/v1/jobs", map[string]any{"queue": "q", "type": "email", "payload": map[string]any{"to": "x"}, "priority": 2, "idempotency_key": "k"})
	wantStatus(t, resp, data, http.StatusCreated)
	var j job.Job
	if err := json.Unmarshal(data, &j); err != nil || j.ID == "" || j.State != job.StatePending || j.Priority != 2 {
		t.Fatalf("job = %+v err %v", j, err)
	}

	// Idempotent repeat: 200 and same job.
	resp, data = do(t, "POST", u+"/v1/jobs", map[string]any{"queue": "q", "type": "email", "idempotency_key": "k"})
	wantStatus(t, resp, data, http.StatusOK)
	var j2 job.Job
	_ = json.Unmarshal(data, &j2)
	if j2.ID != j.ID {
		t.Fatalf("idempotent id %s != %s", j2.ID, j.ID)
	}

	resp, data = do(t, "GET", u+"/v1/jobs/"+j.ID, nil)
	wantStatus(t, resp, data, http.StatusOK)

	resp, data = do(t, "POST", u+"/v1/dequeue", map[string]any{"queues": []string{"q"}, "worker_id": "w1", "lease_ms": 30000, "max": 5})
	wantStatus(t, resp, data, http.StatusOK)
	var dq struct{ Jobs []job.Job }
	_ = json.Unmarshal(data, &dq)
	if len(dq.Jobs) != 1 || dq.Jobs[0].LeaseToken != 1 || dq.Jobs[0].LeaseOwner != "w1" {
		t.Fatalf("dequeue = %s", data)
	}
	claimed := dq.Jobs[0]

	resp, data = do(t, "POST", u+"/v1/jobs/"+j.ID+"/heartbeat", map[string]any{"lease_token": claimed.LeaseToken, "extend_ms": 60000})
	wantStatus(t, resp, data, http.StatusOK)
	var hb job.Job
	_ = json.Unmarshal(data, &hb)
	if !hb.LeaseExpiresAt.After(claimed.LeaseExpiresAt) {
		t.Errorf("heartbeat did not extend lease")
	}

	// Stale token: 409 lease_lost.
	resp, data = do(t, "POST", u+"/v1/jobs/"+j.ID+"/ack", map[string]any{"lease_token": 99})
	wantStatus(t, resp, data, http.StatusConflict)
	wantErrCode(t, data, "lease_lost")

	resp, data = do(t, "POST", u+"/v1/jobs/"+j.ID+"/ack", map[string]any{"lease_token": claimed.LeaseToken})
	wantStatus(t, resp, data, http.StatusNoContent)

	resp, data = do(t, "GET", u+"/v1/stats", nil)
	wantStatus(t, resp, data, http.StatusOK)
	var st broker.Stats
	_ = json.Unmarshal(data, &st)
	if st.Counts["q"][job.StateSucceeded] != 1 {
		t.Errorf("stats = %s", data)
	}

	// Empty dequeue returns {"jobs": []}.
	resp, data = do(t, "POST", u+"/v1/dequeue", map[string]any{"queues": []string{"q"}, "worker_id": "w1", "lease_ms": 1000})
	wantStatus(t, resp, data, http.StatusOK)
	if strings.TrimSpace(string(data)) != `{"jobs":[]}` {
		t.Errorf("empty dequeue body = %s", data)
	}
}

func TestNackFlow(t *testing.T) {
	srv := newTestServer(t, brokertest.NewMemBroker(), api.Options{})
	u := srv.URL
	_, data := do(t, "POST", u+"/v1/jobs", map[string]any{"queue": "q", "type": "t"})
	var j job.Job
	_ = json.Unmarshal(data, &j)
	do(t, "POST", u+"/v1/dequeue", map[string]any{"queues": []string{"q"}, "worker_id": "w", "lease_ms": 30000})

	resp, data := do(t, "POST", u+"/v1/jobs/"+j.ID+"/nack", map[string]any{"lease_token": 1, "error": "boom", "retry": true})
	wantStatus(t, resp, data, http.StatusNoContent)
	_, data = do(t, "GET", u+"/v1/jobs/"+j.ID, nil)
	var got job.Job
	_ = json.Unmarshal(data, &got)
	if got.State != job.StateScheduled || got.LastError != "boom" {
		t.Errorf("after retry nack: %+v", got)
	}
}

func TestDelayMs(t *testing.T) {
	srv := newTestServer(t, brokertest.NewMemBroker(), api.Options{})
	resp, data := do(t, "POST", srv.URL+"/v1/jobs", map[string]any{"queue": "q", "type": "t", "delay_ms": 60000})
	wantStatus(t, resp, data, http.StatusCreated)
	var j job.Job
	_ = json.Unmarshal(data, &j)
	if j.State != job.StateScheduled || time.Until(j.RunAt) < 50*time.Second {
		t.Errorf("job = %+v", j)
	}
}

func TestValidationAndErrors(t *testing.T) {
	srv := newTestServer(t, brokertest.NewMemBroker(), api.Options{})
	u := srv.URL
	cases := []struct {
		name   string
		method string
		path   string
		body   any
		status int
		code   string
	}{
		{"missing queue", "POST", "/v1/jobs", map[string]any{"type": "t"}, 400, "invalid"},
		{"bad payload", "POST", "/v1/jobs", `{"queue":"q","type":"t","payload":{bad}`, 400, "invalid"},
		{"unknown field", "POST", "/v1/jobs", map[string]any{"queue": "q", "type": "t", "bogus": 1}, 400, "invalid"},
		{"trailing data", "POST", "/v1/jobs", `{"queue":"q","type":"t"} {}`, 400, "invalid"},
		{"run_at and delay", "POST", "/v1/jobs", map[string]any{"queue": "q", "type": "t", "delay_ms": 5, "run_at": "2030-01-01T00:00:00Z"}, 400, "invalid"},
		{"lease too short", "POST", "/v1/dequeue", map[string]any{"queues": []string{"q"}, "worker_id": "w", "lease_ms": 10}, 400, "invalid"},
		{"no queues", "POST", "/v1/dequeue", map[string]any{"worker_id": "w", "lease_ms": 1000}, 400, "invalid"},
		{"wait too long", "POST", "/v1/dequeue", map[string]any{"queues": []string{"q"}, "worker_id": "w", "lease_ms": 1000, "wait_ms": 31000}, 400, "invalid"},
		{"get missing", "GET", "/v1/jobs/NOPE", nil, 404, "not_found"},
		{"ack missing", "POST", "/v1/jobs/NOPE/ack", map[string]any{"lease_token": 1}, 404, "not_found"},
		{"unknown route", "GET", "/v1/nothing", nil, 404, ""},
		{"wrong method", "DELETE", "/v1/jobs", nil, 405, ""},
		{"no cluster", "GET", "/v1/cluster", nil, 404, "not_found"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resp, data := do(t, tc.method, u+tc.path, tc.body)
			wantStatus(t, resp, data, tc.status)
			if tc.code != "" {
				wantErrCode(t, data, tc.code)
			}
		})
	}
}

func TestBodyTooLarge(t *testing.T) {
	srv := newTestServer(t, brokertest.NewMemBroker(), api.Options{})
	big := `{"queue":"q","type":"t","payload":"` + strings.Repeat("a", 2<<20) + `"}`
	resp, data := do(t, "POST", srv.URL+"/v1/jobs", big)
	wantStatus(t, resp, data, http.StatusRequestEntityTooLarge)
}

func TestErrorMapping(t *testing.T) {
	fb := &failBroker{Broker: brokertest.NewMemBroker()}
	srv := newTestServer(t, fb, api.Options{})
	body := map[string]any{"queue": "q", "type": "t"}

	fb.err = &broker.NotLeaderError{LeaderAddr: "http://leader:8080"}
	resp, data := do(t, "POST", srv.URL+"/v1/jobs", body)
	wantStatus(t, resp, data, http.StatusMisdirectedRequest)
	wantErrCode(t, data, "not_leader")
	if got := resp.Header.Get("X-Goqueue-Leader"); got != "http://leader:8080" {
		t.Errorf("X-Goqueue-Leader = %q", got)
	}

	// Wrapped errors still map, and an unknown leader omits the header.
	fb.err = fmt.Errorf("wrapped: %w", &broker.NotLeaderError{})
	resp, data = do(t, "POST", srv.URL+"/v1/jobs", body)
	wantStatus(t, resp, data, http.StatusMisdirectedRequest)
	if _, ok := resp.Header["X-Goqueue-Leader"]; ok {
		t.Error("leader header set with no leader")
	}

	fb.err = fmt.Errorf("db down: %w", broker.ErrUnavailable)
	resp, data = do(t, "POST", srv.URL+"/v1/jobs", body)
	wantStatus(t, resp, data, http.StatusServiceUnavailable)
	wantErrCode(t, data, "unavailable")

	fb.err = broker.ErrLeaseLost
	resp, data = do(t, "POST", srv.URL+"/v1/jobs/x/ack", map[string]any{"lease_token": 1})
	wantStatus(t, resp, data, http.StatusConflict)

	fb.err = errors.New("kaboom")
	resp, data = do(t, "POST", srv.URL+"/v1/jobs", body)
	wantStatus(t, resp, data, http.StatusInternalServerError)
	wantErrCode(t, data, "internal")
}

func TestHealthReadyClusterExtra(t *testing.T) {
	var readyErr error
	srv := newTestServer(t, brokertest.NewMemBroker(), api.Options{
		Ready:   func() error { return readyErr },
		Cluster: func() any { return map[string]any{"id": "n1", "term": 3, "role": "leader"} },
		Extra: map[string]http.Handler{"/raft/": http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, _ = w.Write([]byte("raft:" + r.URL.Path))
		})},
	})
	resp, data := do(t, "GET", srv.URL+"/healthz", nil)
	wantStatus(t, resp, data, 200)

	resp, data = do(t, "GET", srv.URL+"/readyz", nil)
	wantStatus(t, resp, data, 200)
	readyErr = errors.New("no leader")
	resp, data = do(t, "GET", srv.URL+"/readyz", nil)
	wantStatus(t, resp, data, 503)
	wantErrCode(t, data, "unavailable")

	resp, data = do(t, "GET", srv.URL+"/v1/cluster", nil)
	wantStatus(t, resp, data, 200)
	if !strings.Contains(string(data), `"term":3`) {
		t.Errorf("cluster = %s", data)
	}

	resp, data = do(t, "POST", srv.URL+"/raft/vote", "{}")
	wantStatus(t, resp, data, 200)
	if string(data) != "raft:/raft/vote" {
		t.Errorf("extra = %s", data)
	}
}

func TestMetricsEndpoint(t *testing.T) {
	reg := prometheus.NewRegistry()
	extra := prometheus.NewCounter(prometheus.CounterOpts{Name: "custom_total", Help: "x"})
	reg.MustRegister(extra)
	srv := newTestServer(t, brokertest.NewMemBroker(), api.Options{Registry: reg})
	u := srv.URL

	do(t, "POST", u+"/v1/jobs", map[string]any{"queue": "q", "type": "t"})
	do(t, "POST", u+"/v1/jobs", map[string]any{"queue": "q", "type": "t"})
	_, data := do(t, "POST", u+"/v1/dequeue", map[string]any{"queues": []string{"q"}, "worker_id": "w", "lease_ms": 30000, "max": 2})
	var dq struct{ Jobs []job.Job }
	_ = json.Unmarshal(data, &dq)
	do(t, "POST", u+"/v1/jobs/"+dq.Jobs[0].ID+"/ack", map[string]any{"lease_token": 1})
	do(t, "POST", u+"/v1/jobs/"+dq.Jobs[1].ID+"/nack", map[string]any{"lease_token": 1, "error": "x", "retry": false})

	resp, data := do(t, "GET", u+"/metrics", nil)
	wantStatus(t, resp, data, 200)
	text := string(data)
	for _, want := range []string{
		`goqueue_jobs_enqueued_total{queue="q"} 2`,
		`goqueue_jobs_claimed_total{queue="q"} 2`,
		`goqueue_jobs_acked_total{queue="q"} 1`,
		`goqueue_jobs_nacked_total{queue="q",retry="false"} 1`,
		`goqueue_jobs_dead_total{queue="q"} 1`,
		`goqueue_job_queue_wait_seconds_count{queue="q"} 2`,
		`goqueue_http_request_duration_seconds_count{code="201",method="POST",route="POST /v1/jobs"} 2`,
		`goqueue_queue_depth{queue="q",state="succeeded"} 1`,
		`custom_total`,
	} {
		if !strings.Contains(text, want) {
			t.Errorf("metrics missing %q", want)
		}
	}
}

func TestLongPoll(t *testing.T) {
	b := brokertest.NewMemBroker()
	srv := newTestServer(t, b, api.Options{})
	go func() {
		time.Sleep(200 * time.Millisecond)
		_, _ = b.Enqueue(context.Background(), broker.EnqueueRequest{Queue: "q", Type: "t"})
	}()
	start := time.Now()
	resp, data := do(t, "POST", srv.URL+"/v1/dequeue", map[string]any{"queues": []string{"q"}, "worker_id": "w", "lease_ms": 30000, "wait_ms": 5000})
	wantStatus(t, resp, data, 200)
	var dq struct{ Jobs []job.Job }
	_ = json.Unmarshal(data, &dq)
	if len(dq.Jobs) != 1 || time.Since(start) > 3*time.Second {
		t.Errorf("long poll: %d jobs after %v", len(dq.Jobs), time.Since(start))
	}
}

// End-to-end against Postgres when GOQUEUE_PG_DSN is set.
func TestPostgresEndToEnd(t *testing.T) {
	dsn := os.Getenv("GOQUEUE_PG_DSN")
	if dsn == "" {
		t.Skip("GOQUEUE_PG_DSN not set")
	}
	schema := fmt.Sprintf("api_e2e_%d", time.Now().UnixNano())
	pg, err := pgbroker.New(context.Background(), dsn, pgbroker.Options{Schema: schema})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = pg.Close()
		dropSchema(t, dsn, schema)
	})
	srv := newTestServer(t, pg, api.Options{Ready: func() error { return pg.Ping(context.Background()) }})
	u := srv.URL

	resp, data := do(t, "GET", u+"/readyz", nil)
	wantStatus(t, resp, data, 200)
	resp, data = do(t, "POST", u+"/v1/jobs", map[string]any{"queue": "q", "type": "t", "payload": map[string]any{"n": 1}})
	wantStatus(t, resp, data, 201)
	var j job.Job
	_ = json.Unmarshal(data, &j)

	resp, data = do(t, "POST", u+"/v1/dequeue", map[string]any{"queues": []string{"q"}, "worker_id": "w", "lease_ms": 30000, "wait_ms": 2000})
	wantStatus(t, resp, data, 200)
	var dq struct{ Jobs []job.Job }
	_ = json.Unmarshal(data, &dq)
	if len(dq.Jobs) != 1 || dq.Jobs[0].ID != j.ID {
		t.Fatalf("dequeue = %s", data)
	}
	resp, data = do(t, "POST", u+"/v1/jobs/"+j.ID+"/ack", map[string]any{"lease_token": dq.Jobs[0].LeaseToken})
	wantStatus(t, resp, data, 204)
	resp, data = do(t, "POST", u+"/v1/jobs/"+j.ID+"/ack", map[string]any{"lease_token": dq.Jobs[0].LeaseToken})
	wantStatus(t, resp, data, 409)
	resp, data = do(t, "GET", u+"/v1/jobs/"+j.ID, nil)
	wantStatus(t, resp, data, 200)
	if !strings.Contains(string(data), `"succeeded"`) {
		t.Errorf("get = %s", data)
	}
}
