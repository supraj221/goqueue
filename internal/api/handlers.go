package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/supraj221/goqueue/internal/broker"
	"github.com/supraj221/goqueue/internal/job"
)

// ErrorBody is the JSON error envelope.
type ErrorBody struct {
	Error   string `json:"error"`
	Message string `json:"message"`
}

// Request and response bodies. Durations are integer milliseconds.
type (
	enqueueBody struct {
		Queue          string          `json:"queue"`
		Type           string          `json:"type"`
		Payload        json.RawMessage `json:"payload"`
		Priority       int             `json:"priority"`
		RunAt          *time.Time      `json:"run_at"`
		DelayMs        int64           `json:"delay_ms"`
		MaxAttempts    int             `json:"max_attempts"`
		IdempotencyKey string          `json:"idempotency_key"`
	}
	dequeueBody struct {
		Queues   []string `json:"queues"`
		WorkerID string   `json:"worker_id"`
		LeaseMs  int64    `json:"lease_ms"`
		Max      int      `json:"max"`
		WaitMs   int64    `json:"wait_ms"`
	}
	dequeueResponse struct {
		Jobs []job.Job `json:"jobs"`
	}
	heartbeatBody struct {
		LeaseToken uint64 `json:"lease_token"`
		ExtendMs   int64  `json:"extend_ms"`
	}
	ackBody struct {
		LeaseToken uint64 `json:"lease_token"`
	}
	nackBody struct {
		LeaseToken uint64 `json:"lease_token"`
		Error      string `json:"error"`
		Retry      bool   `json:"retry"`
	}
)

func (s *Server) routes() http.Handler {
	mux := http.NewServeMux()
	route := func(pattern string, h http.HandlerFunc) {
		mux.Handle(pattern, s.metrics.Middleware(pattern, s.logRequests(pattern, h)))
	}
	route("POST /v1/jobs", s.handleEnqueue)
	route("GET /v1/jobs/{id}", s.handleGet)
	route("POST /v1/dequeue", s.handleDequeue)
	route("POST /v1/jobs/{id}/heartbeat", s.handleHeartbeat)
	route("POST /v1/jobs/{id}/ack", s.handleAck)
	route("POST /v1/jobs/{id}/nack", s.handleNack)
	route("GET /v1/stats", s.handleStats)
	route("GET /v1/cluster", s.handleCluster)
	route("GET /healthz", s.handleHealthz)
	route("GET /readyz", s.handleReadyz)
	mux.Handle("GET /metrics", s.metrics.Middleware("GET /metrics", promhttpHandler(s)))
	for pattern, h := range s.opts.Extra {
		mux.Handle(pattern, h)
	}
	return mux
}

// logRequests logs one slog line per request.
func (s *Server) logRequests(route string, h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &codeRecorder{ResponseWriter: w}
		h.ServeHTTP(rec, r)
		code := rec.code
		if code == 0 {
			code = http.StatusOK
		}
		level := slogLevel(code)
		s.log.Log(r.Context(), level, "request", "route", route, "method", r.Method, "code", code,
			"dur", time.Since(start), "remote", r.RemoteAddr)
	})
}

type codeRecorder struct {
	http.ResponseWriter
	code int
}

func (r *codeRecorder) WriteHeader(c int) {
	if r.code == 0 {
		r.code = c
	}
	r.ResponseWriter.WriteHeader(c)
}

func (r *codeRecorder) Write(p []byte) (int, error) {
	if r.code == 0 {
		r.code = http.StatusOK
	}
	return r.ResponseWriter.Write(p)
}

func (r *codeRecorder) Unwrap() http.ResponseWriter { return r.ResponseWriter }

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, code int, kind, msg string) {
	writeJSON(w, code, ErrorBody{Error: kind, Message: msg})
}

// writeBrokerErr maps broker errors to the status codes of PLAN.md section 6.
func (s *Server) writeBrokerErr(w http.ResponseWriter, r *http.Request, err error) {
	var nl *broker.NotLeaderError
	switch {
	case errors.As(err, &nl):
		if nl.LeaderAddr != "" {
			w.Header().Set("X-Goqueue-Leader", nl.LeaderAddr)
		}
		writeErr(w, http.StatusMisdirectedRequest, "not_leader", err.Error())
	case errors.Is(err, broker.ErrInvalid):
		writeErr(w, http.StatusBadRequest, "invalid", err.Error())
	case errors.Is(err, broker.ErrNotFound):
		writeErr(w, http.StatusNotFound, "not_found", err.Error())
	case errors.Is(err, broker.ErrLeaseLost):
		writeErr(w, http.StatusConflict, "lease_lost", err.Error())
	case errors.Is(err, broker.ErrUnavailable):
		writeErr(w, http.StatusServiceUnavailable, "unavailable", err.Error())
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		writeErr(w, http.StatusServiceUnavailable, "unavailable", err.Error())
	default:
		s.log.Error("internal error", "path", r.URL.Path, "err", err)
		writeErr(w, http.StatusInternalServerError, "internal", "internal error")
	}
}

// decode reads a JSON body, rejecting unknown fields, trailing data, and
// bodies over 1 MiB. It writes the error response itself and returns false.
func decode(w http.ResponseWriter, r *http.Request, dst any) bool {
	r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		var mbe *http.MaxBytesError
		if errors.As(err, &mbe) {
			writeErr(w, http.StatusRequestEntityTooLarge, "too_large", fmt.Sprintf("request body larger than %d bytes", maxBodyBytes))
			return false
		}
		writeErr(w, http.StatusBadRequest, "invalid", "invalid JSON body: "+err.Error())
		return false
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		var mbe *http.MaxBytesError
		if errors.As(err, &mbe) {
			writeErr(w, http.StatusRequestEntityTooLarge, "too_large", "request body too large")
			return false
		}
		writeErr(w, http.StatusBadRequest, "invalid", "unexpected data after JSON body")
		return false
	}
	return true
}

func ms(v int64) time.Duration { return time.Duration(v) * time.Millisecond }

func (s *Server) handleEnqueue(w http.ResponseWriter, r *http.Request) {
	var body enqueueBody
	if !decode(w, r, &body) {
		return
	}
	req := broker.EnqueueRequest{
		Queue:          body.Queue,
		Type:           body.Type,
		Payload:        body.Payload,
		Priority:       body.Priority,
		Delay:          ms(body.DelayMs),
		MaxAttempts:    body.MaxAttempts,
		IdempotencyKey: body.IdempotencyKey,
	}
	if body.RunAt != nil {
		req.RunAt = *body.RunAt
	}
	res, err := s.b.Enqueue(r.Context(), req)
	if err != nil {
		s.writeBrokerErr(w, r, err)
		return
	}
	code := http.StatusOK
	if res.Created {
		code = http.StatusCreated
		s.metrics.JobsEnqueued(res.Job.Queue)
	}
	writeJSON(w, code, res.Job)
}

func (s *Server) handleGet(w http.ResponseWriter, r *http.Request) {
	j, err := s.b.Get(r.Context(), r.PathValue("id"))
	if err != nil {
		s.writeBrokerErr(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, j)
}

func (s *Server) handleDequeue(w http.ResponseWriter, r *http.Request) {
	var body dequeueBody
	if !decode(w, r, &body) {
		return
	}
	if body.Max == 0 {
		body.Max = 1
	}
	jobs, err := s.b.Dequeue(r.Context(), broker.DequeueRequest{
		Queues:   body.Queues,
		WorkerID: body.WorkerID,
		Lease:    ms(body.LeaseMs),
		Max:      body.Max,
		Wait:     ms(body.WaitMs),
	})
	if err != nil {
		s.writeBrokerErr(w, r, err)
		return
	}
	now := s.now()
	for i := range jobs {
		j := &jobs[i]
		s.metrics.JobsClaimed(j.Queue, 1)
		s.metrics.ObserveQueueWait(j.Queue, now.Sub(j.RunAt))
		s.queues.put(j.ID, j.Queue, now)
	}
	if jobs == nil {
		jobs = []job.Job{}
	}
	writeJSON(w, http.StatusOK, dequeueResponse{Jobs: jobs})
}

func (s *Server) handleHeartbeat(w http.ResponseWriter, r *http.Request) {
	var body heartbeatBody
	if !decode(w, r, &body) {
		return
	}
	j, err := s.b.Heartbeat(r.Context(), r.PathValue("id"), body.LeaseToken, ms(body.ExtendMs))
	if err != nil {
		s.writeBrokerErr(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, j)
}

// queueFor finds the queue of a job being acked or nacked, for metric labels.
func (s *Server) queueFor(ctx context.Context, id string) string {
	if q, ok := s.queues.take(id); ok {
		return q
	}
	if j, err := s.b.Get(ctx, id); err == nil {
		return j.Queue
	}
	return "unknown"
}

func (s *Server) handleAck(w http.ResponseWriter, r *http.Request) {
	var body ackBody
	if !decode(w, r, &body) {
		return
	}
	id := r.PathValue("id")
	if err := s.b.Ack(r.Context(), id, body.LeaseToken); err != nil {
		s.writeBrokerErr(w, r, err)
		return
	}
	s.metrics.JobsAcked(s.queueFor(r.Context(), id))
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleNack(w http.ResponseWriter, r *http.Request) {
	var body nackBody
	if !decode(w, r, &body) {
		return
	}
	id := r.PathValue("id")
	if err := s.b.Nack(r.Context(), id, body.LeaseToken, body.Error, body.Retry); err != nil {
		s.writeBrokerErr(w, r, err)
		return
	}
	q := s.queueFor(r.Context(), id)
	s.metrics.JobsNacked(q, body.Retry)
	if !body.Retry {
		s.metrics.JobsDead(q)
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleStats(w http.ResponseWriter, r *http.Request) {
	st, err := s.b.Stats(r.Context())
	if err != nil {
		s.writeBrokerErr(w, r, err)
		return
	}
	if st.Counts == nil {
		st.Counts = map[string]map[job.State]int{}
	}
	writeJSON(w, http.StatusOK, st)
}

func (s *Server) handleCluster(w http.ResponseWriter, r *http.Request) {
	if s.opts.Cluster == nil {
		writeErr(w, http.StatusNotFound, "not_found", "this backend has no cluster")
		return
	}
	writeJSON(w, http.StatusOK, s.opts.Cluster())
}

func (s *Server) handleHealthz(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) handleReadyz(w http.ResponseWriter, r *http.Request) {
	if s.opts.Ready != nil {
		if err := s.opts.Ready(); err != nil {
			writeErr(w, http.StatusServiceUnavailable, "unavailable", err.Error())
			return
		}
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}
