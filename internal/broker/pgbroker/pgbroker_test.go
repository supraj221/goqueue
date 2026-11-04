package pgbroker

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/supraj221/goqueue/internal/broker"
	"github.com/supraj221/goqueue/internal/broker/brokertest"
)

// testBroker creates a broker in a unique schema and drops it on cleanup.
// Tests are skipped unless GOQUEUE_PG_DSN is set.
func testBroker(tb testing.TB) *Broker {
	tb.Helper()
	dsn := os.Getenv("GOQUEUE_PG_DSN")
	if dsn == "" {
		tb.Skip("GOQUEUE_PG_DSN not set")
	}
	var r [6]byte
	_, _ = rand.Read(r[:])
	schema := "t_" + hex.EncodeToString(r[:])
	ctx := context.Background()
	b, err := New(ctx, dsn, Options{Schema: schema})
	if err != nil {
		tb.Fatalf("New: %v", err)
	}
	tb.Cleanup(func() {
		_ = b.Close()
		conn, err := pgx.Connect(ctx, dsn)
		if err != nil {
			tb.Logf("drop schema connect: %v", err)
			return
		}
		defer func() { _ = conn.Close(ctx) }()
		if _, err := conn.Exec(ctx, "DROP SCHEMA "+pgx.Identifier{schema}.Sanitize()+" CASCADE"); err != nil {
			tb.Logf("drop schema: %v", err)
		}
	})
	return b
}

func TestConformance(t *testing.T) {
	brokertest.Run(t, func(t *testing.T) broker.Broker { return testBroker(t) })
}

func TestConcurrentIdempotentEnqueue(t *testing.T) {
	b := testBroker(t)
	const n = 50
	var wg sync.WaitGroup
	ids := make([]string, n)
	created := make([]bool, n)
	errs := make([]error, n)
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			res, err := b.Enqueue(context.Background(), broker.EnqueueRequest{Queue: "q", Type: "t", IdempotencyKey: "same-key"})
			ids[i], created[i], errs[i] = res.Job.ID, res.Created, err
		}()
	}
	wg.Wait()
	nCreated := 0
	for i := range n {
		if errs[i] != nil {
			t.Fatalf("enqueue %d: %v", i, errs[i])
		}
		if created[i] {
			nCreated++
		}
		if ids[i] != ids[0] {
			t.Fatalf("ids differ: %s vs %s", ids[i], ids[0])
		}
	}
	if nCreated != 1 {
		t.Fatalf("created %d times, want exactly 1", nCreated)
	}
	s, err := b.Stats(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got := s.Counts["q"]["pending"]; got != 1 {
		t.Fatalf("pending = %d, want 1", got)
	}
}

func TestMigrationsIdempotent(t *testing.T) {
	b := testBroker(t)
	// A second broker on the same schema must find migrations already applied.
	var schema string
	if err := b.pool.QueryRow(context.Background(), "SELECT current_schema()").Scan(&schema); err != nil {
		t.Fatal(err)
	}
	b2, err := New(context.Background(), os.Getenv("GOQUEUE_PG_DSN"), Options{Schema: schema})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = b2.Close() }()
	var n int
	if err := b2.pool.QueryRow(context.Background(), "SELECT count(*) FROM schema_migrations").Scan(&n); err != nil || n != 1 {
		t.Fatalf("schema_migrations rows = %d, err = %v", n, err)
	}
}

func BenchmarkPgEnqueue(b *testing.B) {
	br := testBroker(b)
	ctx := context.Background()
	payload := []byte(`{"hello":"world"}`)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, err := br.Enqueue(ctx, broker.EnqueueRequest{Queue: "bench", Type: "t", Payload: payload}); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkPgDequeueAck(b *testing.B) {
	br := testBroker(b)
	ctx := context.Background()
	b.StopTimer()
	var wg sync.WaitGroup
	sem := make(chan struct{}, 16)
	for i := 0; i < b.N; i++ {
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			if _, err := br.Enqueue(ctx, broker.EnqueueRequest{Queue: "bench", Type: "t"}); err != nil {
				b.Error(err)
			}
		}()
	}
	wg.Wait()
	b.StartTimer()
	var done int64
	var mu sync.Mutex
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			js, err := br.Dequeue(ctx, broker.DequeueRequest{Queues: []string{"bench"}, WorkerID: fmt.Sprint("w", time.Now().UnixNano()), Lease: time.Minute, Max: 1})
			if err != nil {
				b.Fatal(err)
			}
			for _, j := range js {
				if err := br.Ack(ctx, j.ID, j.LeaseToken); err != nil {
					b.Fatal(err)
				}
				mu.Lock()
				done++
				mu.Unlock()
			}
		}
	})
}
