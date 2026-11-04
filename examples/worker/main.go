// Command worker is a demo goqueue worker that handles "email.send" jobs.
//
//	go run ./examples/worker --url http://127.0.0.1:8080 --queues default
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"log/slog"
	"math/rand/v2"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/supraj221/goqueue/client"
	"github.com/supraj221/goqueue/internal/job"
)

func main() {
	url := flag.String("url", "http://127.0.0.1:8080", "goqueued base URL(s), comma separated")
	queues := flag.String("queues", "default", "queues to consume, comma separated")
	concurrency := flag.Int("concurrency", 10, "jobs to run at once")
	flag.Parse()

	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	c := client.New(strings.Split(*url, ","))
	w := client.NewWorker(c, client.WorkerOptions{
		Queues:      strings.Split(*queues, ","),
		Concurrency: *concurrency,
		Logger:      log,
	})

	w.Handle("email.send", func(ctx context.Context, j job.Job) error {
		var p struct {
			To      string `json:"to"`
			Subject string `json:"subject"`
		}
		if len(j.Payload) > 0 {
			if err := json.Unmarshal(j.Payload, &p); err != nil {
				return client.Permanent(err) // retrying a malformed payload is pointless
			}
		}
		// Pretend to talk to an SMTP server.
		select {
		case <-time.After(time.Duration(10+rand.IntN(41)) * time.Millisecond):
		case <-ctx.Done():
			return ctx.Err()
		}
		if rand.Float64() < 0.10 {
			return errors.New("smtp: temporary failure")
		}
		log.Info("sent email", "job", j.ID, "to", p.To, "subject", p.Subject, "attempt", j.Attempts)
		return nil
	})

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	log.Info("worker started", "id", w.ID(), "url", *url, "queues", *queues)
	if err := w.Run(ctx); err != nil {
		log.Error("worker stopped", "err", err)
		os.Exit(1)
	}
	log.Info("worker stopped cleanly")
}
