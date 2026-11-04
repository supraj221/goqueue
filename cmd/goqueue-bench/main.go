// Command goqueue-bench is the load generator and chaos harness for goqueue.
//
//	goqueue-bench load  --urls http://127.0.0.1:8080 --producers 16 --workers 32 --duration 30s --payload 1024 --backend-label pg
//	goqueue-bench chaos --nodes 3 --binary ./bin/goqueued.exe --duration 120s --kill-every 10s
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"time"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	var err error
	switch os.Args[1] {
	case "load":
		err = cmdLoad(ctx, os.Args[2:])
	case "chaos":
		err = cmdChaos(ctx, os.Args[2:])
	case "-h", "--help", "help":
		usage()
		return
	default:
		usage()
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "goqueue-bench:", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: goqueue-bench <load|chaos> [flags]")
}

func commandLine() string { return "goqueue-bench " + strings.Join(os.Args[1:], " ") }

func addLoadFlags(fs *flag.FlagSet, c *loadCfg) {
	fs.IntVar(&c.producers, "producers", 16, "number of producer goroutines")
	fs.IntVar(&c.workers, "workers", 32, "number of worker goroutines")
	fs.DurationVar(&c.duration, "duration", 30*time.Second, "how long producers run")
	fs.IntVar(&c.payload, "payload", 1024, "approximate payload size in bytes")
	fs.StringVar(&c.queue, "queue", "bench", "queue name")
	fs.DurationVar(&c.lease, "lease", 10*time.Second, "lease duration requested by workers")
	fs.IntVar(&c.batch, "batch", 16, "dequeue batch size")
	fs.DurationVar(&c.drain, "drain", 30*time.Second, "max time to wait for workers to drain the backlog after load stops")
	fs.DurationVar(&c.wait, "wait", time.Second, "long-poll wait per dequeue")
	fs.DurationVar(&c.workTime, "work", 0, "simulated work time per job")
}

func cmdLoad(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("load", flag.ExitOnError)
	var c loadCfg
	addLoadFlags(fs, &c)
	urls := fs.String("urls", "http://127.0.0.1:8080", "comma-separated server base URLs")
	label := fs.String("backend-label", "pg", "label for the backend under test (pg, raft-3, ...)")
	out := fs.String("out", "bench/results", "results directory")
	if err := fs.Parse(args); err != nil {
		return err
	}
	for _, u := range strings.Split(*urls, ",") {
		if u = strings.TrimSpace(u); u != "" {
			c.urls = append(c.urls, strings.TrimRight(u, "/"))
		}
	}
	if len(c.urls) == 0 {
		return fmt.Errorf("--urls is required")
	}

	fmt.Printf("load: %d producers, %d workers, %s, payload %dB against %v\n", c.producers, c.workers, c.duration, c.payload, c.urls)
	s := newLoadState(c)
	s.run(ctx)
	fmt.Println("verifying acknowledged enqueues...")
	vctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	v := s.verify(vctx)

	rep := newReport("load-"+*label, *label, commandLine(), map[string]any{
		"urls": c.urls, "producers": c.producers, "workers": c.workers, "duration": c.duration.String(),
		"payload_bytes": c.payload, "batch": c.batch, "lease": c.lease.String(), "queue": c.queue,
	})
	rep.fill(s, v)
	base, err := rep.write(*out)
	if err != nil {
		return err
	}
	fmt.Print(rep.markdown())
	fmt.Println("results:", base+".json", base+".md")
	if rep.Verify.Lost > 0 {
		return fmt.Errorf("%d acknowledged jobs were lost", rep.Verify.Lost)
	}
	return nil
}
