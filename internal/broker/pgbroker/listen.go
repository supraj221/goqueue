package pgbroker

import (
	"context"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
)

// notifier owns one dedicated connection that LISTENs on the jobs channel and
// fans each notification out to every registered waiter. Waiters also poll,
// so a missed notification (for example during a reconnect) only costs latency.
type notifier struct {
	mu      sync.Mutex
	waiters map[chan struct{}]struct{}
}

func newNotifier() *notifier {
	return &notifier{waiters: map[chan struct{}]struct{}{}}
}

// subscribe registers a wake-up channel; call the returned func to unregister.
func (n *notifier) subscribe() (<-chan struct{}, func()) {
	ch := make(chan struct{}, 1)
	n.mu.Lock()
	n.waiters[ch] = struct{}{}
	n.mu.Unlock()
	return ch, func() {
		n.mu.Lock()
		delete(n.waiters, ch)
		n.mu.Unlock()
	}
}

func (n *notifier) wakeAll() {
	n.mu.Lock()
	for ch := range n.waiters {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
	n.mu.Unlock()
}

// listen runs until ctx is done, reconnecting with a short backoff on errors.
func (b *Broker) listen(ctx context.Context) {
	for ctx.Err() == nil {
		err := b.listenOnce(ctx)
		if ctx.Err() != nil {
			return
		}
		b.log.Warn("pgbroker: listener disconnected", "err", err)
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Second):
		}
	}
}

func (b *Broker) listenOnce(ctx context.Context) error {
	conn, err := pgx.ConnectConfig(ctx, b.connCfg)
	if err != nil {
		return err
	}
	defer func() {
		cctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = conn.Close(cctx)
	}()
	if _, err := conn.Exec(ctx, "LISTEN "+notifyChannel); err != nil {
		return err
	}
	// Anything enqueued before LISTEN took effect is covered by a wake-up.
	b.notify.wakeAll()
	for {
		if _, err := conn.WaitForNotification(ctx); err != nil {
			return err
		}
		b.notify.wakeAll()
	}
}
