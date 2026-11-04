package memnet_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/supraj221/goqueue/internal/raft"
	"github.com/supraj221/goqueue/internal/raft/memnet"
)

type echo struct{}

func (echo) HandleRequestVote(_ context.Context, r *raft.RequestVoteRequest) (*raft.RequestVoteResponse, error) {
	return &raft.RequestVoteResponse{Term: r.Term, VoteGranted: true}, nil
}
func (echo) HandleAppendEntries(_ context.Context, r *raft.AppendEntriesRequest) (*raft.AppendEntriesResponse, error) {
	return &raft.AppendEntriesResponse{Term: r.Term, Success: true}, nil
}
func (echo) HandleInstallSnapshot(_ context.Context, r *raft.InstallSnapshotRequest) (*raft.InstallSnapshotResponse, error) {
	return &raft.InstallSnapshotResponse{Term: r.Term}, nil
}

func TestReachability(t *testing.T) {
	n := memnet.New()
	for _, id := range []string{"a", "b", "c"} {
		n.Register(id, echo{})
	}
	call := func(from, to string) error {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_, err := n.Transport(from).AppendEntries(ctx, to, &raft.AppendEntriesRequest{Term: 1})
		return err
	}
	cases := []struct {
		name  string
		setup func()
		from  string
		to    string
		ok    bool
	}{
		{"connected", func() {}, "a", "b", true},
		{"disconnected destination", func() { n.Disconnect("b") }, "a", "b", false},
		{"disconnected source", func() {}, "b", "a", false},
		{"reconnected", func() { n.Connect("b") }, "a", "b", true},
		{"partition same side", func() { n.Partition([]string{"a", "b"}, []string{"c"}) }, "a", "b", true},
		{"partition across", func() {}, "a", "c", false},
		{"partition across reverse", func() {}, "c", "b", false},
		{"healed", func() { n.Heal() }, "a", "c", true},
		{"unregistered", func() { n.Unregister("c") }, "a", "c", false},
	}
	for _, tc := range cases {
		tc.setup()
		err := call(tc.from, tc.to)
		if (err == nil) != tc.ok {
			t.Errorf("%s: err = %v, want ok=%v", tc.name, err, tc.ok)
		}
	}
}

func TestDropAndDelay(t *testing.T) {
	n := memnet.New()
	n.Register("a", echo{})
	n.Register("b", echo{})
	tr := n.Transport("a")

	n.SetDropRate(1)
	_, err := tr.RequestVote(context.Background(), "b", &raft.RequestVoteRequest{Term: 1})
	if !errors.Is(err, memnet.ErrDropped) {
		t.Fatalf("drop rate 1: err = %v, want ErrDropped", err)
	}

	n.SetDropRate(0)
	n.SetDelay(20*time.Millisecond, 21*time.Millisecond)
	start := time.Now()
	if _, err := tr.RequestVote(context.Background(), "b", &raft.RequestVoteRequest{Term: 1}); err != nil {
		t.Fatal(err)
	}
	if d := time.Since(start); d < 35*time.Millisecond {
		t.Fatalf("round trip took %v, want about 40ms (two delayed legs)", d)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
	defer cancel()
	if _, err := tr.RequestVote(ctx, "b", &raft.RequestVoteRequest{Term: 1}); err == nil {
		t.Fatal("context deadline should abort a delayed RPC")
	}
}
