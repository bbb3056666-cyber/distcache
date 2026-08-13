package grpcpeer

import "testing"

func TestBroadcasterActiveStreams(t *testing.T) {
	b := NewBroadcaster("node-a", NewPool("node-a"))
	b.peerStreams["node-b"] = &peerStream{sendCh: make(chan *Invalidation)}
	b.metrics.sent.Add(3)
	b.metrics.acked.Add(1)

	stats := b.stats()
	if got := stats.ActiveStreams; got != 1 {
		t.Fatalf("ActiveStreams = %d, want 1", got)
	}
	if got := stats.Unacked; got != 2 {
		t.Fatalf("Unacked = %d, want 2", got)
	}
}

func TestBroadcastSkipsUnhealthyPeer(t *testing.T) {
	pool := NewPool("node-a")
	b := NewBroadcaster("node-a", pool)
	defer b.Stop()

	healthy := &peerStream{sendCh: make(chan *Invalidation, 1)}
	unhealthy := &peerStream{sendCh: make(chan *Invalidation, 1)}
	b.peerStreams["node-b"] = healthy
	b.peerStreams["node-c"] = unhealthy
	pool.MarkUnhealthy("node-c")

	if err := b.Broadcast("scores", "tom"); err != nil {
		t.Fatalf("Broadcast() error = %v", err)
	}
	if got := len(healthy.sendCh); got != 1 {
		t.Fatalf("healthy peer messages = %d, want 1", got)
	}
	if got := len(unhealthy.sendCh); got != 0 {
		t.Fatalf("unhealthy peer messages = %d, want 0", got)
	}
}
