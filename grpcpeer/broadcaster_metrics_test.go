package grpcpeer

import "testing"

func TestBroadcasterActiveStreams(t *testing.T) {
	b := NewBroadcaster("node-a", NewPool("node-a"))
	delivery := &peerDelivery{
		sendCh:      make(chan *Invalidation, 1),
		pendingMsgs: make(map[uint64]*pendingMessage),
	}
	delivery.active.Store(true)
	delivery.addPending(&Invalidation{Id: 1})
	delivery.addPending(&Invalidation{Id: 2})
	b.deliveries["node-b"] = delivery
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

func TestBroadcastRetainsMessageForInactivePeer(t *testing.T) {
	pool := NewPool("node-a")
	b := NewBroadcaster("node-a", pool)
	defer b.Stop()

	healthy := &peerDelivery{sendCh: make(chan *Invalidation, 1), pendingMsgs: make(map[uint64]*pendingMessage)}
	inactive := &peerDelivery{sendCh: make(chan *Invalidation, 1), pendingMsgs: make(map[uint64]*pendingMessage)}
	healthy.active.Store(true)
	b.deliveries["node-b"] = healthy
	b.deliveries["node-c"] = inactive

	if err := b.Broadcast("scores", "tom"); err != nil {
		t.Fatalf("Broadcast() error = %v", err)
	}
	if got := len(healthy.sendCh); got != 1 {
		t.Fatalf("healthy peer messages = %d, want 1", got)
	}
	if got := len(inactive.sendCh); got != 0 {
		t.Fatalf("inactive peer messages = %d, want 0", got)
	}
	if healthy.pendingCount() != 1 || inactive.pendingCount() != 1 {
		t.Fatalf("pending messages = healthy:%d inactive:%d, want 1 and 1", healthy.pendingCount(), inactive.pendingCount())
	}
}
