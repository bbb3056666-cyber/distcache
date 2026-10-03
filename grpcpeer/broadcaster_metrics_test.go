package grpcpeer

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestBroadcasterActiveStreams(t *testing.T) {
	b := NewBroadcaster("node-a", NewPool("node-a"))
	delivery := newPeerDelivery(1)
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

	healthy := newPeerDelivery(1)
	inactive := newPeerDelivery(1)
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

func TestPeerDeliverySignalsOnlyFirstPendingAndAckProgress(t *testing.T) {
	pd := newPeerDelivery(2)
	if !pd.addPending(&Invalidation{Id: 1}) {
		t.Fatal("add first pending message failed")
	}
	select {
	case <-pd.stateCh:
	default:
		t.Fatal("first pending message did not signal watchdog")
	}

	if !pd.addPending(&Invalidation{Id: 2}) {
		t.Fatal("add second pending message failed")
	}
	select {
	case <-pd.stateCh:
		t.Fatal("later pending message unexpectedly reset watchdog")
	default:
	}

	if !pd.ackPending(1) {
		t.Fatal("ack pending message failed")
	}
	select {
	case <-pd.stateCh:
	default:
		t.Fatal("ack progress did not signal watchdog")
	}
}

func TestAckWatchdogCancelsStreamWithoutProgress(t *testing.T) {
	b := NewBroadcaster("node-a", NewPool("node-a"))
	defer b.Stop()
	pd := newPeerDelivery(1)
	pd.addPending(&Invalidation{Id: 1})

	streamCtx, cancelStreamCtx := context.WithCancel(context.Background())
	canceled := make(chan struct{})
	var once sync.Once
	streamCancel := func() {
		once.Do(func() {
			close(canceled)
			cancelStreamCtx()
		})
	}

	go b.watchAckProgressWithTimeout("node-b", pd, streamCtx, streamCancel, 20*time.Millisecond)

	select {
	case <-canceled:
	case <-time.After(time.Second):
		t.Fatal("ack watchdog did not cancel stalled stream")
	}
}

func TestBroadcastRequestsRestartWhenSendQueueIsFull(t *testing.T) {
	b := NewBroadcaster("node-a", NewPool("node-a"))
	defer b.Stop()
	pd := newPeerDelivery(1)
	pd.active.Store(true)
	pd.sendCh <- &Invalidation{Id: 99}
	b.deliveries["node-b"] = pd

	if err := b.Broadcast("scores", "tom"); err == nil {
		t.Fatal("Broadcast() error = nil, want deferred delivery error")
	}
	if got := pd.pendingCount(); got != 1 {
		t.Fatalf("pending messages = %d, want 1", got)
	}
	select {
	case <-pd.restartCh:
	default:
		t.Fatal("full send queue did not request stream restart")
	}
}
