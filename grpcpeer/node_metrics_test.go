package grpcpeer

import (
	"context"
	"testing"
	"time"

	"google.golang.org/grpc/health/grpc_health_v1"
)

func TestNodeMetrics(t *testing.T) {
	pool := NewPool("node-a")
	router := NewRouter(pool)
	router.Set("node-a", "node-b", "node-c")
	router.RemovePeer("node-b")

	broadcaster := NewBroadcaster("node-a", pool)
	broadcaster.peerStreams["node-c"] = &peerStream{sendCh: make(chan *Invalidation)}
	broadcaster.metrics.sent.Add(3)
	broadcaster.metrics.acked.Add(1)
	broadcaster.metrics.dropped.Add(2)
	broadcaster.metrics.failures.Add(1)
	health := NewHealthChecker("node-a", pool, router)
	health.metrics.checks.Add(5)
	health.metrics.failures.Add(2)
	health.metrics.ejections.Add(1)
	health.metrics.recoveries.Add(1)

	node := &Node{
		router:        router,
		broadcaster:   broadcaster,
		healthChecker: health,
		startedAt:     time.Unix(1_700_000_000, 0),
	}
	metrics := node.Metrics()

	if metrics.StartedAtUnix != 1_700_000_000 {
		t.Fatalf("StartedAtUnix = %d, want 1700000000", metrics.StartedAtUnix)
	}
	if metrics.ConfiguredNodes != 3 || metrics.RingNodes != 2 || metrics.KnownPeers != 1 {
		t.Fatalf("node counts = %+v, want configured=3 ring=2 known=1", metrics)
	}
	if metrics.BroadcastSent != 3 || metrics.BroadcastAcked != 1 || metrics.BroadcastUnacked != 2 {
		t.Fatalf("broadcast counts = %+v, want sent=3 acked=1 unacked=2", metrics)
	}
	if metrics.BroadcastDropped != 2 || metrics.BroadcastFailures != 1 || metrics.ActiveBroadcastStreams != 1 {
		t.Fatalf("broadcast state = %+v, want dropped=2 failures=1 streams=1", metrics)
	}
	if metrics.HealthChecks != 5 || metrics.HealthCheckFailures != 2 {
		t.Fatalf("health checks = %+v, want checks=5 failures=2", metrics)
	}
	if metrics.PeerEjections != 1 || metrics.PeerRecoveries != 1 {
		t.Fatalf("peer transitions = %+v, want ejections=1 recoveries=1", metrics)
	}
}

func TestNodeStopShutsDownServer(t *testing.T) {
	pool := NewPool("node-a")
	router := NewRouter(pool)
	health := NewHealthChecker("node-a", pool, router)
	server := NewServer()
	node := &Node{
		pool:          pool,
		router:        router,
		broadcaster:   NewBroadcaster("node-a", pool),
		healthChecker: health,
		server:        server,
	}

	node.Stop()

	response, err := server.healthService.Check(context.Background(), &grpc_health_v1.HealthCheckRequest{})
	if err != nil || response.GetStatus() != grpc_health_v1.HealthCheckResponse_NOT_SERVING {
		t.Fatalf("health after Node.Stop = (%v, %v), want NOT_SERVING", response, err)
	}
}
