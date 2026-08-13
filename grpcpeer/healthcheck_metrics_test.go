package grpcpeer

import "testing"

func TestHealthCheckerTransitionMetrics(t *testing.T) {
	pool := NewPool("node-a")
	router := NewRouter(pool)
	router.Set("node-a", "node-b")
	health := NewHealthChecker("node-a", pool, router)
	health.failThreshold = 1
	health.successThreshold = 1

	state := &peerState{healthy: true}
	health.onFailure("node-b", state, nil)
	if got := health.stats().Ejections; got != 1 {
		t.Fatalf("Ejections = %d, want 1", got)
	}
	if got := router.PeerCount(); got != 1 {
		t.Fatalf("PeerCount() after ejection = %d, want 1", got)
	}
	if !pool.IsUnhealthy("node-b") {
		t.Fatal("node-b should be marked unhealthy after ejection")
	}

	health.onSuccess("node-b", state)
	if got := health.stats().Recoveries; got != 1 {
		t.Fatalf("Recoveries = %d, want 1", got)
	}
	if got := router.PeerCount(); got != 2 {
		t.Fatalf("PeerCount() after recovery = %d, want 2", got)
	}
	if pool.IsUnhealthy("node-b") {
		t.Fatal("node-b should be marked healthy after recovery")
	}
}

func TestNotServingEjectsImmediately(t *testing.T) {
	pool := NewPool("node-a")
	router := NewRouter(pool)
	router.Set("node-a", "node-b")
	health := NewHealthChecker("node-a", pool, router)
	health.failThreshold = 3

	health.handleCheckResult("node-b", &peerState{healthy: true}, errPeerNotServing)

	stats := health.stats()
	if stats.Failures != 1 || stats.Ejections != 1 {
		t.Fatalf("health stats = %+v, want failures=1 ejections=1", stats)
	}
	if got := router.PeerCount(); got != 1 {
		t.Fatalf("PeerCount() after NOT_SERVING = %d, want 1", got)
	}
}
