package grpcpeer

import "testing"

func TestRouterNodeCounts(t *testing.T) {
	router := NewRouter(NewPool("node-a"))
	router.Set("node-a", "node-b", "node-c")

	if got := router.ConfiguredNodeCount(); got != 3 {
		t.Fatalf("ConfiguredNodeCount() = %d, want 3", got)
	}
	if got := router.PeerCount(); got != 3 {
		t.Fatalf("PeerCount() = %d, want 3", got)
	}

	router.RemovePeer("node-b")
	if got := router.ConfiguredNodeCount(); got != 3 {
		t.Fatalf("ConfiguredNodeCount() after removal = %d, want 3", got)
	}
	if got := router.PeerCount(); got != 2 {
		t.Fatalf("PeerCount() after removal = %d, want 2", got)
	}
}
