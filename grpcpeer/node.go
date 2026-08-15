package grpcpeer

import (
	"fmt"
	"github.com/bbb3056666-cyber/distcache/core"
	"log/slog"
	"net"
	"sync"
	"time"
)

const (
	gracefulStopTimeout = time.Second
	shutdownDrainDelay  = defaultHealthCheckInterval + 500*time.Millisecond
)

// Node 封装一个缓存节点所需的 gRPC 传输组件。
type Node struct {
	pool          *Pool
	router        *Router
	broadcaster   *Broadcaster
	healthChecker *HealthChecker
	startedAt     time.Time
	self          string
	server        *Server
	stopOnce      sync.Once
}

// NewNode 创建连接池、路由器、广播器、健康检查器和 gRPC 服务端。
func NewNode(self string, nodes ...string) (*Node, error) {
	pool := NewPool(self)
	err := pool.Set(nodes...)
	if err != nil {
		pool.Close()
		return nil, fmt.Errorf("create pool: %w", err)
	}
	router := NewRouter(pool)
	router.Set(nodes...)
	broadcaster := NewBroadcaster(self, pool)
	healthChecker := NewHealthChecker(self, pool, router)
	return &Node{
		pool:          pool,
		router:        router,
		broadcaster:   broadcaster,
		healthChecker: healthChecker,
		startedAt:     time.Now(),
		self:          self,
		server:        NewServer(),
	}, nil
}

// Serve 启动节点的 gRPC 服务和后台传输任务。
func (n *Node) Serve() error {
	listener, err := net.Listen("tcp", n.self)
	if err != nil {
		return err
	}
	slog.Info(
		"grpc node listening",
		"component", "grpc",
		"address", listener.Addr().String(),
	)
	n.broadcaster.Connect()
	n.healthChecker.Start()
	return n.server.ServeListener(listener)
}

func (n *Node) PickPeer(key string) (core.PeerGetter, bool) {
	return n.router.PickPeer(key)
}

func (n *Node) Broadcast(group, key string) error {
	return n.broadcaster.Broadcast(group, key)
}

func (n *Node) Stop() {
	n.stopOnce.Do(func() {
		slog.Info(
			"grpc node stopping",
			"component", "grpc",
			"address", n.self,
		)
		n.server.SetNotServing()
		time.Sleep(shutdownDrainDelay)
		n.broadcaster.Stop()
		n.healthChecker.Stop()
		n.pool.Close()
		n.server.Shutdown(gracefulStopTimeout)
	})
}

type NodeMetrics struct {
	StartedAtUnix          int64
	ConfiguredNodes        int
	RingNodes              int
	KnownPeers             int
	BroadcastSent          uint64
	BroadcastAcked         uint64
	BroadcastUnacked       uint64
	BroadcastDeferred      uint64
	BroadcastRetried       uint64
	BroadcastDropped       uint64
	BroadcastFailures      uint64
	ActiveBroadcastStreams int
	HealthChecks           uint64
	HealthCheckFailures    uint64
	PeerEjections          uint64
	PeerRecoveries         uint64
}

func (n *Node) Metrics() NodeMetrics {
	startedAtUnix := int64(0)
	if !n.startedAt.IsZero() {
		startedAtUnix = n.startedAt.Unix()
	}

	broadcastStats := n.broadcaster.stats()
	healthStats := n.healthChecker.stats()

	return NodeMetrics{
		StartedAtUnix: startedAtUnix,

		ConfiguredNodes: n.router.ConfiguredNodeCount(),
		RingNodes:       n.router.PeerCount(),
		KnownPeers:      max(n.router.PeerCount()-1, 0),

		BroadcastSent:          broadcastStats.Sent,
		BroadcastAcked:         broadcastStats.Acked,
		BroadcastUnacked:       broadcastStats.Unacked,
		BroadcastDeferred:      broadcastStats.Deferred,
		BroadcastRetried:       broadcastStats.Retried,
		BroadcastDropped:       broadcastStats.Dropped,
		BroadcastFailures:      broadcastStats.Failures,
		ActiveBroadcastStreams: broadcastStats.ActiveStreams,

		HealthChecks:        healthStats.Checks,
		HealthCheckFailures: healthStats.Failures,
		PeerEjections:       healthStats.Ejections,
		PeerRecoveries:      healthStats.Recoveries,
	}
}
