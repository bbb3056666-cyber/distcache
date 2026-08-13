package grpcpeer

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc/health/grpc_health_v1"
)

const (
	defaultHealthCheckInterval = 3 * time.Second
	defaultFailThreshold       = 3
	defaultSuccessThreshold    = 2
)

var errPeerNotServing = errors.New("grpcpeer: peer is not serving")

type peerState struct {
	healthy bool
	fails   int
	success int
}

// HealthChecker 定期检查节点健康状态，并同步更新哈希环。
type HealthChecker struct {
	self             string
	pool             *Pool
	router           *Router
	interval         time.Duration
	failThreshold    int
	successThreshold int
	states           map[string]*peerState
	ctx              context.Context
	cancel           context.CancelFunc
	once             sync.Once
	wg               sync.WaitGroup
	metrics          healthCounters
}

type healthCounters struct {
	checks     atomic.Uint64
	failures   atomic.Uint64
	ejections  atomic.Uint64
	recoveries atomic.Uint64
}

type healthStats struct {
	Checks     uint64
	Failures   uint64
	Ejections  uint64
	Recoveries uint64
}

func (h *HealthChecker) stats() healthStats {
	return healthStats{
		Checks:     h.metrics.checks.Load(),
		Failures:   h.metrics.failures.Load(),
		Ejections:  h.metrics.ejections.Load(),
		Recoveries: h.metrics.recoveries.Load(),
	}
}

func NewHealthChecker(self string, pool *Pool, router *Router) *HealthChecker {
	ctx, cancel := context.WithCancel(context.Background())
	return &HealthChecker{
		self:             self,
		pool:             pool,
		router:           router,
		interval:         defaultHealthCheckInterval,
		failThreshold:    defaultFailThreshold,
		successThreshold: defaultSuccessThreshold,
		states:           make(map[string]*peerState),
		ctx:              ctx,
		cancel:           cancel,
	}
}

// Start 启动后台健康检查循环。
func (h *HealthChecker) Start() {
	h.wg.Add(1)
	go func() {
		defer h.wg.Done()
		ticker := time.NewTicker(h.interval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				h.scan()
			case <-h.ctx.Done():
				return
			}
		}
	}()
}

func (h *HealthChecker) scan() {
	for _, addr := range h.pool.Addrs() {
		if addr == h.self {
			continue
		}
		st := h.states[addr]
		if st == nil {
			st = &peerState{healthy: true}
			h.states[addr] = st
		}
		h.metrics.checks.Add(1)
		h.handleCheckResult(addr, st, h.checkPeer(addr))
	}
}

func (h *HealthChecker) checkPeer(addr string) error {
	conn, ok := h.pool.Get(addr)
	if !ok {
		return fmt.Errorf("no connection to %s", addr)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	client := grpc_health_v1.NewHealthClient(conn)
	resp, err := client.Check(ctx, &grpc_health_v1.HealthCheckRequest{})
	if err != nil {
		return err
	}
	if resp.GetStatus() != grpc_health_v1.HealthCheckResponse_SERVING {
		return fmt.Errorf("%w: %s status=%v", errPeerNotServing, addr, resp.GetStatus())
	}
	return nil
}

func (h *HealthChecker) handleCheckResult(addr string, st *peerState, err error) {
	if err == nil {
		h.onSuccess(addr, st)
		return
	}

	h.metrics.failures.Add(1)
	if errors.Is(err, errPeerNotServing) {
		if h.ejectPeer(addr, st) {
			slog.Warn(
				"peer ejected",
				"component", "health",
				"peer", addr,
				"reason", "not_serving",
			)
		}
		return
	}
	h.onFailure(addr, st, err)
}

func (h *HealthChecker) ejectPeer(addr string, st *peerState) bool {
	if !st.healthy {
		return false
	}
	st.healthy = false
	st.fails = 0
	st.success = 0
	h.pool.MarkUnhealthy(addr)
	h.router.RemovePeer(addr)
	h.metrics.ejections.Add(1)
	return true
}

func (h *HealthChecker) onFailure(addr string, st *peerState, err error) {
	if !st.healthy {
		return
	}
	st.fails++
	st.success = 0
	if st.fails >= h.failThreshold {
		fails := st.fails
		if h.ejectPeer(addr, st) {
			slog.Warn(
				"peer ejected",
				"component", "health",
				"peer", addr,
				"reason", "failure_threshold",
				"fail_count", fails,
				"fail_threshold", h.failThreshold,
				"last_error", err,
			)
		}
	}
}

func (h *HealthChecker) onSuccess(addr string, st *peerState) {
	if st.healthy {
		st.fails = 0
		return
	}
	st.success++
	st.fails = 0
	if st.success >= h.successThreshold {
		slog.Info(
			"peer recovered",
			"component", "health",
			"peer", addr,
			"success_count", st.success,
			"success_threshold", h.successThreshold,
		)
		st.healthy = true
		st.success = 0
		h.pool.MarkHealthy(addr)
		h.router.AddPeer(addr)
		h.metrics.recoveries.Add(1)
	}
}

// Stop 停止健康检查循环。
func (h *HealthChecker) Stop() {
	h.once.Do(func() {
		h.cancel()
		h.wg.Wait()
	})
}
