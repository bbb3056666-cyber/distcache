package grpcpeer

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	sendChannelCapacity = 10
	waitingServerTime   = 5 * time.Second
)

type peerStream struct {
	sendCh chan *Invalidation
}

// Broadcaster 维护到其他节点的失效通知流。
type Broadcaster struct {
	mu          sync.RWMutex
	self        string
	pool        *Pool
	peerStreams map[string]*peerStream
	ctx         context.Context
	cancel      context.CancelFunc
	once        sync.Once
	wg          sync.WaitGroup
	metrics     broadcasterCounters
}

type broadcasterCounters struct {
	sent     atomic.Uint64
	acked    atomic.Uint64
	dropped  atomic.Uint64
	failures atomic.Uint64
}

func NewBroadcaster(self string, pool *Pool) *Broadcaster {
	ctx, cancel := context.WithCancel(context.Background())
	return &Broadcaster{
		self:        self,
		pool:        pool,
		peerStreams: make(map[string]*peerStream),
		ctx:         ctx,
		cancel:      cancel,
	}
}

// Connect 为每个 peer 启动一条可重连的失效通知流。
func (b *Broadcaster) Connect() {
	for _, addr := range b.pool.Addrs() {
		if addr == b.self {
			continue
		}

		b.wg.Add(1)
		go func(addr string) {
			defer b.wg.Done()
			b.monitor(addr)
		}(addr)
	}
}

func (b *Broadcaster) monitor(addr string) {
	for {
		select {
		case <-b.ctx.Done():
			return
		default:
		}

		if b.pool.IsUnhealthy(addr) {
			select {
			case <-time.After(defaultHealthCheckInterval):
			case <-b.ctx.Done():
				return
			}
			continue
		}

		conn, ok := b.pool.Get(addr)
		if !ok {
			slog.Warn(
				"peer connection missing for broadcaster",
				"component", "broadcaster",
				"peer", addr,
			)
			return
		}

		client := NewGroupCacheClient(conn)
		streamCtx, streamCancel := context.WithCancel(context.Background())
		stream, err := client.Invalidate(streamCtx)
		if err != nil {
			b.metrics.failures.Add(1)
			slog.Warn(
				"invalidation stream failed to connect",
				"component", "broadcaster",
				"peer", addr,
				"err", err,
			)
			time.Sleep(defaultHealthCheckInterval)
			streamCancel()
			continue
		}

		ps := &peerStream{sendCh: make(chan *Invalidation, sendChannelCapacity)}
		b.mu.Lock()
		b.peerStreams[addr] = ps
		b.mu.Unlock()

		slog.Info(
			"invalidation stream connected",
			"component", "broadcaster",
			"peer", addr,
		)

		var rwWG sync.WaitGroup
		rwWG.Add(2)

		go func() {
			defer rwWG.Done()
			for {
				select {
				case inv, ok := <-ps.sendCh:
					if !ok {
						streamCancel()
						return
					}
					if err := stream.Send(inv); err != nil {
						b.metrics.failures.Add(1)
						slog.Warn(
							"invalidation writer goroutine exited due to failed send",
							"component", "broadcaster",
							"peer", addr,
							"group", inv.GetGroup(),
							"key", inv.GetKey(),
							"err", err,
						)
						streamCancel()
						return
					}
					b.metrics.sent.Add(1)
				case <-streamCtx.Done():
					slog.Warn(
						"invalidation writer goroutine exited due to streamCancel",
						"component", "broadcaster",
						"peer", addr,
					)
					return
				case <-b.ctx.Done():
					slog.Warn(
						"invalidation writer goroutine exited due to cancel",
						"component", "broadcaster",
						"peer", addr,
					)
					_ = stream.CloseSend()
					go func() {
						select {
						case <-streamCtx.Done():
						case <-time.After(waitingServerTime):
							slog.Warn(
								"waiting server to return time exceeded",
								"component", "broadcaster",
								"timeout", waitingServerTime,
							)
							streamCancel()
						}
					}()
					return
				}
			}
		}()

		go func() {
			defer rwWG.Done()
			for {
				ack, err := stream.Recv()
				if err != nil {
					if !errors.Is(err, io.EOF) && status.Code(err) != codes.Canceled && b.ctx.Err() == nil {
						b.metrics.failures.Add(1)
					}
					slog.Warn(
						"invalidation reader goroutine exited",
						"component", "broadcaster",
						"peer", addr,
						"err", err,
					)
					streamCancel()
					return
				}
				b.metrics.acked.Add(1)
				slog.Debug(
					"invalidation ack received",
					"component", "broadcaster",
					"peer", addr,
					"group", ack.GetGroup(),
					"key", ack.GetKey(),
				)
			}
		}()

		rwWG.Wait()

		b.mu.Lock()
		delete(b.peerStreams, addr)
		b.mu.Unlock()
		if b.ctx.Err() == nil {
			slog.Info(
				"invalidation stream disconnected",
				"component", "broadcaster",
				"peer", addr,
			)
		}
		select {
		case <-b.ctx.Done():
			return
		default:
			time.Sleep(time.Second)
		}
	}
}

// Broadcast 向所有可用 peer 投递失效通知。
func (b *Broadcaster) Broadcast(group, key string) error {
	inv := &Invalidation{Group: group, Key: key}
	b.mu.RLock()
	targets := make(map[string]*peerStream, len(b.peerStreams))
	for addr, ps := range b.peerStreams {
		targets[addr] = ps
	}
	b.mu.RUnlock()

	errs := make([]error, 0, len(targets))
	for addr, ps := range targets {
		if b.pool.IsUnhealthy(addr) {
			continue
		}
		select {
		case ps.sendCh <- inv:
		case <-b.ctx.Done():
			b.metrics.failures.Add(1)
			errs = append(errs, errors.New("broadcast channel closed"))
			return errors.Join(errs...)
		default:
			b.metrics.dropped.Add(1)
			errs = append(errs, errors.New("peer sendCh full, message dropped"))
			slog.Warn(
				"invalidation message dropped",
				"component", "broadcaster",
				"peer", addr,
				"group", group,
				"key", key,
				"reason", "send_queue_full",
			)
		}
	}
	return errors.Join(errs...)
}

type broadcasterStats struct {
	Sent          uint64
	Acked         uint64
	Unacked       uint64
	Dropped       uint64
	Failures      uint64
	ActiveStreams int
}

func (b *Broadcaster) stats() broadcasterStats {
	b.mu.RLock()
	activeStreams := len(b.peerStreams)
	b.mu.RUnlock()

	sent := b.metrics.sent.Load()
	acked := b.metrics.acked.Load()
	unacked := uint64(0)
	if sent > acked {
		unacked = sent - acked
	}

	return broadcasterStats{
		Sent:          sent,
		Acked:         acked,
		Unacked:       unacked,
		Dropped:       b.metrics.dropped.Load(),
		Failures:      b.metrics.failures.Load(),
		ActiveStreams: activeStreams,
	}
}

// Stop 停止所有广播流并等待后台 goroutine 退出。
func (b *Broadcaster) Stop() {
	b.once.Do(func() {
		b.cancel()
		b.wg.Wait()
	})
}
