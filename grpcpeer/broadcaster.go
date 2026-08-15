package grpcpeer

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	sendChannelCapacity = 10
	waitingServerTime   = 5 * time.Second
	ackTimeout          = 3 * time.Second
	retryScanInterval   = 1 * time.Second
	maxPendingPerPeer   = 10_000
)

// Broadcaster 实现 core.Broadcaster
type Broadcaster struct {
	mu         sync.RWMutex
	self       string
	pool       *Pool
	deliveries map[string]*peerDelivery // 储存对应流的channel,只能往channel里发消息
	ctx        context.Context          // 全局 Context
	cancel     context.CancelFunc       // Stop 用
	once       sync.Once                // 保证Stop()只被执行一次
	wg         sync.WaitGroup           // 管理所有 monitor goroutine
	nextID     atomic.Uint64            // 为每次广播分配进程内单调递增的消息ID
	metrics    broadcasterCounters      //统计数据
}

type broadcasterCounters struct {
	sent     atomic.Uint64 // 成功写进 gRPC 流的消息数
	acked    atomic.Uint64 // 收到远端确认的消息数
	deferred atomic.Uint64 // 消息在本地发送队列满时，被推迟发送的次数
	retried  atomic.Uint64 // 消息重发次数
	dropped  atomic.Uint64 // 消息丢失次数
	failures atomic.Uint64 // 发送过程中发生错误的次数
}

func NewBroadcaster(self string, pool *Pool) *Broadcaster {
	ctx, cancel := context.WithCancel(context.Background())
	return &Broadcaster{
		self:       self,
		pool:       pool,
		deliveries: make(map[string]*peerDelivery),
		ctx:        ctx,
		cancel:     cancel,
	}
}

// Connect
// 对每个 peer 启动一个monitor goroutine,负责stream的重连和RW goroutine 的管理
func (b *Broadcaster) Connect() {
	for _, addr := range b.pool.Addrs() {
		if addr == b.self {
			//Addrs()里不可能有self,冗余防御
			continue
		}

		pd := &peerDelivery{
			sendCh:      make(chan *Invalidation, sendChannelCapacity),
			pendingMsgs: make(map[uint64]*pendingMessage),
		}
		b.mu.Lock()
		b.deliveries[addr] = pd
		b.mu.Unlock()

		b.wg.Add(2)

		go func(addr string, pd *peerDelivery) {
			defer b.wg.Done()
			b.monitor(addr, pd)
		}(addr, pd)

		//消息重发协程
		go func(addr string, pd *peerDelivery) {
			defer b.wg.Done()
			pd.runRetryLoop(addr, b)
		}(addr, pd)
	}
}

// monitor 维护到某个 peer 的流，断了自动重连
func (b *Broadcaster) monitor(addr string, pd *peerDelivery) {
	//外层: 重连循环
	for {
		//关流后 Recv 返回err=io.EOF → 新一轮循环 → 检查 done → 退出
		select {
		case <-b.ctx.Done():
			return
		default:
		}
		conn, ok := b.pool.Get(addr) // 复用 Pool 的TCP连接
		if !ok {
			//压根没有这个gRPC连接
			slog.Warn(
				"peer connection missing for broadcaster",
				"component", "broadcaster",
				"peer", addr,
			)
			return
		}
		//创建关于本"流"的上下文
		streamCtx, streamCancel := context.WithCancel(context.Background())
		stream, replay, ok := b.buildStream(streamCtx, streamCancel, addr, conn, pd)
		if !ok {
			continue
		}
		pd.active.Store(true)

		var RWwg sync.WaitGroup
		RWwg.Add(2)

		// writer goroutine: 只管发送
		// 只有这一个 goroutine 会串行调用 stream.Send,避免Send并发问题
		go func() {
			defer RWwg.Done()
			b.runWriter(addr, pd, stream, streamCtx, streamCancel, replay)
		}()

		// reader goroutine: 只管接收
		go func() {
			defer RWwg.Done()
			b.runReader(addr, pd, stream, streamCancel)
		}()

		RWwg.Wait()
		pd.active.Store(false)

		//非Stop出现的退出,就是异常
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

func (b *Broadcaster) buildStream(
	streamCtx context.Context,
	streamCancel context.CancelFunc,
	addr string,
	conn *grpc.ClientConn,
	pd *peerDelivery,
) (
	GroupCache_InvalidateClient,
	[]*Invalidation,
	bool,
) {
	//先检查节点是否被摘除,被摘除则3s后再查
	if b.pool.IsUnhealthy(addr) {
		select {
		case <-time.After(defaultHealthCheckInterval):
		case <-b.ctx.Done():
		}
		streamCancel()
		return nil, nil, false
	}

	//创建客户端桩
	client := NewGroupCacheClient(conn)

	//返回一个"能发能收"流对象( 一条 HTTP/2 流的"gRPC封装"),存进map,后续往里发送消息
	//gRPC层: stream对象
	//传输层: HTTP/2 流
	//第一次发起rpc：随机端口被分配给客户端,建立 TCP,HTTP/2 连接,后面再使用这个conn直接复用同一条tcp
	stream, err := client.Invalidate(streamCtx)

	//创建流失败
	if err != nil {
		b.metrics.failures.Add(1)
		slog.Warn(
			"invalidation stream failed to connect",
			"component", "broadcaster",
			"peer", addr,
			"err", err,
		)
		select {
		case <-time.After(defaultHealthCheckInterval):
		case <-b.ctx.Done():
		}
		streamCancel()
		return nil, nil, false
	}
	// 必须在peerStream对外可见前拍快照，否则新消息可能同时进入sendCh和重发列表，造成重复发送
	replay := pd.pendingForPeer()
	//清空sendCh里的消息,避免重复发送
	for ok := true; ok; {
		select {
		case <-pd.sendCh:
		default:
			ok = false
		}
	}

	//流建立成功
	slog.Info(
		"invalidation stream connected",
		"component", "broadcaster",
		"peer", addr,
	)
	return stream, replay, true
}

func (b *Broadcaster) runWriter(
	addr string,
	pd *peerDelivery,
	stream GroupCache_InvalidateClient,
	streamCtx context.Context,
	streamCancel context.CancelFunc,
	replay []*Invalidation,
) {
	// 重连后优先补发上一条流中尚未收到ACK的消息
	for _, inv := range replay {
		//replay响应stop
		select {
		case <-b.ctx.Done():
			streamCancel()
			return
		default:
		}
		if !pd.send(addr, b, inv, stream) {
			streamCancel()
			return
		}
	}

	for {
		select {
		// 串行取消息Send,绝不并发
		case inv, ok := <-pd.sendCh:
			if !ok {
				//管道异常关闭,重新连接
				streamCancel()
				return
			}
			//Send方法不是并发安全的,所有首次发送和重发都由当前writer串行执行
			if !pd.send(addr, b, inv, stream) {
				// writer先发现这条流出现问题,直接streamCancel
				// 通知reader(status.Code(err) == codes.Canceled)
				streamCancel()
				return
			}

		//reader发现流出现问题调用streamCancel
		//writer直接退出,不能只靠Send返回错误发现连接断开
		case <-streamCtx.Done():
			slog.Warn(
				"invalidation writer goroutine exited due to streamCancel",
				"component", "broadcaster",
				"peer", addr,
			)
			return

		case <-b.ctx.Done(): // 直接响应全局停止信号
			slog.Warn(
				"invalidation writer goroutine exited due to cancel",
				"component", "broadcaster",
				"peer", addr,
			)
			//关闭流
			_ = stream.CloseSend()
			// 不立刻 streamCancel，给 reader 一个机会正常收到 EOF
			// 但也不能无限等，给个超时兜底
			go func() {
				select {
				case <-streamCtx.Done():
					// reader 那边正常走完了，streamCtx 已经被 cancel 了
					//（因为 reader 收到 EOF 后会自己 streamCancel）
				case <-time.After(waitingServerTime):
					// 5 秒了还没反应，说明服务端不配合，强制关闭流
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
}

func (b *Broadcaster) runReader(
	addr string,
	pd *peerDelivery,
	stream GroupCache_InvalidateClient,
	streamCancel context.CancelFunc,
) {
	//常驻读,直到流断
	for {
		// 阻塞等流断开（Recv 同时丢掉 Ack,防止堆积,堵死服务端+ 检测断线）
		ack, err := stream.Recv()
		//服务端返回nil时,发送一条err=io.EOF的消息
		//如果status.Code(err) == codes.Canceled,就是暴力关闭,直接丢掉还未处理的数据
		if err != nil {
			if !errors.Is(err, io.EOF) && status.Code(err) != codes.Canceled && b.ctx.Err() == nil {
				// errors.Is(err, io.EOF) -> 服务端已经正常关闭流,用EOF通知客户端
				// status.Code(err) == codes.Canceled -> streamCancel被调用了 -> 服务端不配合,5s超时streamCancel兜底
				// b.ctx.Err() != nil -> Stop被调用了,客户端正常关闭
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

		// err==nil：收到一条 Ack → 处理 → 继续内层 Recv（等下一条）
		// 只有匹配到待确认消息的ACK才计数；重复或未知ACK不重复确认
		if !pd.ackPending(ack.GetId()) {
			// 重复、未知或过期 ACK，忽略
			slog.Warn(
				"unknown invalidation ack ignored",
				"component", "broadcaster",
				"peer", addr,
				"id", ack.GetId(),
			)
			continue
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
}

// Broadcast 往所有流的 Send 推一条失效消息
// rpc Invalidate(stream Invalidation) returns (stream Ack);
func (b *Broadcaster) Broadcast(group, key string) error {
	select {
	case <-b.ctx.Done():
		b.metrics.failures.Add(1)
		return errors.New("broadcaster stopped")
	default:
	}
	//失效消息
	inv := &Invalidation{Id: b.nextID.Add(1), Group: group, Key: key}
	b.mu.RLock()
	targets := make(map[string]*peerDelivery, len(b.deliveries))
	for addr, pd := range b.deliveries {
		targets[addr] = pd
	}
	b.mu.RUnlock()

	errs := make([]error, 0, len(targets))
	for addr, pd := range targets {
		// 增加一条待发消息的记录,当队列满时丢弃或者未收到确认消息或流中断后重发
		if !pd.addPending(inv) {
			b.metrics.dropped.Add(1)
			errs = append(errs, errors.New("peer pending limit reached"))
			slog.Warn(
				"invalidation message dropped",
				"component", "broadcaster",
				"peer", addr,
				"id", inv.GetId(),
				"group", group,
				"key", key,
				"reason", "pending_limit_reached",
			)
			continue
		}
		select {
		case <-b.ctx.Done():
			pd.removePending(inv.GetId())
			b.metrics.failures.Add(1)
			errs = append(errs, errors.New("broadcast channel closed"))
			return errors.Join(errs...)
		default:

			if !pd.active.Load() {
				//流中断了,就不往管道里发了
				continue
			}

			if !pd.tryEnqueue(inv.GetId()) {
				//管道满了,推迟发送
				b.metrics.deferred.Add(1)
				errs = append(errs, errors.New("peer sendCh full, message deferred"))
				slog.Warn(
					"invalidation message deferred",
					"component", "broadcaster",
					"peer", addr,
					"id", inv.GetId(),
					"group", group,
					"key", key,
					"reason", "send_queue_full",
				)
			}
		}
	}
	return errors.Join(errs...)
}

func (b *Broadcaster) pendingCount() int {
	b.mu.RLock()
	deliveries := make([]*peerDelivery, 0, len(b.deliveries))
	for _, pd := range b.deliveries {
		deliveries = append(deliveries, pd)
	}
	b.mu.RUnlock()

	total := 0
	for _, pd := range deliveries {
		total += pd.pendingCount()
	}
	return total
}

type broadcasterStats struct {
	Sent          uint64 // 成功写进 gRPC 流的消息数
	Acked         uint64 // 收到远端确认的消息数
	Unacked       uint64 // 当前待发送或待确认的消息数
	Deferred      uint64 // 消息在本地发送队列满时，被推迟发送的次数
	Retried       uint64 // 消息重试次数
	Dropped       uint64 // 消息丢失次数
	Failures      uint64 // 发送过程中发生错误的次数
	ActiveStreams int    // 此刻可以被投递失效消息的流数量
}

func (b *Broadcaster) stats() broadcasterStats {
	activeStreams := b.activeStreamCount()
	sent := b.metrics.sent.Load()
	acked := b.metrics.acked.Load()
	return broadcasterStats{
		Sent:          sent,
		Acked:         acked,
		Unacked:       uint64(b.pendingCount()),
		Deferred:      b.metrics.deferred.Load(),
		Retried:       b.metrics.retried.Load(),
		Dropped:       b.metrics.dropped.Load(),
		Failures:      b.metrics.failures.Load(),
		ActiveStreams: activeStreams,
	}
}

// activeStreamCount 统计活着的流数
func (b *Broadcaster) activeStreamCount() int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	count := 0
	for _, d := range b.deliveries {
		if d.active.Load() {
			count++
		}
	}
	return count
}

// Stop 优雅关闭：关所有流 + 通知 monitor 退出,Connect()前调用没事
func (b *Broadcaster) Stop() {
	b.once.Do(func() {
		b.cancel()  //给monitor里的b.ctx.Done()发信号,下一次循环退出
		b.wg.Wait() // 等所有 monitor goroutine(内部又各自等了它的 reader/writer)彻底退出
	})
}

// 客户端 CloseSend :往流里发送一条消息,err=io.EOF
// 客户端stream.CloseSend() 只关闭"客户端→服务端"这个方向,客户端依然能继续Recv(),服务端依然能继续Send()
// 服务端handler return(不管nil还是err)	关闭整个Stream,两个方向都结束,客户端再Send()会直接失败

// 不读 Ack（不 Recv）→ 通道堵死
// 服务端每处理一条就发一条 Ack
// → 客户端【从不 Recv】→ Ack 堆积在客户端的接收缓冲
// → gRPC 流控：接收窗口满了
// → 服务端的 Send(Ack) 【阻塞】
// → 服务端 Invalidate 循环卡在 Send(Ack)
// → 收不到下一条失效 → 处理停了
// → 客户端的 Send(失效) 也堵 → 广播整个废掉
//最终：广播通道死锁
