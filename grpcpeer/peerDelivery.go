package grpcpeer

import (
	"log/slog"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// peerDelivery 专门管理一个节点的消息
type peerDelivery struct {
	sendCh      chan *Invalidation         // 外部只能往这里塞消息，自己不碰 stream
	pendingMu   sync.Mutex                 // 每个节点一把锁
	pendingMsgs map[uint64]*pendingMessage // 这个peer尚未收到ACK的消息
	active      atomic.Bool                // 流是否活着
}

type pendingMessage struct {
	inv        *Invalidation //失效消息
	lastSentAt time.Time     //最近一次Send时间
	scheduled  bool          //是否在调度
	retryCount int           //重试过多少次
}

func (pd *peerDelivery) finishSend(id uint64, success bool) {
	pd.pendingMu.Lock()
	defer pd.pendingMu.Unlock()

	pendingMsg := pd.pendingMsgs[id]
	if pendingMsg == nil {
		// ACK 可能已经先到并删除了 pending
		return
	}

	pendingMsg.scheduled = false
	if success {
		pendingMsg.lastSentAt = time.Now()
	}
}

func (pd *peerDelivery) tryEnqueue(id uint64) bool {
	pd.pendingMu.Lock()
	defer pd.pendingMu.Unlock()

	pendingMsg := pd.pendingMsgs[id]
	if pendingMsg == nil {
		return false
	}

	if pendingMsg.scheduled {
		return true
	}

	select {
	// 只往channel发送消息,并发安全
	// 不调用Send,因为一个流被同时Send会panic
	// Send的任务交给另一个goroutine串行执行
	case pd.sendCh <- pendingMsg.inv:
		pendingMsg.scheduled = true
		return true
	default:
		return false
	}
}

func (pd *peerDelivery) runRetryLoop(addr string, b *Broadcaster) {
	ticker := time.NewTicker(retryScanInterval)
	defer ticker.Stop()

	for {
		select {
		case now := <-ticker.C:
			retried := pd.retryExpired(now)
			if retried == 0 {
				continue
			}

			b.metrics.retried.Add(uint64(retried))
			slog.Debug(
				"invalidation messages scheduled for retry",
				"component", "broadcaster",
				"peer", addr,
				"count", retried,
			)

		case <-b.ctx.Done():
			return
		}
	}
}

func (pd *peerDelivery) retryExpired(now time.Time) int {
	if !pd.active.Load() {
		return 0
	}

	pd.pendingMu.Lock()
	defer pd.pendingMu.Unlock()

	retried := 0

	for _, pendingMsg := range pd.pendingMsgs {
		// 已经在队列中或正在发送，不能重复调度
		if pendingMsg.scheduled {
			continue
		}

		// 发送过并且尚未超时，继续等待 ACK
		if !pendingMsg.lastSentAt.IsZero() &&
			now.Sub(pendingMsg.lastSentAt) < ackTimeout {
			continue
		}

		select {
		case pd.sendCh <- pendingMsg.inv:
			pendingMsg.scheduled = true
			pendingMsg.retryCount++
			retried++
		default:
			// 队列已经满了，继续遍历也没有意义
			return retried
		}
	}

	return retried
}

func (pd *peerDelivery) pendingCount() int {
	pd.pendingMu.Lock()
	defer pd.pendingMu.Unlock()
	return len(pd.pendingMsgs)
}

// pendingForPeer 返回某个节点未收到ack的消息,按id升序
func (pd *peerDelivery) pendingForPeer() []*Invalidation {
	pd.pendingMu.Lock()
	defer pd.pendingMu.Unlock()

	messages := make([]*Invalidation, 0, len(pd.pendingMsgs))
	for _, pendingMsg := range pd.pendingMsgs {
		pendingMsg.scheduled = true
		messages = append(messages, pendingMsg.inv)
	}
	sort.Slice(messages, func(i, j int) bool {
		return messages[i].GetId() < messages[j].GetId()
	})
	return messages
}

func (pd *peerDelivery) addPending(inv *Invalidation) bool {
	pd.pendingMu.Lock()
	defer pd.pendingMu.Unlock()

	if len(pd.pendingMsgs) >= maxPendingPerPeer {
		return false
	}

	pd.pendingMsgs[inv.GetId()] = &pendingMessage{inv: inv}
	return true
}

func (pd *peerDelivery) ackPending(id uint64) bool {
	return pd.removePending(id)
}
func (pd *peerDelivery) removePending(id uint64) bool {
	pd.pendingMu.Lock()
	defer pd.pendingMu.Unlock()

	//nil map 可以查、可以遍历、可以 len、可以 delete，但不能直接赋值写入
	if _, ok := pd.pendingMsgs[id]; !ok {
		// 找不到对应的待确认消息，可能是重复、未知或过期 ACK
		return false
	}
	delete(pd.pendingMsgs, id)
	return true
}

func (pd *peerDelivery) send(
	addr string,
	b *Broadcaster,
	inv *Invalidation,
	stream GroupCache_InvalidateClient,
) bool {
	//Send方法不是并发安全的,两个goroutine同时对一个流Send会panic
	if err := stream.Send(inv); err != nil {
		pd.finishSend(inv.GetId(), false)
		b.metrics.failures.Add(1)
		slog.Warn(
			"invalidation writer goroutine exited due to failed send ",
			"component", "broadcaster",
			"peer", addr,
			"id", inv.GetId(),
			"group", inv.GetGroup(),
			"key", inv.GetKey(),
			"err", err,
		)
		return false
	}
	//发送成功
	pd.finishSend(inv.GetId(), true)
	b.metrics.sent.Add(1)
	return true
}
