package grpcpeer

import (
	"log/slog"
	"sort"
	"sync"
	"sync/atomic"
)

// peerDelivery 管理发往单个节点的失效消息。
type peerDelivery struct {
	sendCh      chan *Invalidation
	pendingMu   sync.Mutex
	pendingMsgs map[uint64]*Invalidation
	stateCh     chan struct{}
	restartCh   chan struct{}
	active      atomic.Bool
}

func newPeerDelivery(sendCapacity int) *peerDelivery {
	return &peerDelivery{
		sendCh:      make(chan *Invalidation, sendCapacity),
		pendingMsgs: make(map[uint64]*Invalidation),
		stateCh:     make(chan struct{}, 1),
		restartCh:   make(chan struct{}, 1),
	}
}

// addPending 在发送前记录消息。只有队列从空变为非空时才唤醒 Ack 看门狗。
func (pd *peerDelivery) addPending(inv *Invalidation) bool {
	pd.pendingMu.Lock()
	if len(pd.pendingMsgs) >= maxPendingPerPeer {
		pd.pendingMu.Unlock()
		return false
	}
	if pd.pendingMsgs == nil {
		pd.pendingMsgs = make(map[uint64]*Invalidation)
	}
	wasEmpty := len(pd.pendingMsgs) == 0
	pd.pendingMsgs[inv.GetId()] = inv
	pd.pendingMu.Unlock()

	if wasEmpty {
		pd.signalStateChange()
	}
	return true
}

func (pd *peerDelivery) tryEnqueue(id uint64) bool {
	pd.pendingMu.Lock()
	inv := pd.pendingMsgs[id]
	pd.pendingMu.Unlock()
	if inv == nil {
		return false
	}

	select {
	case pd.sendCh <- inv:
		return true
	default:
		return false
	}
}

func (pd *peerDelivery) pendingCount() int {
	pd.pendingMu.Lock()
	defer pd.pendingMu.Unlock()
	return len(pd.pendingMsgs)
}

// pendingForPeer 返回某个节点尚未确认的消息，并按 ID 排序供重连补发。
func (pd *peerDelivery) pendingForPeer() []*Invalidation {
	pd.pendingMu.Lock()
	defer pd.pendingMu.Unlock()

	messages := make([]*Invalidation, 0, len(pd.pendingMsgs))
	for _, inv := range pd.pendingMsgs {
		messages = append(messages, inv)
	}
	sort.Slice(messages, func(i, j int) bool {
		return messages[i].GetId() < messages[j].GetId()
	})
	return messages
}

func (pd *peerDelivery) ackPending(id uint64) bool {
	return pd.removePending(id)
}

func (pd *peerDelivery) removePending(id uint64) bool {
	pd.pendingMu.Lock()
	if _, ok := pd.pendingMsgs[id]; !ok {
		pd.pendingMu.Unlock()
		return false
	}
	delete(pd.pendingMsgs, id)
	pd.pendingMu.Unlock()

	// Ack 代表远端处理有进展；看门狗据此停止或重置计时。
	pd.signalStateChange()
	return true
}

func (pd *peerDelivery) signalStateChange() {
	if pd.stateCh == nil {
		return
	}
	select {
	case pd.stateCh <- struct{}{}:
	default:
	}
}

func (pd *peerDelivery) requestRestart() {
	if pd.restartCh == nil {
		return
	}
	select {
	case pd.restartCh <- struct{}{}:
	default:
	}
}

func (pd *peerDelivery) clearRestartRequest() {
	if pd.restartCh == nil {
		return
	}
	select {
	case <-pd.restartCh:
	default:
	}
}

func (pd *peerDelivery) send(
	addr string,
	b *Broadcaster,
	inv *Invalidation,
	stream GroupCache_InvalidateClient,
) bool {
	if err := stream.Send(inv); err != nil {
		b.metrics.failures.Add(1)
		slog.Warn(
			"invalidation writer goroutine exited due to failed send",
			"component", "broadcaster",
			"peer", addr,
			"id", inv.GetId(),
			"group", inv.GetGroup(),
			"key", inv.GetKey(),
			"err", err,
		)
		return false
	}
	b.metrics.sent.Add(1)
	return true
}
