package grpcpeer

import (
	"github.com/bbb3056666-cyber/distcache/core"
	"github.com/bbb3056666-cyber/distcache/pkg/consistenthash"
	"sync"
)

const defaultReplicas = 50

// Router 基于一致性哈希环实现 core.PeerPicker。
type Router struct {
	mu              sync.RWMutex
	peers           *consistenthash.Map
	pool            *Pool
	onRing          map[string]struct{}
	configuredNodes int
}

func NewRouter(pool *Pool) *Router {
	return &Router{pool: pool, onRing: make(map[string]struct{})}
}

// Set 根据节点列表重建哈希环。
func (r *Router) Set(addrs ...string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.peers = consistenthash.NewMap(defaultReplicas, nil)
	r.peers.Add(addrs...)

	r.onRing = make(map[string]struct{}, len(addrs))
	for _, addr := range addrs {
		r.onRing[addr] = struct{}{}
	}
	r.configuredNodes = len(r.onRing)
}

// RemovePeer 将节点从哈希环移除。
func (r *Router) RemovePeer(addr string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.onRing[addr]; !ok {
		return
	}
	delete(r.onRing, addr)
	if r.peers != nil {
		r.peers.Remove(addr)
	}
}

// AddPeer 将节点加回哈希环。
func (r *Router) AddPeer(addr string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.onRing[addr]; ok {
		return
	}
	r.onRing[addr] = struct{}{}
	if r.peers == nil {
		r.peers = consistenthash.NewMap(defaultReplicas, nil)
	}
	r.peers.Add(addr)
}

// PickPeer 返回负责该 key 的远程节点。
func (r *Router) PickPeer(key string) (core.PeerGetter, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if r.peers == nil {
		return nil, false
	}
	if addr := r.peers.GetNode(key); addr != "" && addr != r.pool.self {
		conn, ok := r.pool.Get(addr)
		if !ok || r.pool.IsUnhealthy(addr) {
			return nil, false
		}
		return &RemoteGetter{conn: conn}, true
	}
	return nil, false
}

func (r *Router) PeerCount() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.onRing)
}

func (r *Router) ConfiguredNodeCount() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.configuredNodes
}

var _ core.PeerPicker = (*Router)(nil)
