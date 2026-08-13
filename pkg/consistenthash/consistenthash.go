// Package consistenthash 实现带虚拟节点的一致性哈希环。
package consistenthash

import (
	"hash/crc32"
	"sort"
	"strconv"
	"sync"
)

// HashFunc 将字节映射为哈希值。
type HashFunc func(data []byte) uint32

// Map 是并发安全的一致性哈希环。
type Map struct {
	mu       sync.RWMutex
	hash     HashFunc
	replicas int
	keys     []int
	ring     map[int]string
}

// NewMap 创建哈希环。
func NewMap(replicas int, fn HashFunc) *Map {
	if fn == nil {
		fn = crc32.ChecksumIEEE
	}
	return &Map{
		hash:     fn,
		replicas: replicas,
		ring:     make(map[int]string),
	}
}

// Add 将真实节点加入哈希环。
func (m *Map) Add(addrs ...string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, addr := range addrs {
		for i := 0; i < m.replicas; i++ {
			h := int(m.hash([]byte(strconv.Itoa(i) + addr)))
			m.keys = append(m.keys, h)
			m.ring[h] = addr
		}
	}
	sort.Ints(m.keys)
}

// Remove 将真实节点从哈希环移除。
func (m *Map) Remove(addrs ...string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, addr := range addrs {
		for i := 0; i < m.replicas; i++ {
			h := int(m.hash([]byte(strconv.Itoa(i) + addr)))
			delete(m.ring, h)
		}
	}
	keys := m.keys[:0]
	for h := range m.ring {
		keys = append(keys, h)
	}
	sort.Ints(keys)
	m.keys = keys
}

// GetNode 返回负责该 key 的节点。
func (m *Map) GetNode(key string) string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if len(m.keys) == 0 {
		return ""
	}
	h := int(m.hash([]byte(key)))
	idx := sort.Search(len(m.keys), func(i int) bool {
		return m.keys[i] >= h
	})
	return m.ring[m.keys[idx%len(m.keys)]]
}

// Len 返回虚拟节点数量。
func (m *Map) Len() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.keys)
}
