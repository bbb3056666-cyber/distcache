// Package cache 实现并发安全的 LRU + TTL 缓存。
package cache

import (
	"context"
	"sync"
	"time"

	"github.com/bbb3056666-cyber/distcache/pkg/lru"
)

// Item 包装缓存值和过期时间。
type Item[V any] struct {
	Value    V
	expireAt time.Time
}

// Cache 是并发安全的 LRU + TTL 缓存。
type Cache[V any] struct {
	mu sync.RWMutex

	lru *lru.Cache[Item[V]]

	maxBytes   int64
	defaultTTL time.Duration
	sizeOf     func(V) int
	stats      stats

	stop context.CancelFunc
}

// New 创建缓存。
func New[V any](maxBytes int64, defaultTTL time.Duration, sizeOf func(V) int) *Cache[V] {
	if sizeOf == nil {
		panic("cache: nil sizeOf")
	}
	return &Cache[V]{
		maxBytes:   maxBytes,
		defaultTTL: defaultTTL,
		sizeOf:     sizeOf,
	}
}

func (c *Cache[V]) ensureLRU() {
	if c.lru == nil {
		c.lru = lru.New[Item[V]](
			c.maxBytes,
			func(it Item[V]) int { return c.sizeOf(it.Value) },
			func(_ string, _ Item[V], reason lru.RemovalReason) {
				switch reason {
				case lru.RemovalByCapacity:
					c.stats.capacityRemovals++
				case lru.RemovalByExpiration:
					c.stats.expiredRemovals++
				case lru.RemovalByExplicit:
					c.stats.explicitRemovals++
				}
			},
		)
	}
}

func (it Item[V]) expired(now time.Time) bool {
	return !it.expireAt.IsZero() && !now.Before(it.expireAt)
}

type stats struct {
	hits             uint64
	misses           uint64
	capacityRemovals uint64
	expiredRemovals  uint64
	explicitRemovals uint64
}

// Stats 是缓存统计快照。
type Stats struct {
	Hits             uint64
	Misses           uint64
	CapacityRemovals uint64
	ExpiredRemovals  uint64
	ExplicitRemovals uint64
	Len              int
	Bytes            int64
}

// Stats 返回当前缓存统计快照。
func (c *Cache[V]) Stats() Stats {
	c.mu.RLock()
	defer c.mu.RUnlock()

	s := Stats{
		Hits:             c.stats.hits,
		Misses:           c.stats.misses,
		CapacityRemovals: c.stats.capacityRemovals,
		ExpiredRemovals:  c.stats.expiredRemovals,
		ExplicitRemovals: c.stats.explicitRemovals,
	}
	if c.lru != nil {
		s.Len = c.lru.Len()
		s.Bytes = c.lru.Bytes()
	}
	return s
}

// HitRate 返回命中率。
func (s Stats) HitRate() float64 {
	total := s.Hits + s.Misses
	if total == 0 {
		return 0
	}
	return float64(s.Hits) / float64(total)
}

// StartJanitor 启动后台过期清理任务。
func (c *Cache[V]) StartJanitor(ctx context.Context, interval time.Duration) {
	if c.stop != nil {
		return
	}
	ctx, cancel := context.WithCancel(ctx)
	c.stop = cancel
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				c.removeExpired()
			case <-ctx.Done():
				return
			}
		}
	}()
}

func (c *Cache[V]) removeExpired() {
	now := time.Now()
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.lru != nil {
		c.lru.RemoveIfWithReason(
			func(_ string, it Item[V]) bool { return it.expired(now) },
			lru.RemovalByExpiration,
		)
	}
}

// Stop 停止后台过期清理任务。
func (c *Cache[V]) Stop() {
	if c.stop != nil {
		c.stop()
	}
}

// Add 使用默认 TTL 写入或更新 key。
func (c *Cache[V]) Add(key string, value V) {
	c.AddWithTTL(key, value, c.defaultTTL)
}

// AddWithTTL 使用指定 TTL 写入或更新 key。
func (c *Cache[V]) AddWithTTL(key string, value V, ttl time.Duration) {
	it := Item[V]{Value: value}
	if ttl > 0 {
		it.expireAt = time.Now().Add(ttl)
	}

	c.mu.Lock()
	c.ensureLRU()
	c.lru.Add(key, it)
	c.mu.Unlock()
}

// Get 读取 key；过期条目按未命中处理。
func (c *Cache[V]) Get(key string) (value V, ok bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.lru == nil {
		c.stats.misses++
		var nothing V
		return nothing, false
	}

	it, ok := c.lru.Get(key)
	if !ok {
		c.stats.misses++
		var nothing V
		return nothing, false
	}

	if it.expired(time.Now()) {
		c.lru.RemoveWithReason(key, lru.RemovalByExpiration)
		c.stats.misses++
		var nothing V
		return nothing, false
	}

	c.stats.hits++
	return it.Value, true
}

// Peek 读取 key，但不更新 LRU 访问顺序。
func (c *Cache[V]) Peek(key string) (value V, ok bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.lru == nil {
		var nothing V
		return nothing, false
	}
	it, ok := c.lru.Peek(key)
	if !ok {
		var nothing V
		return nothing, false
	}
	if it.expired(time.Now()) {
		var nothing V
		return nothing, false
	}
	return it.Value, true
}

// Remove 主动删除 key。
func (c *Cache[V]) Remove(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.lru != nil {
		c.lru.Remove(key)
	}
}
