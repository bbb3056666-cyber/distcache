// Package lru 实现泛型 LRU 缓存。
package lru

import "container/list"

// RemovalReason 表示缓存条目的移除原因。
type RemovalReason uint8

const (
	RemovalByCapacity RemovalReason = iota
	RemovalByExplicit
	RemovalByExpiration
)

// Cache 是 LRU 缓存；它本身不保证并发安全。
type Cache[V any] struct {
	maxBytes  int64
	nbytes    int64
	ll        *list.List
	items     map[string]*list.Element
	sizeOf    func(v V) int
	onEvicted func(key string, value V, reason RemovalReason)
}

type entry[V any] struct {
	key   string
	value V
}

// New 创建 LRU 缓存。
func New[V any](maxBytes int64, sizeOf func(V) int, onEvicted func(string, V, RemovalReason)) *Cache[V] {
	if sizeOf == nil {
		panic("lru: nil sizeOf")
	}
	return &Cache[V]{
		maxBytes:  maxBytes,
		ll:        list.New(),
		items:     make(map[string]*list.Element),
		sizeOf:    sizeOf,
		onEvicted: onEvicted,
	}
}

// Add 插入或更新 key。
func (c *Cache[V]) Add(key string, value V) {
	if ele, ok := c.items[key]; ok {
		c.ll.MoveToFront(ele)
		kv := ele.Value.(*entry[V])
		c.nbytes += int64(c.sizeOf(value)) - int64(c.sizeOf(kv.value))
		kv.value = value
		return
	}

	ele := c.ll.PushFront(&entry[V]{key: key, value: value})
	c.items[key] = ele
	c.nbytes += int64(len(key)) + int64(c.sizeOf(value))
	c.adjustSize()
}

func (c *Cache[V]) adjustSize() {
	for c.maxBytes > 0 && c.nbytes > c.maxBytes {
		c.removeOldest()
	}
}

func (c *Cache[V]) removeOldest() {
	ele := c.ll.Back()
	if ele != nil {
		c.removeElement(ele, RemovalByCapacity)
	}
}

func (c *Cache[V]) removeElement(ele *list.Element, reason RemovalReason) {
	c.ll.Remove(ele)
	kv := ele.Value.(*entry[V])
	delete(c.items, kv.key)
	c.nbytes -= int64(len(kv.key)) + int64(c.sizeOf(kv.value))
	if c.onEvicted != nil {
		c.onEvicted(kv.key, kv.value, reason)
	}
}

// Get 返回 value，并将条目移动到链表头部。
func (c *Cache[V]) Get(key string) (value V, ok bool) {
	ele, ok := c.items[key]
	if !ok {
		var nothing V
		return nothing, false
	}
	c.ll.MoveToFront(ele)
	return ele.Value.(*entry[V]).value, true
}

// Peek 返回 value，但不更新 LRU 访问顺序。
func (c *Cache[V]) Peek(key string) (value V, ok bool) {
	ele, ok := c.items[key]
	if !ok {
		var nothing V
		return nothing, false
	}
	return ele.Value.(*entry[V]).value, true
}

// Remove 主动删除 key。
func (c *Cache[V]) Remove(key string) {
	c.RemoveWithReason(key, RemovalByExplicit)
}

// RemoveWithReason 按指定原因删除 key。
func (c *Cache[V]) RemoveWithReason(key string, reason RemovalReason) {
	if ele, ok := c.items[key]; ok {
		c.removeElement(ele, reason)
	}
}

// RemoveIf 删除所有满足条件的条目。
func (c *Cache[V]) RemoveIf(remove func(key string, value V) bool) int {
	return c.RemoveIfWithReason(remove, RemovalByExplicit)
}

// RemoveIfWithReason 按指定原因删除所有满足条件的条目。
func (c *Cache[V]) RemoveIfWithReason(remove func(key string, value V) bool, reason RemovalReason) int {
	removed := 0
	for ele := c.ll.Back(); ele != nil; {
		prev := ele.Prev()
		kv := ele.Value.(*entry[V])
		if remove(kv.key, kv.value) {
			c.removeElement(ele, reason)
			removed++
		}
		ele = prev
	}
	return removed
}

// Len 返回当前条目数量。
func (c *Cache[V]) Len() int {
	return c.ll.Len()
}

// Bytes 返回当前记录的 key 和 value 字节数。
func (c *Cache[V]) Bytes() int64 {
	return c.nbytes
}
