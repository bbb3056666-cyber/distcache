package core

import (
	"sync/atomic"
	"time"
)

type groupMetricCounters struct {
	bloomRejected    atomic.Uint64
	peerReads        atomic.Uint64
	localLoads       atomic.Uint64
	loadErrors       atomic.Uint64
	loadLatency      latencyCounters
	peerReadLatency  latencyCounters
	localLoadLatency latencyCounters
}

// GroupMetrics 是 Group 对外返回的监控快照。
type GroupMetrics struct {
	CacheHits            uint64         // 缓存命中次数。
	CacheMisses          uint64         // 缓存未命中次数。
	CapacityRemovals     uint64         // 容量不足导致的 LRU 自动淘汰次数。
	ExpiredRemovals      uint64         // TTL 过期导致的删除次数。
	ExplicitRemovals     uint64         // 主动调用 Remove 导致的删除次数。
	Entries              int            // 当前 key 数量。
	Bytes                int64          // key 和 value 占用总字节数。
	BloomRejected        uint64         // Bloom Filter 挡住的无效查询次数。
	PeerReads            uint64         // 尝试向其他节点读取的次数，成功和失败都算。
	LocalLoads           uint64         // 真正调用本地 Getter 的次数。
	LoadErrors           uint64         // 远程或本地加载时发生的非 ErrNotFound 异常次数。
	SingleflightLoads    uint64         // 实际执行加载函数的次数。
	SingleflightShared   uint64         // 等待同 key 加载结果的次数。
	SingleflightInFlight int            // 当前正在加载的不同 key 数量。
	LoadLatency          LatencyBuckets // 缓存未命中后的完整加载耗时分布。
	PeerReadLatency      LatencyBuckets // 请求其他缓存节点的耗时分布。
	LocalLoadLatency     LatencyBuckets // 调用本地 Getter 的耗时分布。
}

// LatencyBuckets 一组互斥的耗时区间快照
type LatencyBuckets struct {
	Under1ms      uint64
	From1To5ms    uint64
	From5To20ms   uint64
	From20To100ms uint64
	Over100ms     uint64
}

type latencyCounters struct {
	under1ms      atomic.Uint64
	from1To5ms    atomic.Uint64
	from5To20ms   atomic.Uint64
	from20To100ms atomic.Uint64
	over100ms     atomic.Uint64
}

func (c *latencyCounters) observe(d time.Duration) {
	switch {
	case d < time.Millisecond:
		c.under1ms.Add(1)
	case d < 5*time.Millisecond:
		c.from1To5ms.Add(1)
	case d < 20*time.Millisecond:
		c.from5To20ms.Add(1)
	case d < 100*time.Millisecond:
		c.from20To100ms.Add(1)
	default:
		c.over100ms.Add(1)
	}
}

func (c *latencyCounters) snapshot() LatencyBuckets {
	return LatencyBuckets{
		Under1ms:      c.under1ms.Load(),
		From1To5ms:    c.from1To5ms.Load(),
		From5To20ms:   c.from5To20ms.Load(),
		From20To100ms: c.from20To100ms.Load(),
		Over100ms:     c.over100ms.Load(),
	}
}
