// Package core 实现缓存主流程：本地缓存读取、远程节点读取、本地回源、失效删除和指标暴露。
package core

import (
	"context"
	"errors"
	"github.com/bbb3056666-cyber/distcache/pkg/bloom"
	"log/slog"
	"math/rand/v2"
	"strconv"
	"sync"
	"time"

	"github.com/bbb3056666-cyber/distcache/pkg/cache"
	"github.com/bbb3056666-cyber/distcache/pkg/singleflight"
)

const (
	defaultCacheBytes     int64 = 1 << 20
	defaultTTL                  = 5 * time.Minute
	defaultTTLJitterRatio       = 0.2
	notFoundTTL                 = 5 * time.Second
)

// ErrNotFound 表示 key 在数据源中不存在。
var ErrNotFound = errors.New("core: key not found")

var (
	mu     sync.RWMutex
	groups = make(map[string]*Group)
)

type keyState struct {
	generation uint64
	inFlight   int
}

// Group 是一个缓存命名空间。
type Group struct {
	name           string
	localGetter    LocalGetter
	cache          *cache.Cache[cacheEntry]
	peerPicker     PeerPicker
	loader         *singleflight.Group
	bloomFilter    *bloom.Filter
	ttlJitterRatio float64
	ttl            time.Duration
	broadcaster    Broadcaster
	ctx            context.Context
	cancel         context.CancelFunc
	metrics        groupMetricCounters
	stateMu        sync.Mutex
	states         map[string]*keyState
}

// NewGroup 创建缓存命名空间并注册到进程内全局表。
func NewGroup(name string, localGetter LocalGetter, opts ...Option) *Group {
	if localGetter == nil {
		panic("core: nil Getter")
	}
	cfg := defaultConfig()
	for _, opt := range opts {
		opt(cfg)
	}

	ctx, cancel := context.WithCancel(context.Background())
	g := &Group{
		name:           name,
		localGetter:    localGetter,
		cache:          cache.New[cacheEntry](cfg.maxBytes, cfg.ttl, func(v cacheEntry) int { return v.view.Len() }),
		loader:         &singleflight.Group{},
		bloomFilter:    cfg.bloomFilter,
		ttlJitterRatio: cfg.ttlJitterRatio,
		ttl:            cfg.ttl,
		peerPicker:     cfg.peerPicker,
		broadcaster:    cfg.broadcaster,
		ctx:            ctx,
		cancel:         cancel,
	}

	mu.Lock()
	groups[name] = g
	mu.Unlock()

	interval := cfg.ttl / 10
	if interval < time.Second {
		interval = time.Second
	}
	g.cache.StartJanitor(g.ctx, interval)
	return g
}

// Close 停止本组后台清理任务。
func (g *Group) Close() {
	slog.Info(
		"group janitor stopping",
		"component", "group",
		"groupName", g.name,
	)
	g.cancel()
}

type config struct {
	maxBytes       int64
	ttl            time.Duration
	bloomFilter    *bloom.Filter
	ttlJitterRatio float64
	peerPicker     PeerPicker
	broadcaster    Broadcaster
}

// Option 修改 Group 配置。
type Option func(*config)

// WithMaxBytes 设置 Group 缓存容量上限。
func WithMaxBytes(n int64) Option {
	return func(c *config) { c.maxBytes = n }
}

// WithTTL 设置缓存默认存活时间，0 表示不过期。
func WithTTL(d time.Duration) Option {
	return func(c *config) { c.ttl = d }
}

// WithBloomFilter 设置 Bloom Filter。
func WithBloomFilter(f *bloom.Filter) Option {
	return func(c *config) { c.bloomFilter = f }
}

// WithBloomKeys 使用已知 key 集合创建 Bloom Filter。
func WithBloomKeys(keys ...string) Option {
	return func(c *config) {
		f := bloom.NewForExpected(uint64(len(keys)))
		for _, k := range keys {
			f.Add(k)
		}
		c.bloomFilter = f
	}
}

// WithTTLJitterRatio 设置 TTL 抖动比例。
func WithTTLJitterRatio(r float64) Option {
	return func(c *config) { c.ttlJitterRatio = r }
}

// WithPeerPicker 设置远程节点选择器。
func WithPeerPicker(pp PeerPicker) Option {
	return func(c *config) { c.peerPicker = pp }
}

// WithBroadcaster 设置失效广播器。
func WithBroadcaster(b Broadcaster) Option {
	return func(c *config) { c.broadcaster = b }
}

func defaultConfig() *config {
	return &config{maxBytes: defaultCacheBytes, ttl: defaultTTL, ttlJitterRatio: defaultTTLJitterRatio}
}

// GetGroup 按名字查找 Group。
func GetGroup(name string) *Group {
	mu.RLock()
	g := groups[name]
	mu.RUnlock()
	return g
}

// RegisterPeerPicker 给 Group 注册节点选择器。
func (g *Group) RegisterPeerPicker(peerPicker PeerPicker) {
	if g.peerPicker != nil {
		panic("core: RegisterPeers called twice")
	}
	g.peerPicker = peerPicker
}

// RegisterBroadcaster 给 Group 注册失效广播器。
func (g *Group) RegisterBroadcaster(b Broadcaster) {
	if g.broadcaster != nil {
		panic("core: RegisterBroadcaster called twice")
	}
	g.broadcaster = b
}

// LocalGetter 负责在缓存未命中时从本地数据源加载数据。
type LocalGetter interface {
	Get(ctx context.Context, key string) ([]byte, error)
}

// GetterFunc 让普通函数可以作为 LocalGetter 使用。
type GetterFunc func(ctx context.Context, key string) ([]byte, error)

// Get 实现 LocalGetter。
func (f GetterFunc) Get(ctx context.Context, key string) ([]byte, error) {
	return f(ctx, key)
}

// Get 读取缓存；未命中时会触发远程读取或本地回源。
func (g *Group) Get(ctx context.Context, key string) (ByteView, error) {
	if key == "" {
		return ByteView{}, errors.New("core: empty key")
	}
	if v, ok := g.cache.Get(key); ok {
		if v.found {
			return v.view, nil
		}
		return ByteView{}, ErrNotFound
	}
	return g.load(ctx, key)
}

func (g *Group) beginLoad(key string) uint64 {
	g.stateMu.Lock()
	defer g.stateMu.Unlock()

	if g.states == nil {
		g.states = make(map[string]*keyState)
	}
	state := g.states[key]
	if state == nil {
		state = &keyState{}
		g.states[key] = state
	}
	state.inFlight++
	return state.generation
}

func (g *Group) finishLoad(key string) {
	g.stateMu.Lock()
	defer g.stateMu.Unlock()

	state := g.states[key]
	if state == nil {
		return
	}
	state.inFlight--
	if state.inFlight == 0 {
		delete(g.states, key)
	}
}

func (g *Group) cacheIfCurrent(key string, generation uint64, entry cacheEntry, ttl time.Duration) bool {
	g.stateMu.Lock()
	defer g.stateMu.Unlock()

	state := g.states[key]
	if state == nil || state.generation != generation {
		return false
	}
	g.cache.AddWithTTL(key, entry, ttl)
	return true
}

func (g *Group) load(ctx context.Context, key string) (ByteView, error) {
	if g.bloomFilter != nil && !g.bloomFilter.Test(key) {
		g.metrics.bloomRejected.Add(1)
		return ByteView{}, ErrNotFound
	}

	generation := g.beginLoad(key)
	defer g.finishLoad(key)
	flightKey := strconv.FormatUint(generation, 10) + "\x00" + key

	startedAt := time.Now()
	defer func() {
		g.metrics.loadLatency.observe(time.Since(startedAt))
	}()

	v, err := g.loader.Do(ctx, flightKey, func() (any, error) {
		value, err := g.loadValue(ctx, key)
		if err != nil {
			if errors.Is(err, ErrNotFound) {
				g.setNonExist(key, generation)
				return ByteView{}, ErrNotFound
			}
			return ByteView{}, err
		}
		g.cacheIfCurrent(key, generation, cacheEntry{view: value, found: true}, g.jitteredTTL(g.ttl))
		return value, nil
	})
	if err != nil {
		return ByteView{}, err
	}
	return v.(ByteView), nil
}

func (g *Group) loadValue(ctx context.Context, key string) (ByteView, error) {
	if g.peerPicker != nil {
		if peerGetter, ok := g.peerPicker.PickPeer(key); ok {
			value, err := g.getFromPeer(ctx, peerGetter, key)
			if err == nil {
				return value, nil
			}
			if errors.Is(err, ErrNotFound) {
				return ByteView{}, err
			}
			g.metrics.loadErrors.Add(1)
			slog.Warn(
				"remote load failed, falling back to local",
				"component", "cache",
				"group", g.name,
				"key", key,
				"fallback", "local",
				"err", err,
			)
		}
	}
	return g.getLocally(ctx, key)
}

func (g *Group) getFromPeer(ctx context.Context, peerGetter PeerGetter, key string) (ByteView, error) {
	g.metrics.peerReads.Add(1)

	startedAt := time.Now()
	res, err := peerGetter.GetFromPeer(ctx, g.name, key)
	duration := time.Since(startedAt)
	g.metrics.peerReadLatency.observe(duration)
	if err != nil {
		return ByteView{}, err
	}

	slog.Debug(
		"remote load completed",
		"component", "cache",
		"group", g.name,
		"key", key,
		"duration", duration,
	)
	value := ByteView{b: cloneBytes(res)}
	return value, nil
}

func (g *Group) getLocally(ctx context.Context, key string) (ByteView, error) {
	g.metrics.localLoads.Add(1)

	startedAt := time.Now()
	data, err := g.localGetter.Get(ctx, key)
	duration := time.Since(startedAt)
	g.metrics.localLoadLatency.observe(duration)
	if err != nil {
		if !errors.Is(err, ErrNotFound) {
			slog.Warn(
				"local load failed",
				"component", "cache",
				"group", g.name,
				"key", key,
				"duration", duration,
				"err", err,
			)
			g.metrics.loadErrors.Add(1)
		}
		return ByteView{}, err
	}

	slog.Debug(
		"local load completed",
		"component", "cache",
		"group", g.name,
		"key", key,
		"duration", duration,
	)
	value := ByteView{b: cloneBytes(data)}
	return value, nil
}

func (g *Group) setNonExist(key string, generation uint64) bool {
	return g.cacheIfCurrent(
		key,
		generation,
		cacheEntry{
			view:  ByteView{},
			found: false,
		},
		g.jitteredTTL(notFoundTTL),
	)
}

func (g *Group) jitteredTTL(base time.Duration) time.Duration {
	if base > 0 && g.ttlJitterRatio > 0 {
		jitter := time.Duration(float64(base) * g.ttlJitterRatio)
		if jitter <= 0 {
			return base
		}
		return base + time.Duration(rand.Int64N(int64(jitter)))
	}
	return base
}

// Remove 删除本地缓存并向其他节点广播失效通知。
func (g *Group) Remove(key string) error {
	g.RemoveLocal(key)
	if g.broadcaster != nil {
		if err := g.broadcaster.Broadcast(g.name, key); err != nil {
			slog.Warn(
				"cache invalidation broadcast failed",
				"component", "cache",
				"group", g.name,
				"key", key,
				"err", err,
			)
			return errors.New("core: broadcast fail: " + err.Error())
		}
	}
	return nil
}

// RemoveLocal 只删除本节点缓存。
func (g *Group) RemoveLocal(key string) {
	if key == "" {
		return
	}

	g.stateMu.Lock()
	if state := g.states[key]; state != nil {
		state.generation++
	}
	g.cache.Remove(key)
	g.stateMu.Unlock()

	slog.Debug(
		"cache key removed locally",
		"component", "cache",
		"group", g.name,
		"key", key,
	)
}

// Stats 返回底层缓存统计快照。
func (g *Group) Stats() cache.Stats {
	return g.cache.Stats()
}

// Metrics 返回 Group 级监控快照。
func (g *Group) Metrics() GroupMetrics {
	cacheStats := g.cache.Stats()
	singleflightStats := g.loader.Stats()
	return GroupMetrics{
		CacheHits:        cacheStats.Hits,
		CacheMisses:      cacheStats.Misses,
		CapacityRemovals: cacheStats.CapacityRemovals,
		ExpiredRemovals:  cacheStats.ExpiredRemovals,
		ExplicitRemovals: cacheStats.ExplicitRemovals,
		Entries:          cacheStats.Len,
		Bytes:            cacheStats.Bytes,

		BloomRejected:    g.metrics.bloomRejected.Load(),
		PeerReads:        g.metrics.peerReads.Load(),
		LocalLoads:       g.metrics.localLoads.Load(),
		LoadErrors:       g.metrics.loadErrors.Load(),
		LoadLatency:      g.metrics.loadLatency.snapshot(),
		PeerReadLatency:  g.metrics.peerReadLatency.snapshot(),
		LocalLoadLatency: g.metrics.localLoadLatency.snapshot(),

		SingleflightLoads:    singleflightStats.Loads,
		SingleflightShared:   singleflightStats.Shared,
		SingleflightInFlight: singleflightStats.InFlight,
	}
}
