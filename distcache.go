// Package distcache 提供分布式缓存的高层入口。
package distcache

import (
	"errors"
	"github.com/bbb3056666-cyber/distcache/core"
	"github.com/bbb3056666-cyber/distcache/grpcpeer"
	"sync"
)

// 常用类型别名，调用方可以只导入根包。
type (
	Group          = core.Group
	ByteView       = core.ByteView
	LocalGetter    = core.LocalGetter
	GetterFunc     = core.GetterFunc
	Option         = core.Option
	GroupMetrics   = core.GroupMetrics
	LatencyBuckets = core.LatencyBuckets
	NodeMetrics    = grpcpeer.NodeMetrics
)

var (
	ErrNotFound = core.ErrNotFound
	// 建组可选选项
	WithMaxBytes           = core.WithMaxBytes
	WithTTL                = core.WithTTL
	WithBloomKeys          = core.WithBloomKeys
	WithTTLJitter          = core.WithTTLJitterRatio
	WithTTLJitterRatio     = core.WithTTLJitterRatio
	WithMaxConcurrentLoads = core.WithMaxConcurrentLoads
)

// Config 是缓存节点配置。
type Config struct {
	Addr  string
	Nodes []string
}

// Cache 一个完整可运行的缓存节点。
type Cache struct {
	mu     sync.RWMutex
	node   *grpcpeer.Node
	groups map[string]*core.Group
}

// MetricsSnapshot 此节点和节点上所有 Group 的指标快照。
type MetricsSnapshot struct {
	Node   NodeMetrics
	Groups map[string]GroupMetrics
}

// New 创建一个完整缓存节点。
func New(cfg Config) (*Cache, error) {
	if cfg.Addr == "" {
		return nil, errors.New("distcache: empty addr")
	}

	node, err := grpcpeer.NewNode(cfg.Addr, normalizeNodes(cfg.Addr, cfg.Nodes)...)
	if err != nil {
		return nil, err
	}
	return &Cache{
		node:   node,
		groups: make(map[string]*core.Group),
	}, nil
}

// NewGroup 创建缓存组，并自动接入当前节点的路由和失效广播。
func (c *Cache) NewGroup(name string, getter LocalGetter, opts ...Option) *Group {
	if c == nil {
		panic("distcache: nil Cache")
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.groups[name]; ok {
		panic("distcache: duplicate group: " + name)
	}

	groupOpts := make([]core.Option, 0, len(opts)+2)
	groupOpts = append(groupOpts, core.WithPeerPicker(c.node), core.WithBroadcaster(c.node))
	groupOpts = append(groupOpts, opts...)

	g := core.NewGroup(name, getter, groupOpts...)
	c.groups[name] = g
	return g
}

// Group 返回已创建的缓存组。
func (c *Cache) Group(name string) (*Group, bool) {
	if c == nil {
		return nil, false
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	g, ok := c.groups[name]
	return g, ok
}

// Serve 启动 gRPC 节点服务，调用后会阻塞。
func (c *Cache) Serve() error {
	if c == nil {
		return errors.New("distcache: nil Cache")
	}
	return c.node.Serve()
}

// Stop 停止节点服务和后台任务。
func (c *Cache) Stop() {
	if c == nil {
		return
	}
	c.node.Stop()

	c.mu.RLock()
	defer c.mu.RUnlock()
	for _, g := range c.groups {
		g.Close()
	}
}

// Metrics 返回节点和所有缓存组的指标快照。
func (c *Cache) Metrics() MetricsSnapshot {
	if c == nil {
		return MetricsSnapshot{}
	}

	c.mu.RLock()
	defer c.mu.RUnlock()
	groups := make(map[string]GroupMetrics, len(c.groups))
	for name, g := range c.groups {
		groups[name] = g.Metrics()
	}
	return MetricsSnapshot{
		Node:   c.node.Metrics(),
		Groups: groups,
	}
}

func normalizeNodes(self string, nodes []string) []string {
	seen := make(map[string]struct{}, len(nodes)+1)
	normalized := make([]string, 0, len(nodes)+1)
	for _, node := range nodes {
		if node == "" {
			continue
		}
		if _, ok := seen[node]; ok {
			continue
		}
		seen[node] = struct{}{}
		normalized = append(normalized, node)
	}
	if _, ok := seen[self]; !ok {
		normalized = append(normalized, self)
	}
	return normalized
}
