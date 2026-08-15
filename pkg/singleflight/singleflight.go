// Package singleflight 按 key 合并并发调用。
package singleflight

import (
	"context"
	"fmt"
	"sync"
)

// Result 是 DoChan 返回的结果。
type Result struct {
	Val    any
	Err    error
	Shared bool
}

type call struct {
	key   string
	done  chan struct{}
	val   any
	err   error
	dups  int
	chans []chan<- Result
}

// Group 按 key 合并正在执行的调用。
type Group struct {
	mu      sync.Mutex
	m       map[string]*call
	metrics groupCounters
}

type groupCounters struct {
	loads  uint64
	shared uint64
}

// Stats 是 singleflight 统计快照。
type Stats struct {
	Loads    uint64
	Shared   uint64
	InFlight int
}

// Stats 返回当前 singleflight 统计。
func (g *Group) Stats() Stats {
	g.mu.Lock()
	defer g.mu.Unlock()
	return Stats{
		Loads:    g.metrics.loads,
		Shared:   g.metrics.shared,
		InFlight: len(g.m),
	}
}

// Do 对同一个 key 只执行一次 fn，并让并发调用共享结果。
func (g *Group) Do(ctx context.Context, key string, fn func() (any, error)) (any, error) {
	g.mu.Lock()
	if g.m == nil {
		g.m = make(map[string]*call)
	}
	if c, ok := g.m[key]; ok {
		c.dups++
		g.metrics.shared++
		g.mu.Unlock()

		select {
		case <-c.done:
			return c.val, c.err
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	c := &call{key: key, done: make(chan struct{})}
	g.m[key] = c
	g.metrics.loads++
	g.mu.Unlock()

	g.run(c, fn)
	return c.val, c.err
}

// DoChan 启动或加入某个 key 的进行中调用，并返回结果通道。
func (g *Group) DoChan(key string, fn func() (any, error)) <-chan Result {
	ch := make(chan Result, 1)
	g.mu.Lock()
	if g.m == nil {
		g.m = make(map[string]*call)
	}
	if c, ok := g.m[key]; ok {
		c.dups++
		g.metrics.shared++
		c.chans = append(c.chans, ch)
		g.mu.Unlock()
		return ch
	}

	c := &call{
		key:   key,
		done:  make(chan struct{}),
		chans: []chan<- Result{ch},
	}
	g.m[key] = c
	g.metrics.loads++
	g.mu.Unlock()

	go g.run(c, fn)
	return ch
}

func (g *Group) run(c *call, fn func() (any, error)) {
	defer g.finish(c)
	c.val, c.err = fn()
}

func (g *Group) finish(c *call) {
	if r := recover(); r != nil {
		c.err = fmt.Errorf("singleflight: fn panic: %v", r)
	}
	close(c.done)

	g.mu.Lock()
	dups := c.dups
	for i, ch := range c.chans {
		ch <- Result{Val: c.val, Err: c.err, Shared: dups > 0 || i > 0}
	}
	if g.m[c.key] == c {
		delete(g.m, c.key)
	}
	g.mu.Unlock()
}

// Forget 从进行中调用表中移除 key。
func (g *Group) Forget(key string) {
	g.mu.Lock()
	delete(g.m, key)
	g.mu.Unlock()
}

// Len 返回当前进行中的调用数量。
func (g *Group) Len() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return len(g.m)
}
