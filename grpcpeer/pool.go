package grpcpeer

import (
	"errors"
	"fmt"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Pool 管理到其他节点的 gRPC 客户端连接。
type Pool struct {
	mu        sync.RWMutex
	self      string
	conns     map[string]*grpc.ClientConn
	unhealthy map[string]struct{}
}

func NewPool(self string) *Pool {
	return &Pool{
		self:      self,
		conns:     make(map[string]*grpc.ClientConn),
		unhealthy: make(map[string]struct{}),
	}
}

// Set 为其他节点创建可复用的 gRPC 连接。
func (p *Pool) Set(addrs ...string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	var errs []error
	for _, addr := range addrs {
		if addr == p.self {
			continue
		}
		if _, exists := p.conns[addr]; exists {
			continue
		}
		conn, err := grpc.NewClient(
			addr,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		if err != nil {
			errs = append(errs, fmt.Errorf("create gRPC client for %s: %w", addr, err))
			continue
		}
		p.conns[addr] = conn
	}
	return errors.Join(errs...)
}

func (p *Pool) Get(addr string) (*grpc.ClientConn, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if conn, ok := p.conns[addr]; ok {
		return conn, true
	}
	return nil, false
}

func (p *Pool) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for addr, conn := range p.conns {
		_ = conn.Close()
		delete(p.conns, addr)
	}
}

func (p *Pool) Addrs() []string {
	p.mu.RLock()
	defer p.mu.RUnlock()
	var addrs []string
	for addr := range p.conns {
		addrs = append(addrs, addr)
	}
	return addrs
}

// MarkUnhealthy 将节点标记为不可用，但不关闭连接。
func (p *Pool) MarkUnhealthy(addr string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.unhealthy[addr] = struct{}{}
}

// MarkHealthy 清除节点的不可用标记。
func (p *Pool) MarkHealthy(addr string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.unhealthy, addr)
}

func (p *Pool) IsUnhealthy(addr string) bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	_, unhealthy := p.unhealthy[addr]
	return unhealthy
}
