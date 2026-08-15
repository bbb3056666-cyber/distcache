package grpcpeer

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"

	"github.com/bbb3056666-cyber/distcache/core"
)

// Server 管理本节点对外提供的 gRPC 服务和健康状态。
type Server struct {
	grpcServer    *grpc.Server
	healthService *health.Server
	cacheService  *cacheServer
}

// NewServer 创建并注册缓存服务与健康检查服务。
func NewServer() *Server {
	grpcServer := grpc.NewServer()
	cacheService := &cacheServer{shutdownCh: make(chan struct{})}
	RegisterGroupCacheServer(grpcServer, cacheService)

	healthService := health.NewServer()
	healthService.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)
	grpc_health_v1.RegisterHealthServer(grpcServer, healthService)

	return &Server{
		grpcServer:    grpcServer,
		healthService: healthService,
		cacheService:  cacheService,
	}
}

// Serve 在指定地址上提供 gRPC 服务。
func (s *Server) Serve(addr string) error {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	slog.Info(
		"grpc server listening",
		"component", "grpc",
		"address", listener.Addr().String(),
	)
	return s.grpcServer.Serve(listener)
}

// ServeListener 使用已有 listener 启动 gRPC 服务。
func (s *Server) ServeListener(listener net.Listener) error {
	return s.grpcServer.Serve(listener)
}

// SetNotServing 将本节点标记为不再接收新的路由请求。
func (s *Server) SetNotServing() {
	s.healthService.SetServingStatus("", grpc_health_v1.HealthCheckResponse_NOT_SERVING)
}

// GracefulStop 停止接收新的 RPC，并等待正在执行的 RPC 完成。
func (s *Server) GracefulStop() {
	s.grpcServer.GracefulStop()
}

// Shutdown 先切换健康状态，再优雅关闭；超时后强制停止长时间 RPC。
func (s *Server) Shutdown(timeout time.Duration) {
	s.SetNotServing()
	if timeout <= 0 {
		s.grpcServer.Stop()
		return
	}

	done := make(chan struct{})
	go func() {
		s.cacheService.triggerShutdown()
		s.GracefulStop()
		close(done)
	}()

	select {
	case <-done:
		slog.Info(
			"grpc gracefully stopped",
			"component", "grpc",
		)
	case <-time.After(timeout):
		slog.Warn(
			"grpc graceful stop timed out, forcing stop",
			"component", "grpc",
			"timeout", timeout,
		)
		s.grpcServer.Stop()
		<-done
	}
}

type cacheServer struct {
	shutdownCh   chan struct{}
	shutdownOnce sync.Once
	UnimplementedGroupCacheServer
}

// Get 处理远程缓存读取请求。
func (s *cacheServer) Get(ctx context.Context, req *Request) (*Response, error) {
	groupName := req.GetGroup()
	group := core.GetGroup(groupName)
	if group == nil {
		slog.Warn(
			"remote cache request for missing group",
			"component", "grpc",
			"group", groupName,
			"key", req.GetKey(),
		)
		return nil, status.Error(codes.NotFound, "no such group: "+req.GetGroup())
	}
	view, err := group.Get(ctx, req.GetKey())
	if err != nil {
		if errors.Is(err, core.ErrNotFound) {
			return nil, status.Error(codes.NotFound, err.Error())
		}
		return nil, err
	}
	return &Response{Value: view.ByteSlice()}, nil
}

// Invalidate 持续接收其他节点发来的失效通知。
func (s *cacheServer) Invalidate(stream grpc.BidiStreamingServer[Invalidation, Ack]) error {
	recvCh := make(chan recvResult, 1)
	go func() {
		for {
			msg, err := stream.Recv()
			select {
			case recvCh <- recvResult{msg: msg, err: err}:
			case <-stream.Context().Done():
				return
			}
			if err != nil {
				return
			}
		}
	}()

	for {
		select {
		case <-s.shutdownCh:
			slog.Info("closing invalidate stream due to server shutdown", "component", "grpc")
			return status.Error(codes.Unavailable, "server shutting down, please reconnect")
		case result := <-recvCh:
			msg, err := result.msg, result.err
			if err == io.EOF {
				return nil
			}
			if err != nil {
				return err
			}

			group := core.GetGroup(msg.GetGroup())
			if group == nil {
				slog.Warn(
					"invalidation ignored for missing group",
					"component", "grpc",
					"group", msg.GetGroup(),
					"key", msg.GetKey(),
				)
			} else {
				group.RemoveLocal(msg.GetKey())
			}

			if err := stream.Send(&Ack{Id: msg.GetId(), Group: msg.GetGroup(), Key: msg.GetKey()}); err != nil {
				slog.Warn(
					"invalidation ack send failed",
					"component", "grpc",
					"group", msg.GetGroup(),
					"key", msg.GetKey(),
					"err", err,
				)
				return err
			}
		}
	}
}

type recvResult struct {
	msg *Invalidation
	err error
}

func (s *cacheServer) triggerShutdown() {
	s.shutdownOnce.Do(func() { close(s.shutdownCh) })
}

func ServeAddr(addr string) error {
	return NewServer().Serve(addr)
}

// Serve 保留旧入口，兼容直接启动独立 gRPC 服务的调用方。
func Serve(addr string) error {
	return ServeAddr(addr)
}
