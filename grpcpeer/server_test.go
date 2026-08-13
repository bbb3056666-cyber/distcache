package grpcpeer

import (
	"context"
	"testing"
	"time"

	"google.golang.org/grpc/health/grpc_health_v1"
)

func TestServerSetNotServing(t *testing.T) {
	server := NewServer()

	initial, err := server.healthService.Check(context.Background(), &grpc_health_v1.HealthCheckRequest{})
	if err != nil || initial.GetStatus() != grpc_health_v1.HealthCheckResponse_SERVING {
		t.Fatalf("initial health = (%v, %v), want SERVING", initial, err)
	}

	server.SetNotServing()
	response, err := server.healthService.Check(context.Background(), &grpc_health_v1.HealthCheckRequest{})
	if err != nil || response.GetStatus() != grpc_health_v1.HealthCheckResponse_NOT_SERVING {
		t.Fatalf("health after SetNotServing = (%v, %v), want NOT_SERVING", response, err)
	}
}

func TestServerShutdownMarksNotServing(t *testing.T) {
	server := NewServer()
	done := make(chan struct{})
	go func() {
		server.Shutdown(time.Second)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Shutdown did not return")
	}

	response, err := server.healthService.Check(context.Background(), &grpc_health_v1.HealthCheckRequest{})
	if err != nil || response.GetStatus() != grpc_health_v1.HealthCheckResponse_NOT_SERVING {
		t.Fatalf("health after Shutdown = (%v, %v), want NOT_SERVING", response, err)
	}
}
