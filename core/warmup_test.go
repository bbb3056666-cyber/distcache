package core

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestWarmUp(t *testing.T) {
	var mu sync.Mutex
	calls := make(map[string]int)
	g := NewGroup("warm-up", GetterFunc(func(_ context.Context, key string) ([]byte, error) {
		mu.Lock()
		calls[key]++
		mu.Unlock()
		switch key {
		case "Tom", "Sam":
			return []byte(key), nil
		case "missing":
			return nil, ErrNotFound
		default:
			return nil, errors.New("data source failed")
		}
	}))
	defer g.Close()

	result, err := g.WarmUp(context.Background(), []string{"Tom", "Sam", "missing", "failed"}, 2)
	if err != nil {
		t.Fatalf("WarmUp() error = %v", err)
	}
	want := (WarmUpResult{Requested: 4, Attempted: 4, Succeeded: 2, NotFound: 1, Failed: 1})
	if result != want {
		t.Fatalf("WarmUp() = %+v, want %+v", result, want)
	}

	if value, err := g.Get(context.Background(), "Tom"); err != nil || value.String() != "Tom" {
		t.Fatalf("Get(Tom) = (%q, %v)", value.String(), err)
	}
	mu.Lock()
	defer mu.Unlock()
	if calls["Tom"] != 1 {
		t.Fatalf("Tom getter calls = %d, want 1", calls["Tom"])
	}
}

func TestWarmUpLimitsWorkerConcurrency(t *testing.T) {
	var active atomic.Int64
	var maximum atomic.Int64
	release := make(chan struct{})
	started := make(chan struct{}, 3)
	g := NewGroup("warm-up-concurrency", GetterFunc(func(_ context.Context, key string) ([]byte, error) {
		current := active.Add(1)
		defer active.Add(-1)
		for {
			observed := maximum.Load()
			if current <= observed || maximum.CompareAndSwap(observed, current) {
				break
			}
		}
		started <- struct{}{}
		<-release
		return []byte(key), nil
	}))
	defer g.Close()

	done := make(chan WarmUpResult, 1)
	go func() {
		result, _ := g.WarmUp(context.Background(), []string{"a", "b", "c"}, 2)
		done <- result
	}()

	<-started
	<-started
	select {
	case <-started:
		t.Fatal("WarmUp started more workers than requested")
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	result := <-done
	if result.Succeeded != 3 || maximum.Load() != 2 {
		t.Fatalf("WarmUp result = %+v, maximum concurrency = %d", result, maximum.Load())
	}
}

func TestWarmUpRejectsInvalidConcurrency(t *testing.T) {
	g := NewGroup("warm-up-invalid", GetterFunc(func(context.Context, string) ([]byte, error) {
		t.Fatal("getter should not be called")
		return nil, nil
	}))
	defer g.Close()

	result, err := g.WarmUp(context.Background(), []string{"Tom"}, 0)
	if err == nil || result.Requested != 1 || result.Attempted != 0 {
		t.Fatalf("WarmUp() = (%+v, %v), want validation error", result, err)
	}
}
