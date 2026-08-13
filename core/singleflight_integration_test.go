package core

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestGroupSingleFlight 验证 singleflight 接入 Group 后的效果：
// N 个并发请求同时 Get 同一个【冷 key】（缓存 miss），
// getter 只被回调一次，其余请求共享结果 → 防缓存击穿。
func TestGroupSingleFlight(t *testing.T) {
	var loads atomic.Int64
	g := NewGroup("scores", GetterFunc(func(ctx context.Context, key string) ([]byte, error) {
		loads.Add(1)
		time.Sleep(50 * time.Millisecond) // 模拟慢回源
		return []byte("630"), nil
	}))

	const n = 50
	var wg sync.WaitGroup
	start := make(chan struct{}) // 同时放行，制造真并发
	errs := make([]error, n)
	vals := make([]string, n)

	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			v, err := g.Get(context.Background(), "Tom") // 冷 key，全员 miss
			vals[i], errs[i] = v.String(), err
		}(i)
	}
	close(start)
	wg.Wait()

	// 关键断言：getter 只被调了 1 次（50 个并发请求合并成一次回源）
	if loads.Load() != 1 {
		t.Fatalf("getter 被调用 %d 次, want 1（缓存击穿了）", loads.Load())
	}
	metrics := g.Metrics()
	if metrics.SingleflightLoads != 1 || metrics.SingleflightShared == 0 || metrics.SingleflightInFlight != 0 {
		t.Fatalf("singleflight metrics = %+v, want loads=1 shared>0 inFlight=0", metrics)
	}
	// 所有人拿到正确结果
	for i := 0; i < n; i++ {
		if errs[i] != nil || vals[i] != "630" {
			t.Fatalf("请求%d: (%q, %v)", i, vals[i], errs[i])
		}
	}
}
