package singleflight

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestStats(t *testing.T) {
	var g Group
	started := make(chan struct{})
	release := make(chan struct{})

	first := g.DoChan("key", func() (any, error) {
		close(started)
		<-release
		return "value", nil
	})
	<-started
	second := g.DoChan("key", func() (any, error) {
		t.Fatal("duplicate call should not execute its function")
		return nil, nil
	})

	stats := g.Stats()
	if stats.Loads != 1 || stats.Shared != 1 || stats.InFlight != 1 {
		t.Fatalf("Stats() = %+v, want loads=1 shared=1 inFlight=1", stats)
	}

	close(release)
	<-first
	<-second
	if got := g.Stats().InFlight; got != 0 {
		t.Fatalf("InFlight after completion = %d, want 0", got)
	}
}

// TestBasic 基础：fn 执行一次，返回正确结果。
func TestBasic(t *testing.T) {
	var g Group
	v, err := g.Do(context.Background(), "key", func() (any, error) {
		return "bar", nil
	})
	if err != nil || v != "bar" {
		t.Fatalf("Do = %v, %v; want bar, nil", v, err)
	}
}

// TestDedup 核心：100 个并发同 key 请求，fn 只执行一次，全拿到同一结果。
func TestDedup(t *testing.T) {
	var g Group
	var calls atomic.Int64

	const n = 100
	var wg sync.WaitGroup
	wg.Add(n)
	results := make([]string, n)
	errs := make([]error, n)

	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			// fn 故意睡 50ms，让 100 个 goroutine 都撞进"同 key 在执行"的窗口
			v, err := g.Do(context.Background(), "key", func() (any, error) {
				calls.Add(1)
				time.Sleep(50 * time.Millisecond)
				return "bar", nil
			})
			results[i] = v.(string)
			errs[i] = err
		}(i)
	}
	wg.Wait()

	if calls.Load() != 1 {
		t.Fatalf("fn 执行了 %d 次, want 1（单飞失效）", calls.Load())
	}
	for i := 0; i < n; i++ {
		if errs[i] != nil || results[i] != "bar" {
			t.Fatalf("caller %d: (%q, %v)", i, results[i], errs[i])
		}
	}
}

// TestDifferentKeys 不同 key 不去重：各执行各的，互不等待。
func TestDifferentKeys(t *testing.T) {
	var g Group
	var calls atomic.Int64

	var wg sync.WaitGroup
	wg.Add(2)
	for _, key := range []string{"Tom", "Jack"} {
		go func(key string) {
			defer wg.Done()
			_, _ = g.Do(context.Background(), key, func() (any, error) {
				calls.Add(1)
				time.Sleep(20 * time.Millisecond)
				return key, nil
			})
		}(key)
	}
	wg.Wait()

	if calls.Load() != 2 {
		t.Fatalf("两个不同 key 应各自执行一次, 实际 %d", calls.Load())
	}
}

// TestContextCancel 等待者 ctx 被取消 → 提前返回 ctx.Err()，
// 但执行者照常跑完（结果对执行者仍然有效）。
func TestContextCancel(t *testing.T) {
	var g Group
	var calls atomic.Int64

	// 执行者先占位
	executorDone := make(chan struct{})
	go func() {
		g.Do(context.Background(), "key", func() (any, error) {
			calls.Add(1)
			time.Sleep(100 * time.Millisecond)
			return "bar", nil
		})
		close(executorDone)
	}()

	// 等执行者真正开始跑
	time.Sleep(10 * time.Millisecond)

	// 等待者：ctx 立刻取消
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := g.Do(ctx, "key", func() (any, error) { return "should-not-run", nil })

	if !errors.Is(err, context.Canceled) {
		t.Fatalf("等待者 err = %v, want context.Canceled", err)
	}
	// 执行者的 fn 没被取消，照常跑完
	<-executorDone
	if calls.Load() != 1 {
		t.Fatalf("fn 执行了 %d 次, want 1", calls.Load())
	}
}

// TestForget 丢弃后，新请求会重新执行 fn。
func TestForget(t *testing.T) {
	var g Group
	var calls atomic.Int64

	// 第一个请求占位后 Forget，模拟"fn 会失败，不等了"
	executorDone := make(chan struct{})
	go func() {
		g.Do(context.Background(), "key", func() (any, error) {
			calls.Add(1)
			time.Sleep(100 * time.Millisecond)
			return "first", nil
		})
		close(executorDone)
	}()

	time.Sleep(10 * time.Millisecond) // 让它占上位
	g.Forget("key")                   // 丢掉占位

	// 新请求不再等待旧的，重新执行 fn
	v, err := g.Do(context.Background(), "key", func() (any, error) {
		calls.Add(1)
		return "second", nil
	})
	if err != nil || v != "second" {
		t.Fatalf("after Forget, Do = %v, %v", v, err)
	}
	if calls.Load() != 2 {
		t.Fatalf("fn 应执行 2 次（第二次不被旧调用挡住）, 实际 %d", calls.Load())
	}
	<-executorDone
}

// TestDoChan 异步拿结果：不阻塞，结果从 channel 来。
func TestDoChan(t *testing.T) {
	var g Group
	ch := g.DoChan("key", func() (any, error) {
		time.Sleep(20 * time.Millisecond)
		return "async-bar", nil
	})

	// 返回后可以干别的（这里模拟：立即检查 ch 还没结果）
	select {
	case r := <-ch:
		t.Fatalf("还没执行完就收到结果？ %+v", r)
	default:
		// 正常：还没结果，不阻塞
	}

	r := <-ch // 等真正的结果
	if r.Val != "async-bar" || r.Err != nil {
		t.Fatalf("DoChan result = %+v", r)
	}
}

// TestShared 验证 Shared 语义（用公开 API DoChan 测）：
//   - 独立调用（没人共享）→ Shared=false
//   - 两个并发调用 → 执行者和等待者都 Shared=true
func TestShared(t *testing.T) {
	var g Group

	// ① 独立调用：唯一一个调用，没人共享 → Shared=false
	ch1 := g.DoChan("solo", func() (any, error) {
		return "solo", nil
	})
	if r := <-ch1; r.Shared {
		t.Fatalf("独立调用 Shared = true, want false: %+v", r)
	}

	// ② 两个并发调用同一个 key → 都共享
	chA := g.DoChan("share", func() (any, error) {
		time.Sleep(30 * time.Millisecond)
		return "shared", nil
	})
	chB := g.DoChan("share", func() (any, error) {
		return "should-not-run", nil
	})
	rA, rB := <-chA, <-chB
	if !rA.Shared || !rB.Shared {
		t.Fatalf("并发调用 Shared 应为 true: A=%+v B=%+v", rA, rB)
	}
}

// TestFnPanic 验证：fn panic 时，等待者和执行者都拿到 panic 转的 error，
// 而不是卡死；且 map 清理干净（不泄漏）。
func TestFnPanic(t *testing.T) {
	var g Group

	// 执行者：先睡 30ms 占位（让等待者能加入），再 panic
	execDone := make(chan struct{})
	var execErr error
	go func() {
		defer close(execDone)
		_, execErr = g.Do(context.Background(), "key", func() (any, error) {
			time.Sleep(30 * time.Millisecond)
			panic("boom")
		})
	}()

	// 等执行者占位
	time.Sleep(10 * time.Millisecond)

	// 等待者：应该拿到错误而不是卡死
	_, err := g.Do(context.Background(), "key", func() (any, error) {
		return "should-not-run", nil
	})
	if err == nil {
		t.Fatal("等待者应拿到 panic 转的 error")
	}
	if !strings.Contains(err.Error(), "boom") {
		t.Fatalf("错误应包含 panic 信息: %v", err)
	}

	// 执行者也不该卡死，且也拿到 panic 错误（不是 (nil, nil)）
	select {
	case <-execDone:
	case <-time.After(time.Second):
		t.Fatal("执行者卡死了")
	}
	if execErr == nil || !strings.Contains(execErr.Error(), "boom") {
		t.Fatalf("执行者也应拿到 panic 错误, got %v", execErr)
	}

	// map 清理干净：没有泄漏
	if g.Len() != 0 {
		t.Fatalf("Len = %d, want 0（map 泄漏了）", g.Len())
	}
}

// TestMixedDoDoChan 验证：Do 当执行者、DoChan 当等待者时，
// DoChan 也能收到结果（以前的 bug：Do 执行者不通知 chans → DoChan 卡死）。
func TestMixedDoDoChan(t *testing.T) {
	var g Group

	execDone := make(chan struct{})
	go func() {
		defer close(execDone)
		v, err := g.Do(context.Background(), "key", func() (any, error) {
			time.Sleep(30 * time.Millisecond)
			return "bar", nil
		})
		if v != "bar" || err != nil {
			t.Errorf("Do 执行者: (%v, %v)", v, err)
		}
	}()

	time.Sleep(5 * time.Millisecond) // 让 Do 先占位

	// DoChan 等待者：应该能收到结果（不应卡死）
	ch := g.DoChan("key", func() (any, error) {
		return "should-not-run", nil
	})
	select {
	case r := <-ch:
		if r.Val != "bar" || !r.Shared {
			t.Fatalf("DoChan 收到: %+v, want bar/shared", r)
		}
	case <-time.After(time.Second):
		t.Fatal("DoChan 等待者卡死了（Do 执行者没通知 chans）")
	}
	<-execDone
}
