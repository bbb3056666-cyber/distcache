package cache

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

// sizeOf 测试用：string 占 len 字节。
func sizeOf(s string) int { return len(s) }

func TestBasic(t *testing.T) {
	c := New[string](0, 0, sizeOf)
	c.Add("k1", "v1")
	c.Add("k2", "v2")

	if v, ok := c.Get("k1"); !ok || v != "v1" {
		t.Fatalf("Get(k1) = %q, %v", v, ok)
	}
	if _, ok := c.Get("nope"); ok {
		t.Fatal("Get(nope) should miss")
	}
	if s := c.Stats(); s.Hits != 1 || s.Misses != 1 {
		t.Fatalf("stats = %+v, want hits=1 misses=1", s)
	}
}

func TestUpdateValue(t *testing.T) {
	c := New[string](0, 0, sizeOf)
	c.Add("k1", "v1")
	c.Add("k1", "v2") // 覆盖

	if v, ok := c.Get("k1"); !ok || v != "v2" {
		t.Fatalf("Get(k1) = %q, want v2", v)
	}
	// 更新不改变条目数
	if s := c.Stats(); s.Len != 1 {
		t.Fatalf("Len = %d, want 1", s.Len)
	}
}

func TestTTLExpired(t *testing.T) {
	c := New[string](0, 0, sizeOf)
	c.AddWithTTL("k1", "v1", 30*time.Millisecond)

	// 没过期：能读到
	if v, ok := c.Get("k1"); !ok || v != "v1" {
		t.Fatalf("before expiry Get(k1) = %q, %v", v, ok)
	}

	time.Sleep(60 * time.Millisecond) // 等它过期

	// 过期了：Get 应 miss，且内部把条目删掉了
	if _, ok := c.Get("k1"); ok {
		t.Fatal("expired key should miss")
	}
	// 因为懒过期删掉了，Len 应回到 0
	if s := c.Stats(); s.Len != 0 {
		t.Fatalf("Len after expiry = %d, want 0", s.Len)
	}
}

func TestJanitor(t *testing.T) {
	c := New[string](0, 0, sizeOf)
	c.StartJanitor(context.Background(), 20*time.Millisecond)
	defer c.Stop()

	c.AddWithTTL("k1", "v1", 30*time.Millisecond)
	c.Add("k2", "v2") // 不过期

	time.Sleep(80 * time.Millisecond) // 让 janitor 至少扫两轮

	s := c.Stats()
	// 过期条目被 janitor 清了
	if s.Len != 1 {
		t.Fatalf("Len = %d, want 1 (only k2 alive)", s.Len)
	}
	if _, ok := c.Get("k2"); !ok {
		t.Fatal("k2 (no TTL) should still be alive")
	}
	if _, ok := c.Get("k1"); ok {
		t.Fatal("k1 should be cleaned by janitor")
	}
}

func TestRemovalByCapacity(t *testing.T) {
	// 容量 8 字节：key1(4) + v1(2) = 6，key2(4) + v2(2) = 6，两条 12 > 8 → 淘汰一条
	c := New[string](8, 0, sizeOf)
	c.Add("key1", "v1") // 6
	c.Add("key2", "v2") // 12 > 8 → 淘汰 key1

	if _, ok := c.Get("key1"); ok {
		t.Fatal("key1 should be evicted by capacity")
	}
	if v, ok := c.Get("key2"); !ok || v != "v2" {
		t.Fatalf("key2 should be alive, got %q %v", v, ok)
	}
	if s := c.Stats(); s.CapacityRemovals != 1 {
		t.Fatalf("CapacityRemovals = %d, want 1", s.CapacityRemovals)
	}
}

func TestRemovalStats(t *testing.T) {
	c := New[string](0, 0, sizeOf)
	c.Add("explicit", "v")
	c.Remove("explicit")

	c.AddWithTTL("expired", "v", 20*time.Millisecond)
	time.Sleep(40 * time.Millisecond)
	c.Get("expired")

	s := c.Stats()
	if s.ExplicitRemovals != 1 {
		t.Fatalf("ExplicitRemovals = %d, want 1", s.ExplicitRemovals)
	}
	if s.ExpiredRemovals != 1 {
		t.Fatalf("ExpiredRemovals = %d, want 1", s.ExpiredRemovals)
	}
}

func TestPeekDoesNotRefresh(t *testing.T) {
	c := New[string](8, 0, sizeOf)
	c.Add("key1", "v1") // 6
	c.Add("key2", "v2") // 12 > 8 → 淘汰 key1

	// 用 Peek 读 key1 也不会救它（已经在淘汰时被删了）
	if _, ok := c.Peek("key1"); ok {
		t.Fatal("key1 already evicted")
	}

	// 单独验证 Peek 命中但不影响顺序：
	c2 := New[string](12, 0, sizeOf)
	c2.Add("key1", "v1") // 6
	c2.Add("key2", "v2") // 12，刚好
	_, ok := c2.Peek("key1")
	if !ok {
		t.Fatal("Peek(key1) should hit")
	}
	// 如果 Peek 会刷新，key1 变成最近使用，再塞一条该淘汰 key2；
	// 因为 Peek 不刷新，塞 key3 后淘汰的是 key1
	c2.Add("key3", "v3") // 18 > 12
	if _, ok := c2.Get("key1"); ok {
		t.Fatal("key1 should be evicted (Peek didn't refresh it)")
	}
}

func TestHitRate(t *testing.T) {
	c := New[string](0, 0, sizeOf)
	c.Add("k1", "v1")
	c.Get("k1")      // hit
	c.Get("k1")      // hit
	c.Get("missing") // miss

	s := c.Stats()
	if s.HitRate() != 2.0/3.0 {
		t.Fatalf("HitRate = %v, want 2/3", s.HitRate())
	}

	// 空缓存命中率 = 0，不是 NaN
	empty := New[string](0, 0, sizeOf)
	if empty.Stats().HitRate() != 0 {
		t.Fatal("empty cache HitRate should be 0, not NaN")
	}
}

// TestConcurrent 多 goroutine 同时读写。
// 注意运行方式：go test -race ./pkg/cache/ ，
// -race 会用竞争检测器跑一遍，有任何数据竞争都会标红。
func TestConcurrent(t *testing.T) {
	c := New[string](0, 5*time.Minute, sizeOf)

	var wg sync.WaitGroup
	// 8 个写者：各写自己的 key
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := fmt.Sprintf("key%d", i)
			for j := 0; j < 200; j++ {
				c.Add(key, fmt.Sprintf("v%d-%d", i, j))
			}
		}(i)
	}
	// 8 个读者：读所有人的 key
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 8; j++ {
				key := fmt.Sprintf("key%d", j)
				c.Get(key) // 返回值对不对不重要，重点是并发下不崩、无竞争
				c.Peek(key)
			}
		}(i)
	}
	wg.Wait()
}

// TestConcurrentSameKey 所有 goroutine 抢同一个 key，压力更大。
func TestConcurrentSameKey(t *testing.T) {
	c := New[string](0, 0, sizeOf)

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				c.Add("hot", "value")
				c.Get("hot")
			}
		}()
	}
	wg.Wait()
}
