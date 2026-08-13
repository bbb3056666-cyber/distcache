package lru

import (
	"reflect"
	"strconv"
	"testing"
)

// sizeOf 测试用的简单字节计算：string 占 len 字节。
func sizeOf(s string) int { return len(s) }

func TestGet(t *testing.T) {
	c := New[string](0, sizeOf, nil)
	c.Add("key1", "1234")

	// 命中：值要一模一样
	if v, ok := c.Get("key1"); !ok || v != "1234" {
		t.Fatalf("cache hit key1=1234 failed, got %q ok=%v", v, ok)
	}
	// miss：ok 必须是 false，value 是零值 ""
	if v, ok := c.Get("key2"); ok || v != "" {
		t.Fatalf("cache miss key2 failed, got %q ok=%v", v, ok)
	}
}

func TestAddUpdate(t *testing.T) {
	c := New[int](0, func(v int) int { return len(strconv.Itoa(v)) }, nil)
	c.Add("key", 1)
	c.Add("key", 111) // 更新：value 从 1 → 111

	// 更新后读到新值
	if v, ok := c.Get("key"); !ok || v != 111 {
		t.Fatalf("after update got %d ok=%v", v, ok)
	}
	// 字节数应等于 len("key") + len("111") = 3 + 3 = 6，而不是把旧的 1 也算进去
	if c.Bytes() != int64(6) {
		t.Fatalf("Bytes() = %d, want 6", c.Bytes())
	}
	// 还是同一条目，不是两条
	if c.Len() != 1 {
		t.Fatalf("Len() = %d, want 1", c.Len())
	}
}

func TestEviction(t *testing.T) {
	// 容量：正好够放 2 个条目（key1: 4+1=5, key2: 4+1=5）
	c := New[string](10, sizeOf, nil)
	c.Add("key1", "a") // 5
	c.Add("key2", "b") // 10
	c.Add("key3", "c") // 15 > 10 → 淘汰最久未用的 key1

	if _, ok := c.Get("key1"); ok {
		t.Fatal("key1 should have been evicted")
	}
	if c.Len() != 2 {
		t.Fatalf("Len() = %d, want 2", c.Len())
	}

	// Get 之后 key2 变成最近使用，再插入会淘汰 key3 而不是 key2
	c.Get("key2")
	c.Add("key4", "d")
	if _, ok := c.Get("key3"); ok {
		t.Fatal("key3 should have been evicted (key2 was touched)")
	}
	if _, ok := c.Get("key2"); !ok {
		t.Fatal("key2 should still be alive (recently used)")
	}
}

func TestOnEvicted(t *testing.T) {
	var evicted []string
	c := New[string](10, sizeOf, func(key string, _ string, _ RemovalReason) {
		evicted = append(evicted, key)
	})
	c.Add("key1", "123456") // 4+6 = 10，正好
	c.Add("k2", "k2")       // 2+2 = 4 → 14 > 10，淘汰 key1
	c.Add("k3", "k3")       // 2+2 = 4 → 8  ≤ 10，不淘汰
	c.Add("k4", "k4")       // 2+2 = 4 → 12 > 10，淘汰 k2

	want := []string{"key1", "k2"}
	if !reflect.DeepEqual(evicted, want) {
		t.Fatalf("evicted = %v, want %v", evicted, want)
	}
	// 顺带验证淘汰后还在的条目
	if _, ok := c.Get("k3"); !ok {
		t.Fatal("k3 should survive")
	}
	if _, ok := c.Get("k4"); !ok {
		t.Fatal("k4 should survive")
	}
}

func TestPeekNoOrderChange(t *testing.T) {
	c := New[string](10, sizeOf, nil)
	c.Add("key1", "a") // 5
	c.Add("key2", "b") // 10

	// Peek 只读，不改顺序
	if _, ok := c.Peek("key1"); !ok {
		t.Fatal("Peek key1 should hit")
	}
	// key1 没被"续命"，所以再插一条时被淘汰的仍是 key1
	c.Add("key3", "c") // 15 > 10
	if _, ok := c.Get("key1"); ok {
		t.Fatal("key1 should have been evicted (Peek does not refresh recency)")
	}
}

func TestRemove(t *testing.T) {
	evicted := 0
	c := New[string](0, sizeOf, func(_ string, _ string, _ RemovalReason) { evicted++ })
	c.Add("key1", "a")
	c.Add("key2", "b")
	c.Remove("key1")

	if _, ok := c.Get("key1"); ok {
		t.Fatal("key1 should be removed")
	}
	if evicted != 1 {
		t.Fatalf("onEvicted called %d times, want 1", evicted)
	}
	if c.Len() != 1 {
		t.Fatalf("Len() = %d, want 1", c.Len())
	}
}

func TestRemovalReasons(t *testing.T) {
	var reasons []RemovalReason
	c := New[string](1, func(string) int { return 0 }, func(_ string, _ string, reason RemovalReason) {
		reasons = append(reasons, reason)
	})

	c.Add("a", "")
	c.Add("b", "") // 容量为 1，淘汰 a。
	c.Remove("b")
	c.Add("c", "")
	c.RemoveWithReason("c", RemovalByExpiration)

	want := []RemovalReason{RemovalByCapacity, RemovalByExplicit, RemovalByExpiration}
	if !reflect.DeepEqual(reasons, want) {
		t.Fatalf("removal reasons = %v, want %v", reasons, want)
	}
}

func TestRemoveIf(t *testing.T) {
	c := New[string](0, sizeOf, nil)
	c.Add("a", "x")
	c.Add("bb", "y")
	c.Add("ccc", "z")

	removed := c.RemoveIf(func(key string, _ string) bool { return len(key) == 1 })
	if removed != 1 {
		t.Fatalf("RemoveIf removed %d, want 1", removed)
	}
	if _, ok := c.Get("a"); ok {
		t.Fatal("key a should be purged")
	}
	if c.Len() != 2 {
		t.Fatalf("Len() = %d, want 2", c.Len())
	}
}

func TestBytes(t *testing.T) {
	c := New[string](0, sizeOf, nil)
	c.Add("key1", "abc") // 4 + 3 = 7
	if c.Bytes() != 7 {
		t.Fatalf("Bytes() = %d, want 7", c.Bytes())
	}
}
