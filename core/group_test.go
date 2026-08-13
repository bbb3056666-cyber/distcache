package core

import (
	"context"
	"errors"
	"testing"
	"time"
)

var testDB = map[string]string{
	"Tom":  "630",
	"Jack": "589",
	"Sam":  "567",
}

type latencyTestPeerPicker struct {
	peer PeerGetter
}

func (p latencyTestPeerPicker) PickPeer(string) (PeerGetter, bool) {
	return p.peer, true
}

type latencyTestPeerGetter struct{}

func (latencyTestPeerGetter) GetFromPeer(context.Context, string, string) ([]byte, error) {
	return []byte("remote-value"), nil
}

// TestGetter 验证"接口型函数"：
// 一个普通函数，通过 GetterFunc 类型转换就能当成 LocalGetter 接口用。
func TestGetter(t *testing.T) {
	var f LocalGetter = GetterFunc(func(ctx context.Context, key string) ([]byte, error) {
		return []byte(key), nil
	})
	if v, _ := f.Get(context.Background(), "key"); string(v) != "key" {
		t.Fatalf("callback failed, got %q", v)
	}
}

// TestGet 验证主流程：
//  1. 第一次 Get → miss → 回调加载 → 缓存
//  2. 第二次 Get → 命中 → 不再回调（loadCount 保持 1）
//  3. 不存在的 key → 返回错误
func TestGet(t *testing.T) {
	loadCounts := make(map[string]int, len(testDB))
	g := NewGroup("scores", GetterFunc(
		func(ctx context.Context, key string) ([]byte, error) {
			loadCounts[key]++
			if v, ok := testDB[key]; ok {
				return []byte(v), nil
			}
			return nil, errors.New("no such key: " + key)
		}),
		WithMaxBytes(2<<10),
	)

	ctx := context.Background()
	for k, want := range testDB {
		// 第一次：加载并缓存
		if v, err := g.Get(ctx, k); err != nil || v.String() != want {
			t.Fatalf("Get(%s) = %q, %v", k, v, err)
		}
		// 第二次：命中，不再回调
		if v, err := g.Get(ctx, k); err != nil || v.String() != want {
			t.Fatalf("Get(%s) cache = %q, %v", k, v, err)
		}
		if loadCounts[k] != 1 {
			t.Fatalf("key %s loaded %d times, want 1 (cache should hit)", k, loadCounts[k])
		}
	}

	// 不存在的 key：返回错误（注意：我们的 getter 没返回 ErrNotFound，
	// 这里先验证"错误能透传"；ErrNotFound 的用法后面加）
	if _, err := g.Get(ctx, "unknown"); err == nil {
		t.Fatal("Get(unknown) should error")
	}
}

// TestGetGroup 验证全局注册表。
func TestGetGroup(t *testing.T) {
	name := "test-group"
	NewGroup(name, GetterFunc(func(ctx context.Context, key string) ([]byte, error) {
		return nil, nil
	}))

	if g := GetGroup(name); g == nil || g.name != name {
		t.Fatalf("GetGroup(%s) = %v, want registered group", name, g)
	}
	if g := GetGroup("not-exist"); g != nil {
		t.Fatalf("GetGroup(not-exist) = %v, want nil", g)
	}
}

// TestEmptyKey 空 key 直接拒绝，不走到缓存/回调。
func TestEmptyKey(t *testing.T) {
	g := NewGroup("empty-key-test", GetterFunc(func(ctx context.Context, key string) ([]byte, error) {
		return []byte(key), nil
	}))
	if _, err := g.Get(context.Background(), ""); err == nil {
		t.Fatal("empty key should error")
	}
}

func TestMetrics(t *testing.T) {
	g := NewGroup("metrics-test", GetterFunc(func(ctx context.Context, key string) ([]byte, error) {
		if key == "known" {
			return []byte("value"), nil
		}
		return nil, ErrNotFound
	}), WithBloomKeys("known"))

	if _, err := g.Get(context.Background(), "missing"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get(missing) error = %v, want ErrNotFound", err)
	}
	if _, err := g.Get(context.Background(), "known"); err != nil {
		t.Fatal(err)
	}
	if _, err := g.Get(context.Background(), "known"); err != nil {
		t.Fatal(err)
	}

	m := g.Metrics()
	if m.CacheHits != 1 || m.CacheMisses != 2 {
		t.Fatalf("cache metrics = %+v, want one hit and two misses", m)
	}
	if m.BloomRejected != 1 || m.LocalLoads != 1 || m.LoadErrors != 0 {
		t.Fatalf("load metrics = %+v, want bloom=1 local=1 errors=0", m)
	}
	if m.SingleflightLoads != 1 || m.SingleflightShared != 0 || m.SingleflightInFlight != 0 {
		t.Fatalf("singleflight metrics = %+v, want loads=1 shared=0 inFlight=0", m)
	}
	loadLatencyCount := m.LoadLatency.Under1ms + m.LoadLatency.From1To5ms + m.LoadLatency.From5To20ms + m.LoadLatency.From20To100ms + m.LoadLatency.Over100ms
	if loadLatencyCount != 1 {
		t.Fatalf("load latency buckets = %+v, want one recorded load", m.LoadLatency)
	}
	localLoadLatencyCount := m.LocalLoadLatency.Under1ms + m.LocalLoadLatency.From1To5ms + m.LocalLoadLatency.From5To20ms + m.LocalLoadLatency.From20To100ms + m.LocalLoadLatency.Over100ms
	if localLoadLatencyCount != 1 {
		t.Fatalf("local load latency buckets = %+v, want one recorded local load", m.LocalLoadLatency)
	}
	peerReadLatencyCount := m.PeerReadLatency.Under1ms + m.PeerReadLatency.From1To5ms + m.PeerReadLatency.From5To20ms + m.PeerReadLatency.From20To100ms + m.PeerReadLatency.Over100ms
	if peerReadLatencyCount != 0 {
		t.Fatalf("peer read latency buckets = %+v, want no peer read", m.PeerReadLatency)
	}
	if m.Entries != 1 || m.Bytes == 0 {
		t.Fatalf("cache size metrics = %+v, want one non-empty entry", m)
	}
}

func TestPeerReadLatencyMetrics(t *testing.T) {
	g := NewGroup(
		"metrics-peer-latency",
		GetterFunc(func(context.Context, string) ([]byte, error) {
			t.Fatal("local getter should not run after a successful peer read")
			return nil, nil
		}),
		WithPeerPicker(latencyTestPeerPicker{peer: latencyTestPeerGetter{}}),
	)
	defer g.Close()

	value, err := g.Get(context.Background(), "key")
	if err != nil || value.String() != "remote-value" {
		t.Fatalf("Get() = (%q, %v), want remote value", value.String(), err)
	}

	m := g.Metrics()
	peerReadLatencyCount := m.PeerReadLatency.Under1ms + m.PeerReadLatency.From1To5ms + m.PeerReadLatency.From5To20ms + m.PeerReadLatency.From20To100ms + m.PeerReadLatency.Over100ms
	if m.PeerReads != 1 || peerReadLatencyCount != 1 {
		t.Fatalf("peer metrics = %+v, want one peer read and one latency record", m)
	}
	localLoadLatencyCount := m.LocalLoadLatency.Under1ms + m.LocalLoadLatency.From1To5ms + m.LocalLoadLatency.From5To20ms + m.LocalLoadLatency.From20To100ms + m.LocalLoadLatency.Over100ms
	if m.LocalLoads != 0 || localLoadLatencyCount != 0 {
		t.Fatalf("local metrics = %+v, want no local load", m)
	}
}

func TestGroupRemovalMetrics(t *testing.T) {
	getter := GetterFunc(func(context.Context, string) ([]byte, error) {
		return []byte("v"), nil
	})

	capacityGroup := NewGroup("metrics-capacity", getter, WithMaxBytes(3), WithTTL(0))
	defer capacityGroup.Close()
	if _, err := capacityGroup.Get(context.Background(), "a"); err != nil {
		t.Fatal(err)
	}
	if _, err := capacityGroup.Get(context.Background(), "b"); err != nil {
		t.Fatal(err)
	}
	if got := capacityGroup.Metrics().CapacityRemovals; got != 1 {
		t.Fatalf("CapacityRemovals = %d, want 1", got)
	}

	if err := capacityGroup.Remove("b"); err != nil {
		t.Fatal(err)
	}
	if got := capacityGroup.Metrics().ExplicitRemovals; got != 1 {
		t.Fatalf("ExplicitRemovals = %d, want 1", got)
	}

	expiryGroup := NewGroup("metrics-expiry", getter, WithTTL(10*time.Millisecond))
	defer expiryGroup.Close()
	if _, err := expiryGroup.Get(context.Background(), "x"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)
	if _, err := expiryGroup.Get(context.Background(), "x"); err != nil {
		t.Fatal(err)
	}
	if got := expiryGroup.Metrics().ExpiredRemovals; got != 1 {
		t.Fatalf("ExpiredRemovals = %d, want 1", got)
	}
}
