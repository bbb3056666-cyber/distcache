package bloom

import (
	"fmt"
	"testing"
)

// TestBasic 基本行为：Add 后必 Test 通过；没 Add 的基本不通过。
func TestBasic(t *testing.T) {
	f := New(100000, 6)

	// Add 后：Test 一定返回 true（布隆过滤器无假阴性）
	f.Add("Tom")
	f.Add("Jack")
	f.Add("Sam")
	for _, key := range []string{"Tom", "Jack", "Sam"} {
		if !f.Test(key) {
			t.Fatalf("Add 过的 %s 应 Test 通过（无假阴性）", key)
		}
	}

	// 没 Add 的：绝大多数返回 false（允许极小概率误判）
	// 用一组 key 测，避免单个 key 恰好命中误判
	falsePositives := 0
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("never-added-%d", i)
		if f.Test(key) {
			falsePositives++
		}
	}
	// 1000 个不存在的 key，误判应该远小于 5%
	if falsePositives > 50 {
		t.Fatalf("误判率过高: %d/1000, want < 50", falsePositives)
	}
}

// TestNoFalseNegative 核心性质：Add 过的 key 绝不能 Test 为 false。
// 用大量 key 验证"无假阴性"这个布隆过滤器的硬保证。
func TestNoFalseNegative(t *testing.T) {
	f := New(1_000_000, 6)

	const n = 10000
	for i := 0; i < n; i++ {
		f.Add(fmt.Sprintf("key-%d", i))
	}
	for i := 0; i < n; i++ {
		if !f.Test(fmt.Sprintf("key-%d", i)) {
			t.Fatalf("key-%d 是 Add 过的，绝不能误判为不存在（假阴性 = 致命错误）", i)
		}
	}
}

// TestFalsePositiveRate 误判率：1 万条数据下，测 1 万个不存在的，误判应在预期范围。
func TestFalsePositiveRate(t *testing.T) {
	// m=8n=80000, k=6 → 理论误判率约 1~2%
	f := New(80000, 6)

	const n = 10000
	for i := 0; i < n; i++ {
		f.Add(fmt.Sprintf("exists-%d", i))
	}

	// 测 1 万个"不在集合里"的 key
	fp := 0
	for i := 0; i < n; i++ {
		if f.Test(fmt.Sprintf("missing-%d", i)) {
			fp++
		}
	}
	rate := float64(fp) / n
	t.Logf("误判率 = %.2f%% (%d/%d)", rate*100, fp, n)

	// 允许一定波动，但不能离谱（理论 ~1-2%，上限给 8%）
	if rate > 0.08 {
		t.Fatalf("误判率 %.2f%% 过高，超过 8%% 上限", rate*100)
	}
}

// TestReuse 同一个过滤器 Add 不同 key，互不干扰地 Test。
func TestReuse(t *testing.T) {
	f := New(10000, 5)
	keys := []string{"a", "bb", "ccc", "dddd"}
	for _, k := range keys {
		f.Add(k)
	}
	for _, k := range keys {
		if !f.Test(k) {
			t.Fatalf("%s 应 Test 通过", k)
		}
	}
}
