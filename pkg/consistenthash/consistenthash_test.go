package consistenthash

import (
	"fmt"
	"strconv"
	"testing"
)

// hashAtoi 假哈希函数：把字符串直接当数值。
// 好处：哈希值可手算，测试期望值能精确写出来。
func hashAtoi(key []byte) uint32 {
	i, _ := strconv.Atoi(string(key))
	return uint32(i)
}

// TestHashing 是 GeeCache 原版测试，验证虚拟节点 + 查找 + 加节点后的重映射。
func TestHashing(t *testing.T) {
	// replicas=3：每个节点生成 3 个虚拟点
	hash := NewMap(3, hashAtoi)
	hash.Add("6", "4", "2")

	// 虚拟点哈希值：
	//   "2" → 02,12,22   "4" → 04,14,24   "6" → 06,16,26
	// keys = [2,4,6,12,14,16,22,24,26]

	cases := map[string]string{
		"2":  "2", // hash=2   → 第一个>=2 是 2   → "2"
		"11": "2", // hash=11  → 第一个>=11 是 12 → "2"（虚拟点12属于"2"）
		"23": "4", // hash=23  → 第一个>=23 是 24 → "4"
		"27": "2", // hash=27  → 全<27，绕环回 2  → "2"
	}
	for k, want := range cases {
		if got := hash.GetNode(k); got != want {
			t.Errorf("Get(%s) = %s, want %s", k, got, want)
		}
	}

	// 加节点 "8"：虚拟点 08,18,28 → keys 多了 8,18,28
	hash.Add("8")

	// "27" 之前绕环回"2"，现在第一个>=27 是 28 → "8"
	if got := hash.GetNode("27"); got != "8" {
		t.Errorf("after Add(8), Get(27) = %s, want 8", got)
	}
}

// TestWrapAround 显式验证"绕环"：哈希值比环上所有点都大时回到起点。
func TestWrapAround(t *testing.T) {
	hash := NewMap(1, hashAtoi) // replicas=1 简化
	hash.Add("5", "10")         // keys = [5, 10]

	// hash=999 > 所有点 → sort.Search 返回 2 → 2%2=0 → keys[0]=5
	if got := hash.GetNode("999"); got != "5" {
		t.Fatalf("Get(999) = %s, want 5 (wrapped around)", got)
	}
	// hash=3 < 5 → 第一个>=3 是 5
	if got := hash.GetNode("3"); got != "5" {
		t.Fatalf("Get(3) = %s, want 5", got)
	}
}

// TestRemove 验证删节点后：只有受影响的一段 key 改道，其余不变。
func TestRemove(t *testing.T) {
	hash := NewMap(1, hashAtoi)
	hash.Add("2", "4", "6")

	// 删除前：hash=3 → 第一个>=3 是 4 → "4"
	if got := hash.GetNode("3"); got != "4" {
		t.Fatalf("before remove, Get(3) = %s, want 4", got)
	}

	hash.Remove("4")

	// hash=3 现在第一个>=3 是 6 → "6"（受影响的 key 改道）
	if got := hash.GetNode("3"); got != "6" {
		t.Fatalf("after remove, Get(3) = %s, want 6", got)
	}
	// hash=1 → "2" 不变；hash=5 → "6" 不变；hash=999 绕环 → "2" 不变
	for k, want := range map[string]string{"1": "2", "5": "6", "999": "2"} {
		if got := hash.GetNode(k); got != want {
			t.Fatalf("Get(%s) = %s, want %s (should be unaffected)", k, got, want)
		}
	}
}

// TestEmpty 空环返回空字符串，不 panic（对应 Get 里的 len 判断）。
func TestEmpty(t *testing.T) {
	hash := NewMap(3, nil)
	if got := hash.GetNode("anything"); got != "" {
		t.Fatalf("empty ring Get = %q, want \"\"", got)
	}
}

// TestDistribution 实测证明：虚拟节点让 key 分布更均匀。
// 没有虚拟节点时 3 个节点直接扎堆，分布可能严重倾斜。
func TestDistribution(t *testing.T) {
	const n = 1000
	keys := make([]string, n)
	for i := 0; i < n; i++ {
		keys[i] = fmt.Sprintf("key-%d", i)
	}

	// 返回 3 个节点各分到多少 key
	dist := func(replicas int) []int {
		m := NewMap(replicas, nil) // 用真实 crc32 哈希
		m.Add("node-a", "node-b", "node-c")
		counts := make([]int, 3)
		for _, k := range keys {
			switch m.GetNode(k) {
			case "node-a":
				counts[0]++
			case "node-b":
				counts[1]++
			case "node-c":
				counts[2]++
			}
		}
		return counts
	}

	spread := func(c []int) int { // 最大-最小 = 倾斜程度
		max, min := c[0], c[0]
		for _, v := range c[1:] {
			if v > max {
				max = v
			}
			if v < min {
				min = v
			}
		}
		return max - min
	}

	noVirt := dist(1)
	withVirt := dist(100)
	t.Logf("replicas=1  分布: %v (极差 %d)", noVirt, spread(noVirt))
	t.Logf("replicas=100 分布: %v (极差 %d)", withVirt, spread(withVirt))

	if spread(withVirt) >= spread(noVirt) {
		t.Fatalf("virtual nodes should distribute more evenly")
	}
}
