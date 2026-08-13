// 一致性哈希演示（第 4 天）。
//
// 运行：go run ./pkg/consistenthash/example
package main

import (
	"fmt"

	"github.com/bbb3056666-cyber/distcache/pkg/consistenthash"
)

func main() {
	fmt.Println("== 一致性哈希：key 稳定路由到节点 ==")

	// 3 个节点，每个 50 个虚拟节点
	ring := consistenthash.NewMap(50, nil)
	ring.Add("node-8001", "node-8002", "node-8003")

	// 1. 一批 key 分别路由到哪
	fmt.Println("\n-- 路由演示 --")
	for _, key := range []string{"Tom", "Jack", "Sam", "Alice", "Bob", "user-1"} {
		fmt.Printf("  %-8s → %s\n", key, ring.GetNode(key))
	}

	// 2. 加一个节点，看多少 key 搬家（一致性：应该只有一小部分）
	fmt.Println("\n-- 加节点 node-8004 前后对比 --")
	before := map[string]string{}
	for i := 0; i < 100; i++ {
		k := fmt.Sprintf("key-%d", i)
		before[k] = ring.GetNode(k)
	}
	ring.Add("node-8004")
	moved := 0
	for k, oldNode := range before {
		if ring.GetNode(k) != oldNode {
			moved++
		}
	}
	fmt.Printf("  100 个 key 里 %d 个搬家（%.0f%%），其余原地不动\n", moved, float64(moved))

	// 3. 分布均匀性：1000 个 key 各节点分到多少
	fmt.Println("\n-- 虚拟节点让分布均匀 --")
	counts := map[string]int{}
	for i := 0; i < 1000; i++ {
		k := fmt.Sprintf("user-%d", i)
		counts[ring.GetNode(k)]++
	}
	for _, node := range []string{"node-8001", "node-8002", "node-8003", "node-8004"} {
		fmt.Printf("  %-12s 分到 %d 个 key\n", node, counts[node])
	}

	// 4. 删节点：再看分布（一致性另一面）
	fmt.Println("\n-- 删节点 node-8004 后的重新分布 --")
	ring.Remove("node-8004")
	counts2 := map[string]int{}
	for i := 0; i < 1000; i++ {
		counts2[ring.GetNode(fmt.Sprintf("user-%d", i))]++
	}
	for _, node := range []string{"node-8001", "node-8002", "node-8003"} {
		fmt.Printf("  %-12s 分到 %d 个 key\n", node, counts2[node])
	}
}
