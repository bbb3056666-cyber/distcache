// LRU 缓存演示（第 1 天）。
//
// 运行：go run ./pkg/lru/example
package main

import (
	"fmt"

	"distcache/pkg/lru"
)

func main() {
	// 容量 12 字节：len(key) + len(value) 之和
	// 最后一个参数是"淘汰回调"，被淘汰时打印一行
	c := lru.New[string](12, func(s string) int { return len(s) }, func(key, value string, _ lru.RemovalReason) {
		fmt.Printf("  [淘汰] %q = %q\n", key, value)
	})

	fmt.Println("== LRU 缓存：O(1) 的 Get/Add，容量满时淘汰最久未用 ==")

	c.Add("Tom", "630") // 3+3=6
	fmt.Println("  Add Tom=630    (共 6 字节)")

	c.Add("Jack", "589") // 4+3=7 → 总 13 > 12，淘汰 Tom
	fmt.Println("  Add Jack=589   (共 13 字节 → 超了!)")

	c.Add("Sam", "567") // 3+3=6 → 7+6=13，淘汰 Jack
	fmt.Println("  Add Sam=567    (又超了!)")

	// Get 会"续命"：Sam 变成最近使用
	if v, ok := c.Get("Sam"); ok {
		fmt.Printf("  Get(Sam) = %s（Sam 续命，变最近使用）\n", v)
	}

	c.Add("Ben", "111") // 3+3=6 → 6+6=12，刚好，不淘汰
	fmt.Println("  Add Ben=111    (共 12 字节，刚好放下)")

	for _, k := range []string{"Tom", "Jack", "Sam", "Ben"} {
		if v, ok := c.Get(k); ok {
			fmt.Printf("  %-4s = %s  ✓ 在缓存里\n", k, v)
		} else {
			fmt.Printf("  %-4s = 已被淘汰\n", k)
		}
	}
}
