// 并发安全 LRU + TTL 缓存演示（第 2 天）。
//
// 运行：go run ./pkg/cache/example
package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	"distcache/pkg/cache"
)

func main() {
	fmt.Println("== 并发安全缓存：LRU + TTL + 统计 ==")

	sizeOf := func(str string) int { return len(str) }
	// 容量 100 字节，默认 TTL 5 分钟
	c := cache.New[string](100, 5*time.Minute, sizeOf)
	// 启动后台清理：每 30ms 扫一次过期数据
	c.StartJanitor(context.Background(), 30*time.Millisecond)
	defer c.Stop()

	// 1. 基本读写
	c.Add("Tom", "630")
	if v, ok := c.Get("Tom"); ok {
		fmt.Printf("  Get(Tom) = %s\n", v)
	}

	// 2. 短 TTL：这条 50ms 就过期
	c.AddWithTTL("flash", "过期很快", 50*time.Millisecond)
	if v, ok := c.Get("flash"); ok {
		fmt.Printf("  刚写入，Get(flash) = %s\n", v)
	}

	// 3. 并发读写：10 个协程同时打缓存
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := fmt.Sprintf("user-%d", i)
			for j := 0; j < 50; j++ {
				c.Add(key, fmt.Sprintf("%d", i))
				c.Get(key)
			}
		}(i)
	}
	wg.Wait()
	fmt.Println("  10 个协程并发读写完毕（有锁保护，没崩）")

	// 4. 等过期 + janitor 清理
	fmt.Println("  等 100ms，让 flash 过期、让 janitor 清扫...")
	time.Sleep(100 * time.Millisecond)
	if _, ok := c.Get("flash"); ok {
		fmt.Println("  Get(flash) = 居然还在？（不该发生）")
	} else {
		fmt.Println("  Get(flash) = miss（已过期）")
	}

	// 5. 统计快照
	s := c.Stats()
	fmt.Printf("  统计: hits=%d misses=%d evictions=%d len=%d\n",
		s.Hits, s.Misses, s.CapacityRemovals, s.Len)
	fmt.Printf("  命中率 = %.1f%%\n", s.HitRate()*100)
}
