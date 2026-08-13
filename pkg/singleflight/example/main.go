// singleflight 演示（第 6 天）：防缓存击穿。
//
// 运行：go run ./pkg/singleflight/example
package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	"distcache/pkg/singleflight"
)

func main() {
	var g singleflight.Group
	var mu sync.Mutex

	// 模拟"回源加载"：记录到底执行了几次
	loadCount := 0
	load := func(key string) (any, error) {
		mu.Lock()
		loadCount++
		mu.Unlock()
		fmt.Printf("  [回源] 加载 %s（模拟查数据库，耗时 100ms）\n", key)
		time.Sleep(100 * time.Millisecond)
		return key + ":630", nil
	}

	fmt.Println("== singleflight：同 key 并发只回源一次 ==")

	// 模拟"缓存刚好过期，50 个请求同时涌来"
	const n = 50
	var wg sync.WaitGroup
	start := make(chan struct{}) // 同时放行，制造真并发
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start // 等所有人就位
			v, err := g.Do(context.Background(), "hot-key", func() (any, error) {
				return load("hot-key")
			})
			if err == nil && i%10 == 0 {
				fmt.Printf("  请求 %d 拿到: %v\n", i, v)
			}
		}(i)
	}
	close(start) // 50 个请求同一瞬间发起
	wg.Wait()

	fmt.Printf("\n结果：%d 个并发请求，数据库只被查了 %d 次（其余 49 个共享结果）\n", n, loadCount)
}
