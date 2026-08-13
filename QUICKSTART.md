# Quick Start

这份文档展示如何用根包 `distcache` 启动一个可用的缓存节点。

## 1. 创建节点

```go
dc, err := distcache.New(distcache.Config{
	Addr:  "localhost:8001",
	Nodes: []string{"localhost:8001", "localhost:8002", "localhost:8003"},
})
if err != nil {
	log.Fatal(err)
}
defer dc.Stop()
```

`Addr` 是当前节点地址。`Nodes` 是集群节点列表，通常包含当前节点；如果漏写当前节点，`distcache.New` 会自动补上。

## 2. 创建缓存组

```go
scores := dc.NewGroup("scores", distcache.GetterFunc(
	func(ctx context.Context, key string) ([]byte, error) {
		db := map[string]string{
			"Tom":  "630",
			"Jack": "589",
			"Sam":  "567",
		}
		v, ok := db[key]
		if !ok {
			return nil, distcache.ErrNotFound
		}
		return []byte(v), nil
	}),
	distcache.WithTTL(5*time.Minute),
	distcache.WithBloomKeys("Tom", "Jack", "Sam"),
)
```

`NewGroup` 内部会自动接入当前节点的路由和失效广播，不需要调用方手动配置 `core.WithPeerPicker` 或 `core.WithBroadcaster`。

## 3. 启动节点服务

```go
go func() {
	if err := dc.Serve(); err != nil {
		log.Println(err)
	}
}()
```

`Serve` 会启动当前节点的 gRPC 服务、广播流和健康检查循环。

## 4. 读取和删除缓存

```go
value, err := scores.Get(context.Background(), "Tom")
if err != nil {
	if errors.Is(err, distcache.ErrNotFound) {
		fmt.Println("key not found")
		return
	}
	log.Println(err)
	return
}
fmt.Println(value.String())
```

```go
if err := scores.Remove("Tom"); err != nil {
	log.Println(err)
}
```

`Remove` 会删除当前节点缓存，并向其他节点广播失效通知。

## 5. 查看指标

```go
metrics := dc.Metrics()
fmt.Println(metrics.Node.RingNodes)
fmt.Println(metrics.Groups["scores"].CacheHits)
```

`Metrics` 会返回节点指标和所有缓存组的指标快照。

## 完整最小示例

```go
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/bbb3056666-cyber/distcache"
)

func main() {
	dc, err := distcache.New(distcache.Config{
		Addr:  "localhost:8001",
		Nodes: []string{"localhost:8001"},
	})
	if err != nil {
		log.Fatal(err)
	}
	defer dc.Stop()

	scores := dc.NewGroup("scores", distcache.GetterFunc(
		func(ctx context.Context, key string) ([]byte, error) {
			db := map[string]string{
				"Tom":  "630",
				"Jack": "589",
				"Sam":  "567",
			}
			v, ok := db[key]
			if !ok {
				return nil, distcache.ErrNotFound
			}
			return []byte(v), nil
		}),
		distcache.WithTTL(5*time.Minute),
		distcache.WithBloomKeys("Tom", "Jack", "Sam"),
	)

	go func() {
		if err := dc.Serve(); err != nil {
			log.Println(err)
		}
	}()

	value, err := scores.Get(context.Background(), "Tom")
	if err != nil {
		if errors.Is(err, distcache.ErrNotFound) {
			fmt.Println("key not found")
			return
		}
		log.Fatal(err)
	}
	fmt.Println(value.String())
}
```

## Demo 运行命令

如果使用单独的 demo 应用，可以直接启动单节点：

```powershell
cd path\to\your-app
go run . -port 8001 -nodes localhost:8001 -api
```

访问：

```text
http://localhost:9999
```

三节点启动：

```powershell
cd path\to\your-app
go run . -port 8001 -nodes localhost:8001,localhost:8002,localhost:8003 -api
```

```powershell
cd path\to\your-app
go run . -port 8002 -nodes localhost:8001,localhost:8002,localhost:8003
```

```powershell
cd path\to\your-app
go run . -port 8003 -nodes localhost:8001,localhost:8002,localhost:8003
```
