// 分布式缓存节点演示（第 5 天）。
//
// 用法：
//
//	# 开 3 个节点（分别在 8001/8002/8003），第 1 个顺带起前端 API
//	.\./run.ps1
//
//	# 或手动分别启动：
//	go run ./core/example -port 8001 -api
//	go run ./core/example -port 8002
//	go run ./core/example -port 8003
//
// 然后通过前端 API 查缓存（key 会自动路由到归属节点）：
//
//	curl "http://localhost:9999/api?key=Tom"   → 630
//	curl "http://localhost:9999/api?key=Jack"  → 589
//
// 也可以直接打某个节点的缓存接口：
//
//	curl http://localhost:8001/_geecache/scores/Tom
package main

import (
	"context"
	"distcache/httppeer"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"strings"

	"distcache/core"
)

// 模拟数据源（真实项目里这里是数据库/文件/RPC）。
// 所有节点共享同一份"逻辑数据库"——谁回源谁查它。
var db = map[string]string{
	"Tom":  "630",
	"Jack": "589",
	"Sam":  "567",
}

// createGroup 建一个 scores 组，回调去"数据源"取数。
func createGroup() *core.Group {
	return core.NewGroup("scores",
		core.GetterFunc(func(ctx context.Context, key string) ([]byte, error) {
			log.Printf("[SlowDB] 查询 key=%q（假装很慢的数据库）", key)
			if v, ok := db[key]; ok {
				return []byte(v), nil
			}
			return nil, core.ErrNotFound
		}),
		core.WithMaxBytes(2<<10), // 容量 2KB
	)
}

// startCacheServer 启动一个分布式缓存节点。
// 三步：建 pool → Set 所有节点（含自己）→ RegisterPeers 挂到 group。
func startCacheServer(addr string, nodes []string, g *core.Group) {
	pool := httppeer.NewHTTPPool(addr)
	pool.Set(nodes...)         // 环上认识所有节点（包括自己）
	g.RegisterPeerPicker(pool) // group 的 load 从此走"先远程"分支

	log.Printf("[cache] 节点 %s 已启动，集群: %v", addr, nodes)
	// addr 形如 "http://localhost:8001"，ListenAndServe 只要 ":8001"
	log.Fatal(http.ListenAndServe(addr[7:], pool))
}

// startAPIServer 前端 API：把 HTTP 请求翻译成 group.Get，对用户屏蔽分布式细节。
func startAPIServer(apiAddr string, g *core.Group) {
	mux := http.NewServeMux()
	mux.HandleFunc("/api", func(w http.ResponseWriter, r *http.Request) {
		key := r.URL.Query().Get("key")
		view, err := g.Get(r.Context(), key)
		if err != nil {
			if errors.Is(err, core.ErrNotFound) {
				http.Error(w, err.Error(), http.StatusNotFound)
				return
			}
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Write(view.ByteSlice())
	})
	log.Printf("[api] 前端服务已启动: %s/api?key=xxx", apiAddr)
	log.Fatal(http.ListenAndServe(apiAddr[7:], mux))
}

func main() {
	var (
		port     int
		api      bool
		apiAddr  string
		nodeList string
	)
	flag.IntVar(&port, "port", 8001, "缓存节点端口")
	flag.BoolVar(&api, "api", false, "是否同时启动前端 API 服务")
	flag.StringVar(&apiAddr, "api-addr", "http://localhost:9999", "前端 API 地址")
	flag.StringVar(&nodeList, "nodes",
		"http://localhost:8001,http://localhost:8002,http://localhost:8003",
		"节点列表(逗号分隔)")
	flag.Parse()

	nodes := strings.Split(nodeList, ",")
	addr := fmt.Sprintf("http://localhost:%d", port)

	g := createGroup()
	if api {
		go startAPIServer(apiAddr, g) // API 服务器和本节点同进程
	}
	startCacheServer(addr, nodes, g)
}
