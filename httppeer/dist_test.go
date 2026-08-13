package httppeer

import (
	"context"
	"github.com/bbb3056666-cyber/distcache/core"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	pb "github.com/bbb3056666-cyber/distcache/pkg/cachepb"
	"github.com/bbb3056666-cyber/distcache/pkg/consistenthash"

	"google.golang.org/protobuf/proto"
)

// TestDistributed 核心集成测试：3 个节点组成的分布式缓存。
//
// 验证目标：
//  1. 每个 key 只由"环上归属它的那个节点"回源（loads[owner]==1）
//  2. 其他节点查不到时去 owner 取，自己【不】回源（loads[其他]==0）
//  3. 任何节点查询都能返回正确值
//
// 注意：真实部署是"每进程一个节点"，各节点的 group 都是同名的 scores。
// 单进程测试里如果也用全局注册表按名查，3 个同名 group 会互相覆盖（GetGroup
// 只返回最后一个），导致无限转发。所以这里服务器 handler 直接用本节点的
// group，不经过全局注册表——这是测试的模拟手段，不改变被测的分布式逻辑。
func TestDistributed(t *testing.T) {
	// 每个节点自己的加载计数（按节点 × key）
	loads := make([]map[string]int, 3)
	for i := range loads {
		loads[i] = make(map[string]int)
	}
	var mu sync.Mutex

	// 建 3 个 Group，getter 里给"本节点"计数
	groups := make([]*core.Group, 3)
	for i := 0; i < 3; i++ {
		idx := i
		groups[i] = core.NewGroup("scores", core.GetterFunc(func(ctx context.Context, key string) ([]byte, error) {
			mu.Lock()
			loads[idx][key]++
			mu.Unlock()
			if v, ok := testDB[key]; ok {
				return []byte(v), nil
			}
			return nil, core.ErrNotFound
		}))
	}

	// 起 3 个假 HTTP 服务：handler 直接调"本节点的 group"
	servers := make([]*httptest.Server, 3)
	for i := 0; i < 3; i++ {
		idx := i
		servers[i] = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// 路径: /_geecache/<group>/<key>，这里只取 key，group 名字忽略
			parts := strings.SplitN(strings.TrimPrefix(r.URL.Path, defaultBasePath), "/", 2)
			if len(parts) != 2 {
				http.Error(w, "bad request", http.StatusBadRequest)
				return
			}
			view, err := groups[idx].Get(r.Context(), parts[1])
			if err != nil {
				http.Error(w, err.Error(), http.StatusNotFound)
				return
			}
			// 模拟真实节点：响应用 protobuf 编码（和 httpGetter 的 proto.Unmarshal 对上）
			body, err := proto.Marshal(&pb.Response{Value: view.ByteSlice()})
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			w.Write(body)
		}))
	}
	addrs := make([]string, 3)
	for i := 0; i < 3; i++ {
		addrs[i] = servers[i].URL
	}

	// 每个节点配 pool（仅作 PeerPicker，不负责服务），并挂到 group 上
	pools := make([]*HTTPPool, 3)
	for i := 0; i < 3; i++ {
		pools[i] = NewHTTPPool(addrs[i])
		pools[i].Set(addrs...) // 环上认识全部节点
		groups[i].RegisterPeerPicker(pools[i])
	}

	// 用和实现相同的哈希环，算出每个 key 的"法定归属节点"
	ref := consistenthash.NewMap(defaultReplicas, nil)
	ref.Add(addrs...)
	ownerIdx := func(key string) int {
		owner := ref.GetNode(key)
		for i, a := range addrs {
			if a == owner {
				return i
			}
		}
		return -1
	}

	// 辅助：从节点 i 请求一个 key，返回解码后的响应值
	getVia := func(i int, key string) string {
		resp, err := http.Get(addrs[i] + "/_geecache/scores/" + key)
		if err != nil {
			t.Fatalf("节点%d请求失败: %v", i, err)
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		var out pb.Response
		if err := proto.Unmarshal(body, &out); err != nil {
			t.Fatalf("节点%d protobuf 解码失败: %v", i, err)
		}
		return string(out.GetValue())
	}

	// 对每个存在的 key：从 3 个节点各查一遍
	// range map 第一个返回值是 key，第二个是 value
	for key, want := range testDB {
		owner := ownerIdx(key)

		for i := 0; i < 3; i++ {
			if got := getVia(i, key); got != want {
				t.Fatalf("key %s 从节点%d拿到 %q, want %q", key, i, got, want)
			}
		}

		// 只有 owner 回源了一次
		if loads[owner][key] != 1 {
			t.Fatalf("key %s 归属节点%d, 但回源了 %d 次, want 1",
				key, owner, loads[owner][key])
		}
		for i := 0; i < 3; i++ {
			if i != owner && loads[i][key] != 0 {
				t.Fatalf("key %s 非归属节点%d 也回源了 %d 次, want 0",
					key, i, loads[i][key])
			}
		}
	}
}
