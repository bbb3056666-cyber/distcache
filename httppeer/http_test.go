package httppeer

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/bbb3056666-cyber/distcache/core"
	pb "github.com/bbb3056666-cyber/distcache/pkg/cachepb"

	"google.golang.org/protobuf/proto"
)

// testDB 本测试包的测试数据（每个测试包有自己的，不访问 core 的私有变量）
var testDB = map[string]string{
	"Tom":  "630",
	"Jack": "589",
	"Sam":  "567",
}

// TestHTTPServer 用 httptest 起假的 HTTP 服务，验证路由解析和各种错误分支。
func TestHTTPServer(t *testing.T) {
	// NewGroup 会把 group 注册进全局表，pool.ServeHTTP 通过名字找到它
	core.NewGroup("scores", core.GetterFunc(func(ctx context.Context, key string) ([]byte, error) {
		if v, ok := testDB[key]; ok {
			return []byte(v), nil
		}
		return nil, core.ErrNotFound
	}))

	ts := httptest.NewServer(NewHTTPPool("http://localhost:8001"))
	defer ts.Close()

	base := ts.URL + "/_geecache/scores/"

	// ① 正常命中（响应是 protobuf 编码，要解码出 Value）
	resp, err := http.Get(base + "Tom")
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("Tom status = %d, want 200", resp.StatusCode)
	}
	var out pb.Response
	if err := proto.Unmarshal(body, &out); err != nil {
		t.Fatalf("protobuf 解码失败: %v", err)
	}
	if string(out.GetValue()) != "630" {
		t.Fatalf("Tom value = %q, want 630", out.GetValue())
	}

	// ② 不存在的 key → 404
	resp, err = http.Get(base + "Nobody")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("Nobody status = %d, want 404", resp.StatusCode)
	}

	// ③ 不存在的组 → 404
	resp, err = http.Get(ts.URL + "/_geecache/nogroup/whatever")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("no group status = %d, want 404", resp.StatusCode)
	}

	// ④ 路径只有一段（缺组名或 key）→ 400
	resp, err = http.Get(ts.URL + "/_geecache/onlyone")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("bad path status = %d, want 400", resp.StatusCode)
	}

	// ⑤ 前缀不对 → 400
	resp, err = http.Get(ts.URL + "/wrong/scores/Tom")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("wrong prefix status = %d, want 400", resp.StatusCode)
	}
}

// TestHTTPCacheHit 通过真实 HTTP 请求验证：第二次请求命中缓存，不再回调数据源。
func TestHTTPCacheHit(t *testing.T) {
	loadCount := 0
	core.NewGroup("scores", core.GetterFunc(func(ctx context.Context, key string) ([]byte, error) {
		loadCount++
		if v, ok := testDB[key]; ok {
			return []byte(v), nil
		}
		return nil, core.ErrNotFound
	}))

	ts := httptest.NewServer(NewHTTPPool("http://localhost:8001"))
	defer ts.Close()

	base := ts.URL + "/_geecache/scores/"

	// 第一次请求：miss → 回调加载 → 缓存
	// 第二次请求：命中 → 直接返回，不再回调
	for i := 0; i < 2; i++ {
		resp, err := http.Get(base + "Tom")
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		var out pb.Response
		if err := proto.Unmarshal(body, &out); err != nil {
			t.Fatalf("protobuf 解码失败: %v", err)
		}
		if string(out.GetValue()) != "630" {
			t.Fatalf("request %d value = %q, want 630", i+1, out.GetValue())
		}
	}

	if loadCount != 1 {
		t.Fatalf("loadCount = %d, want 1 (second request should hit cache)", loadCount)
	}
}
