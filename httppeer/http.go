// Package httppeer 通过 HTTP 暴露缓存节点能力。
package httppeer

import (
	"context"
	"distcache/core"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	pb "distcache/pkg/cachepb"
	"distcache/pkg/consistenthash"

	"google.golang.org/protobuf/proto"
)

const defaultBasePath = "/_geecache/"
const defaultReplicas = 50

// HTTPPool 同时实现 HTTP 节点服务端和节点选择器。
type HTTPPool struct {
	self     string
	basePath string

	client      *http.Client
	mu          sync.RWMutex
	peers       *consistenthash.Map
	httpGetters map[string]*httpGetter
}

func NewHTTPPool(self string) *HTTPPool {
	return &HTTPPool{
		self:     self,
		basePath: defaultBasePath,
		client:   &http.Client{Timeout: 3 * time.Second},
	}
}

func (p *HTTPPool) Log(format string, v ...any) {
	slog.Debug(
		fmt.Sprintf(format, v...),
		"component", "httppeer",
		"peer", p.self,
	)
}

// ServeHTTP 处理其他节点发来的读取请求。
func (p *HTTPPool) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if !strings.HasPrefix(r.URL.Path, p.basePath) {
		http.Error(w, "unexpected path: "+r.URL.Path, http.StatusBadRequest)
		return
	}
	p.Log("%s %s", r.Method, r.URL.Path)

	parts := strings.SplitN(strings.TrimPrefix(r.URL.Path, p.basePath), "/", 2)
	if len(parts) != 2 {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	groupName, key := parts[0], parts[1]

	group := core.GetGroup(groupName)
	if group == nil {
		http.Error(w, "no such group: "+groupName, http.StatusNotFound)
		return
	}

	view, err := group.Get(r.Context(), key)
	if err != nil {
		if errors.Is(err, core.ErrNotFound) {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	body, err := proto.Marshal(&pb.Response{Value: view.ByteSlice()})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	_, _ = w.Write(body)
}

// Set 配置节点列表。
func (p *HTTPPool) Set(nodes ...string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.peers = consistenthash.NewMap(defaultReplicas, nil)
	p.peers.Add(nodes...)
	p.httpGetters = make(map[string]*httpGetter, len(nodes))
	for _, node := range nodes {
		p.httpGetters[node] = &httpGetter{
			baseURL: node + p.basePath,
			client:  p.client,
		}
	}
}

// PickPeer 返回负责该 key 的远程节点。
func (p *HTTPPool) PickPeer(key string) (core.PeerGetter, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if p.peers == nil {
		return nil, false
	}
	if node := p.peers.GetNode(key); node != "" && node != p.self {
		p.Log("key %q routed to peer %s", key, node)
		return p.httpGetters[node], true
	}
	return nil, false
}

type httpGetter struct {
	baseURL string
	client  *http.Client
}

func (h *httpGetter) GetFromPeer(ctx context.Context, group, key string) ([]byte, error) {
	u := fmt.Sprintf("%s%s/%s", h.baseURL, url.QueryEscape(group), url.QueryEscape(key))

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}

	httpRes, err := h.client.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("core: remote request failed: %w", err)
	}
	defer httpRes.Body.Close()

	if httpRes.StatusCode != http.StatusOK {
		if httpRes.StatusCode == http.StatusNotFound {
			return nil, core.ErrNotFound
		}
		return nil, fmt.Errorf("core: remote returned %s", httpRes.Status)
	}

	body, err := io.ReadAll(httpRes.Body)
	if err != nil {
		return nil, fmt.Errorf("core: read remote response: %w", err)
	}
	res := &pb.Response{}
	if err := proto.Unmarshal(body, res); err != nil {
		return nil, fmt.Errorf("core: decode remote response: %w", err)
	}
	return res.GetValue(), nil
}

var _ core.PeerPicker = (*HTTPPool)(nil)
var _ core.PeerGetter = (*httpGetter)(nil)
