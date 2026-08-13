package core

import "context"

// PeerPicker 根据 key 选择负责的远程节点。
type PeerPicker interface {
	PickPeer(key string) (peer PeerGetter, ok bool)
}

// PeerGetter 从远程节点读取缓存值。
type PeerGetter interface {
	GetFromPeer(ctx context.Context, group, key string) ([]byte, error)
}
