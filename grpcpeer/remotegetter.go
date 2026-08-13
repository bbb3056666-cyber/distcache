package grpcpeer

import (
	"context"
	"errors"
	"github.com/bbb3056666-cyber/distcache/core"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// RemoteGetter 基于 gRPC 实现 core.PeerGetter。
type RemoteGetter struct {
	conn *grpc.ClientConn
}

func (rg *RemoteGetter) GetFromPeer(ctx context.Context, group, key string) ([]byte, error) {
	if rg.conn == nil {
		return nil, errors.New("grpcpeer: nil remote connection")
	}

	req := &Request{Group: group, Key: key}
	client := NewGroupCacheClient(rg.conn)
	res, err := client.Get(ctx, req)
	if err != nil {
		if status.Code(err) == codes.NotFound {
			return nil, core.ErrNotFound
		}
		return nil, err
	}
	return res.GetValue(), err
}

var _ core.PeerGetter = (*RemoteGetter)(nil)
