package core

// Broadcaster 广播失效通知到集群所有节点。
// 接口定义在 core（消费方），实现放 grpcpeer（传输层）。
// 用字符串参数（group, key），不依赖任何序列化格式。
type Broadcaster interface {
	Broadcast(group, key string) error
}
