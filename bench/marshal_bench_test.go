// protobuf vs JSON 序列化压测（benchmark）。
//
// 目的：用数字证明"节点间为什么用 protobuf 而不是 JSON"。
// 跑法：
//
//	go test ./bench/ -bench . -benchmem
//	go test ./bench/ -run TestMarshalSize -v
package bench

import (
	"bytes"
	"encoding/json"
	"testing"

	pb "github.com/bbb3056666-cyber/distcache/pkg/cachepb"

	"google.golang.org/protobuf/proto"
)

// payload 模拟缓存值：192 字节的真实数据（图片/音频/文本都行）。
var payload = bytes.Repeat([]byte("cached-data-"), 16)

// 包级 sink：把结果赋给包级变量，防止编译器把"没用到的结果"优化掉。
var (
	pbSink []byte
	jsSink []byte
)

// jsonResponse 模拟我们缓存里的"一个 value 字段"的 JSON 结构。
type jsonResponse struct {
	Value []byte `json:"value"`
}

// BenchmarkProtoMarshal protobuf 编码：Request → 字节。
func BenchmarkProtoMarshal(b *testing.B) {
	msg := &pb.Response{Value: payload}
	b.ReportAllocs()                // 报告内存分配
	b.SetBytes(int64(len(payload))) // 告诉框架每次处理多少字节,从而算出?MB/s
	for i := 0; i < b.N; i++ {
		pbSink, _ = proto.Marshal(msg)
		//{value: []byte("630")} ->  0a 03 36 33 30(5 字节)
	}
}

//BenchmarkProtoMarshal-20(20 个 CPU 核)    	14954966（b.N）	        81.08 ns/op（单次耗时）
//2368.01 MB/s（吞吐量,每秒能序列化 2368 MB）   208 B/op（每次分配字节）  1 allocs/op（每次分配次数）

// BenchmarkJSONMarshal JSON 编码：结构体 → 字节。
func BenchmarkJSONMarshal(b *testing.B) {
	msg := jsonResponse{Value: payload}
	b.ReportAllocs()
	b.SetBytes(int64(len(payload)))
	for i := 0; i < b.N; i++ {
		jsSink, _ = json.Marshal(msg)
		//{value: []byte("630")} -> {"value":"NjMw"}(16 字节)
	}
}

//BenchmarkJSONMarshal-20   4873818   244.6ns/op
//784.97 MB/s   312 B/op  2 allocs/op

// BenchmarkProtoUnmarshal protobuf 解码：字节 → Response。
func BenchmarkProtoUnmarshal(b *testing.B) {
	//先编码
	msgBytes, _ := proto.Marshal(&pb.Response{Value: payload})
	var out pb.Response
	b.ReportAllocs()
	b.SetBytes(int64(len(payload)))
	for i := 0; i < b.N; i++ {
		_ = proto.Unmarshal(msgBytes, &out)
	}
}

//BenchmarkProtoUnmarshal-20    	12676627	        90.92 ns/op
//2111.67 MB/s	     192 B/op	       1 allocs/op

// BenchmarkJSONUnmarshal JSON 解码：字节 → 结构体。
func BenchmarkJSONUnmarshal(b *testing.B) {
	msgBytes, _ := json.Marshal(jsonResponse{Value: payload})
	var out jsonResponse
	b.ReportAllocs()
	b.SetBytes(int64(len(payload)))
	for i := 0; i < b.N; i++ {
		_ = json.Unmarshal(msgBytes, &out)
	}
}

//BenchmarkJSONUnmarshal-20    	  785828	      1381 ns/op
//139.00 MB/s	     408 B/op	       5 allocs/op

// TestMarshalSize 体积对比：同样数据，两种格式差多少字节。
func TestMarshalSize(t *testing.T) {
	pbBytes, _ := proto.Marshal(&pb.Response{Value: payload})
	jsBytes, _ := json.Marshal(jsonResponse{Value: payload})
	t.Logf("protobuf: %d 字节 | JSON: %d 字节 | JSON 是 protobuf 的 %.1f 倍",
		len(pbBytes), len(jsBytes), float64(len(jsBytes))/float64(len(pbBytes)))
}

//protobuf: 195 字节 | JSON: 268 字节 | JSON 是 protobuf 的 1.4 倍
