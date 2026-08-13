package core

// ByteView 持有不可变的缓存字节。
type ByteView struct {
	b []byte
}

// Len 返回字节长度。
func (v ByteView) Len() int {
	return len(v.b)
}

// String 将字节转换为字符串。
func (v ByteView) String() string {
	return string(v.b)
}

// ByteSlice 返回字节副本，避免调用方修改缓存底层数据。
func (v ByteView) ByteSlice() []byte {
	return cloneBytes(v.b)
}

func cloneBytes(b []byte) []byte {
	c := make([]byte, len(b))
	copy(c, b)
	return c
}

// IsEmpty 判断是否为空字节值。
func (v ByteView) IsEmpty() bool {
	return len(v.b) == 0
}

type cacheEntry struct {
	view  ByteView
	found bool
}
