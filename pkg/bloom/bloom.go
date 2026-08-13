package bloom

import (
	"hash/crc32"
	"hash/fnv"
)

// Filter 布隆过滤器：位数组 + 多个哈希函数
// 使用前必须提前加载并插入所有key
type Filter struct {
	bits []uint64 // 位数组（用 uint64 数组实现位集合）
	m    uint64   // 位总数
	k    uint64   // 哈希函数个数
}

//n = 预计放多少个 key
//m = 位数组位数，经验值 m ≈ 8 * n  （8 倍，约 1 bit/key × 8）
//k = 哈希个数，最优 k ≈ (m/n) * ln2 ≈ 5~7

func New(m, k uint64) *Filter {
	// bits 长度 = ceil(m/64)：m 位需要几个 uint64
	return &Filter{
		bits: make([]uint64, (m+63)/64), // m/64 向上取整
		m:    m,
		k:    k,
	}
}

// NewForExpected 按"预计放多少个 key"自动推导参数，让调用方不用操心调参。
// 标准公式：m ≈ 8n 位，k ≈ ln2 × (m/n) ≈ 5.5 → 取 6。
func NewForExpected(n uint64) *Filter {
	if n == 0 {
		n = 1 // m=0 会导致 %0 panic，保底
	}
	return New(n*8, 6)
}

// 用两个独立哈希 + 双哈希技巧，推导出 k 个位置
// 标准公式：gi = (h1 + i*h2) % m
func (f *Filter) hashPositions(s string) []uint64 {
	h1 := uint64(crc32.ChecksumIEEE([]byte(s))) // 哈希1
	fnvHash := fnv.New32a()
	_, _ = fnvHash.Write([]byte(s))
	h2 := uint64(fnvHash.Sum32()) // 哈希2

	//计算k个哈希/f.m的值,存到切片中返回
	positions := make([]uint64, 0, f.k)
	for i := uint64(0); i < f.k; i++ {
		positions = append(positions, (h1+i*h2)%f.m) // 第 i 个位置
	}
	return positions
}

func (f *Filter) setBit(p uint64) {
	f.bits[p/64] |= 1 << (p % 64)
}

func (f *Filter) getBit(pos uint64) bool {
	return f.bits[pos/64]&(1<<(pos%64)) != 0 // 与：看该位是不是 1
}

// Add 添加一个 key
func (f *Filter) Add(s string) {
	for _, pos := range f.hashPositions(s) {
		f.setBit(pos) // 把 k 个位置全部置 1
	}
}

// Test 判断key是否插入过
func (f *Filter) Test(s string) bool {
	for _, pos := range f.hashPositions(s) {
		if !f.getBit(pos) {
			return false // 有一位是 0 → 【一定】不存在
		}
	}
	return true // k 位全 1 → 【可能】存在
}
