package core

import "testing"

func TestByteView(t *testing.T) {
	bv := ByteView{b: []byte("hello")}

	if bv.Len() != 5 {
		t.Fatalf("Len() = %d, want 5", bv.Len())
	}
	if bv.String() != "hello" {
		t.Fatalf("String() = %q, want hello", bv.String())
	}

	// ByteSlice 返回副本：改副本不影响原始数据
	s := bv.ByteSlice()
	s[0] = 'X'
	if bv.String() != "hello" {
		t.Fatalf("original corrupted: %q", bv.String())
	}
}

func TestByteViewEmpty(t *testing.T) {
	var bv ByteView
	if !bv.IsEmpty() {
		t.Fatal("zero-value ByteView should be empty")
	}
	if bv.Len() != 0 {
		t.Fatalf("Len() = %d, want 0", bv.Len())
	}
	// 空值也能安全转 string / bytes，不 panic
	if bv.String() != "" || len(bv.ByteSlice()) != 0 {
		t.Fatal("empty view should convert safely")
	}
}
