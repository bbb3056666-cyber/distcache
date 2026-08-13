package core

import (
	"testing"
	"time"
)

func TestLatencyCounters(t *testing.T) {
	var counters latencyCounters
	counters.observe(500 * time.Microsecond)
	counters.observe(2 * time.Millisecond)
	counters.observe(9 * time.Millisecond)
	counters.observe(30 * time.Millisecond)
	counters.observe(100 * time.Millisecond)

	got := counters.snapshot()
	want := LatencyBuckets{
		Under1ms:      1,
		From1To5ms:    1,
		From5To20ms:   1,
		From20To100ms: 1,
		Over100ms:     1,
	}
	if got != want {
		t.Fatalf("snapshot = %+v, want %+v", got, want)
	}
}
