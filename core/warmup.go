package core

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
)

// WarmUpResult 汇总一次缓存预热的执行结果。
type WarmUpResult struct {
	Requested int
	Attempted int
	Succeeded int
	NotFound  int
	Failed    int
}

// WarmUp 使用固定数量的 worker 预加载指定 key，并复用完整的 Get 流程。
func (g *Group) WarmUp(ctx context.Context, keys []string, concurrency int) (WarmUpResult, error) {
	result := WarmUpResult{Requested: len(keys)}
	if concurrency <= 0 {
		return result, errors.New("core: warm-up concurrency must be positive")
	}
	if len(keys) == 0 {
		return result, nil
	}
	if concurrency > len(keys) {
		concurrency = len(keys)
	}

	jobs := make(chan string)
	var wg sync.WaitGroup
	var attempted atomic.Int64
	var succeeded atomic.Int64
	var notFound atomic.Int64
	var failed atomic.Int64

	wg.Add(concurrency)
	for range concurrency {
		go func() {
			defer wg.Done()
			for key := range jobs {
				attempted.Add(1)
				_, err := g.Get(ctx, key)
				switch {
				case err == nil:
					succeeded.Add(1)
				case errors.Is(err, ErrNotFound):
					notFound.Add(1)
				default:
					failed.Add(1)
				}
			}
		}()
	}

sendLoop:
	for _, key := range keys {
		select {
		case jobs <- key:
		case <-ctx.Done():
			break sendLoop
		}
	}
	close(jobs)
	wg.Wait()

	result.Attempted = int(attempted.Load())
	result.Succeeded = int(succeeded.Load())
	result.NotFound = int(notFound.Load())
	result.Failed = int(failed.Load())
	return result, ctx.Err()
}
