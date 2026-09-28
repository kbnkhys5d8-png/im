package api

import (
	"sync"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/internal/options"
	"github.com/WuKongIM/WuKongIM/internal/types"
	"github.com/WuKongIM/WuKongIM/pkg/wkutil"
)

func TestNewTagRespConcurrentAccessTime(t *testing.T) {
	previousOptions := options.G
	options.G = options.New()
	t.Cleanup(func() { options.G = previousOptions })
	tag := &types.Tag{}
	if got := newTagResp(tag); got.LastGetAt != "" || got.ExpireAt != "" {
		t.Fatal("zero access time must remain empty in the response")
	}
	first := time.Date(2026, 9, 28, 1, 2, 3, 0, time.UTC)
	second := first.Add(5 * time.Minute)
	tag.LastGetTime.Store(first)
	start := make(chan struct{})
	var workers sync.WaitGroup
	workers.Add(1)
	go func() {
		defer workers.Done()
		<-start
		for range 1000 {
			tag.LastGetTime.Store(first)
			tag.LastGetTime.Store(second)
		}
	}()
	close(start)
	for range 1000 {
		got := newTagResp(tag)
		// 两个响应字段必须来自同一次时间快照，不能拼接两次访问的值。
		want := first
		if got.LastGetAt == wkutil.ToyyyyMMddHHmm(second) {
			want = second
		}
		if got.LastGetAt != wkutil.ToyyyyMMddHHmm(want) ||
			got.ExpireAt != wkutil.ToyyyyMMddHHmm(want.Add(options.G.Tag.Expire)) {
			t.Errorf("inconsistent access time response: %+v", got)
			break
		}
	}
	workers.Wait()
}
