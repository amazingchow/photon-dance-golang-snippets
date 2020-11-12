package lockfreequeue

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLockFreeQueue(t *testing.T) {
	lkqueue := NewLockFreeQueue()

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			lkqueue.Push("Lucy")
			wg.Done()
		}()
	}
	successCh := make(chan int, 50)
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			if _, ok := lkqueue.Pop(); ok {
				successCh <- 1
			} else {
				successCh <- 0
			}
			wg.Done()
		}()
	}

	stopCh := make(chan struct{})
	go func() {
		succ := 0
		for x := range successCh {
			succ += x
		}
		assert.Equal(t, int64(100-succ), lkqueue.Len())
		stopCh <- struct{}{}
	}()

	wg.Wait()
	close(successCh)
	<-stopCh

	// TODO: 测试失败？问题出在哪里？
}
