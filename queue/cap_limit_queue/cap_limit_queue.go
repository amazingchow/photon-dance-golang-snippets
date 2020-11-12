package caplimitqueue

import (
	"sync"

	"github.com/gammazero/deque"
)

// CapLimitQueue 限容队列
type CapLimitQueue struct {
	cond *sync.Cond
	q    deque.Deque
	cap  int
}

// NewCapLimitQueue 返回CapLimitQueue实例.
func NewCapLimitQueue(cap int) *CapLimitQueue {
	if cap == 0 {
		cap = 64
	}
	q := &CapLimitQueue{
		cap: cap,
	}
	q.cond = sync.NewCond(&sync.Mutex{})
	return q
}

// Push 往限容队列添加数据对象(并发安全).
func (q *CapLimitQueue) Push(elem interface{}) {
	q.cond.L.Lock()
	for q.q.Len() >= q.cap {
		// (1) 队列已满, 等待消费goroutine取出数据对象.
		q.cond.Wait()
	}
	defer q.cond.L.Unlock()

	q.q.PushBack(elem)
	// (2) 通知消费goroutine已有数据对象进队列 -> (3)
	q.cond.Broadcast()
}

// Pop 从限容队列取出数据对象(并发安全).
func (q *CapLimitQueue) Pop(want int) []interface{} {
	q.cond.L.Lock()
	for q.q.Len() == 0 {
		// (3) 队列为空, 等待生产goroutine添加数据对象.
		q.cond.Wait()
	}
	defer q.cond.L.Unlock()

	if want > q.q.Len() {
		want = q.q.Len()
	}
	output := make([]interface{}, want)
	for i := 0; i < want; i++ {
		output[i] = q.q.PopFront()
	}
	// (4) 通知生产goroutine已有数据对象出队列 -> (1)
	q.cond.Broadcast()

	return output
}

// Len 返回限容队列当前长度(并发安全).
func (q *CapLimitQueue) Len() int {
	q.cond.L.Lock()
	defer q.cond.L.Unlock()
	return q.q.Len()
}
