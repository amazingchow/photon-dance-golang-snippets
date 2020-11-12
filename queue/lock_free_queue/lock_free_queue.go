package lockfreequeue

import (
	"sync/atomic"
	"unsafe"
)

// LockFreeQueue 并发安全的无锁队列
type LockFreeQueue struct {
	head  unsafe.Pointer
	tail  unsafe.Pointer
	count int64
}

type node struct {
	v    interface{}
	next unsafe.Pointer
}

// NewLockFreeQueue 返回LockFreeQueue实例.
func NewLockFreeQueue() *LockFreeQueue {
	// n作为dummy node
	n := unsafe.Pointer(&node{})

	// 初始化队列, 使得 head -> dummy node, tail -> dummy node
	return &LockFreeQueue{
		head:  n,
		tail:  n,
		count: 0,
	}
}

// Push 往无锁队列添加数据对象(并发安全).
func (q *LockFreeQueue) Push(elem interface{}) {
	n := &node{v: elem}
	for {
		tail := load(&q.tail)
		next := load(&tail.next)
		if tail == load(&q.tail) { // 第一波检查, 尾还是尾
			if next == nil { // 第二波检查, 其他线程没有插入新数据
				if cas(&tail.next, next, n) { // 第三波检查, 尝试插入新数据到队尾
					if cas(&q.tail, tail, n) { // 入队成功, 移动尾指针
						atomic.AddInt64(&(q.count), 1)
					}
					return
				}
			} else { // 第二波检查失败, 其他线程已插入新数据, 需要移动尾指针
				cas(&q.tail, tail, next)
			}
		}
	}
}

// Pop 从无锁队列取出数据对象(并发安全).
func (q *LockFreeQueue) Pop() (interface{}, bool) {
	for {
		head := load(&q.head)
		tail := load(&q.tail)
		next := load(&head.next)
		if head == load(&q.head) { // 第一波检查, 头还是头
			if head == tail { // 第二波检查, 头和尾重合
				if next == nil { // 第三波检查, 发现是空队列就直接返回
					return nil, false
				}
				// 其他线程已插入新数据, 需要移动尾指针
				cas(&q.tail, tail, next)
			} else {
				// 读取队头数据
				v := next.v
				// 尝试移动头指针, 如果有其他线程取出数据, 则放弃本次尝试
				if cas(&q.head, head, next) {
					// next作为队头的dummy node
					atomic.AddInt64(&(q.count), -1)
					return v, true
				}
			}
		}
	}
}

// Len 返回无锁队列当前长度(并发安全).
func (q *LockFreeQueue) Len() int64 {
	return atomic.LoadInt64(&(q.count))
}

func load(p *unsafe.Pointer) (n *node) {
	return (*node)(atomic.LoadPointer(p))
}

func cas(p *unsafe.Pointer, old, new *node) (ok bool) {
	return atomic.CompareAndSwapPointer(p, unsafe.Pointer(old), unsafe.Pointer(new))
}
