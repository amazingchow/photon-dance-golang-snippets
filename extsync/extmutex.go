package extsync

import (
	"sync"
	"sync/atomic"
	"unsafe"
)

const (
	mutexLocked = 1 << iota // mutex is locked
	mutexWoken
	mutexStarving
	mutexWaiterShift = iota
)

// ExtMutex 互斥锁的拓展版本
type ExtMutex struct {
	sync.Mutex
}

// TryLock ...
func (m *ExtMutex) TryLock() bool {
	if atomic.CompareAndSwapInt32((*int32)(unsafe.Pointer(&m.Mutex)), 0, mutexLocked) {
		return true
	}

	old := atomic.LoadInt32((*int32)(unsafe.Pointer(&m.Mutex)))
	if old&(mutexLocked|mutexWoken|mutexStarving) != 0 {
		return false
	}

	new := old | mutexLocked
	return atomic.CompareAndSwapInt32((*int32)(unsafe.Pointer(&m.Mutex)), old, new)
}

// Count ...
func (m *ExtMutex) Count() int {
	v := atomic.LoadInt32((*int32)(unsafe.Pointer(&m.Mutex)))
	v = v >> mutexWaiterShift
	v = v + (v & mutexLocked)
	return int(v)
}

// IsWoken ...
func (m *ExtMutex) IsWoken() bool {
	v := atomic.LoadInt32((*int32)(unsafe.Pointer(&m.Mutex)))
	return v&mutexWoken == mutexWoken
}

// IsStarving ...
func (m *ExtMutex) IsStarving() bool {
	v := atomic.LoadInt32((*int32)(unsafe.Pointer(&m.Mutex)))
	return v&mutexStarving == mutexStarving
}
