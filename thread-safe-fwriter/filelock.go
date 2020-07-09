package fwriter

import (
	"errors"
	"os"
	"syscall"
)

// FileLock 文件锁
type FileLock struct {
	fn string
	fd int
}

// NewFileLock 新建FileLock对象.
func NewFileLock(fn string) *FileLock {
	return &FileLock{
		fn: fn,
	}
}

// Filename 返回文件锁保护的文件.
func (l *FileLock) Filename() string {
	return l.fn
}

// Acquire 获取文件锁.
func (l *FileLock) Acquire() error {
	if err := l.open(); err != nil {
		return err
	}
	err := syscall.Flock(l.fd, syscall.LOCK_EX|syscall.LOCK_NB) // 非阻塞互斥锁
	if err != nil {
		if _err := syscall.Close(l.fd); _err != nil {
			return _err
		}
		if err == syscall.EWOULDBLOCK {
			return errors.New("file locked")
		}
	}
	return nil
}

// Release 释放文件锁.
func (l *FileLock) Release() error {
	return syscall.Close(l.fd)
}

// Remove 删除文件锁保护的文件.
func (l *FileLock) Remove() error {
	return os.Remove(l.fn)
}

func (l *FileLock) open() error {
	fd, err := syscall.Open(l.fn, syscall.O_CREAT|syscall.O_RDONLY, 0600)
	if err != nil {
		return err
	}
	l.fd = fd
	return nil
}
