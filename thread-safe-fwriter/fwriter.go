package fwriter

import (
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// FWriter 线程安全的文件读写器.
type FWriter struct {
	flock     *FileLock
	writer    *os.File
	fn        string
	tmpSuffix string
}

// NewFWriter 新建FWriter对象.
func NewFWriter(fn string) (*FWriter, error) {
	if err := os.MkdirAll(filepath.Dir(fn), 0750); err != nil {
		return nil, err
	}

	flock := NewFileLock(fn + ".lock")
	if err := flock.Acquire(); err != nil {
		return nil, err
	}

	tmpSuffix := fmt.Sprintf(".tmp%v", time.Now().UnixNano())

	writer, err := os.OpenFile(fn+tmpSuffix, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		flock.Release()
		flock.Remove()
		return nil, err
	}

	return &FWriter{
		flock:     flock,
		writer:    writer,
		fn:        fn,
		tmpSuffix: tmpSuffix,
	}, nil
}

// Write 写文件流.
func (w *FWriter) Write(b []byte) (int, error) {
	return w.writer.Write(b)
}

// Commit 同步内存数据到硬盘.
func (w *FWriter) Commit() error {
	defer w.unlock()

	if err := w.writer.Sync(); err != nil {
		w.exit()
		return err
	}

	if err := os.Rename(w.fn+w.tmpSuffix, w.fn); err != nil {
		w.exit()
		return err
	}

	return w.exit()
}

// Abort 放弃当前写操作.
func (w *FWriter) Abort() error {
	defer w.unlock()
	return w.exit()
}

func (w *FWriter) unlock() {
	w.flock.Release()
	w.flock.Remove()
}

func (w *FWriter) exit() error {
	if err := w.writer.Close(); err != nil {
		return err
	}
	return os.Remove(w.fn + w.tmpSuffix)
}
