package batchprocessing

import (
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog/log"
)

const (
	_DefaultBatcherNum         = 1
	_DefaultMaxBatchedSize     = 16
	_DefaultBatcherFlushTimeMs = 200
	_DefaultSourceQueueSize    = 32
	_DefaultBatchedQueueSize   = 32

	_DefaultBatcherPutTimeout   = 10 * time.Second
	_DefaultBatcherCloseTimeout = 5 * time.Second
)

// BatcherGroupCfg 匹处理器组配置
type BatcherGroupCfg struct {
	BatcherNum            int
	BatcherConcurrencyNum int
	MaxBatchedSize        int
	BatcherFlushTimeMs    int
	SourceQueueSize       int
	BatchedQueueSize      int
}

// SourceItem 待处理的事务单元
type SourceItem struct {
	key  string
	item interface{}
}

// BatchedItem 进入批处理队列里待处理的事务单元
type BatchedItem struct {
	CreatedTime time.Time
	Items       []interface{}
}

// DoBatch 匹处理器处理事务的处理函数
type DoBatch func(*BatchedItem) error

// Batcher 匹处理器, 用于批量处理指定的事务
type Batcher struct {
	id              int
	cfg             *BatcherGroupCfg
	sourceQ         chan *SourceItem
	batchedQ        chan *BatchedItem
	doBatch         DoBatch
	flushInterval   time.Duration
	expiredInterval time.Duration
	totalBatched    int64
	done            chan struct{}
}

// BatcherGroup 一组匹处理器
type BatcherGroup []*Batcher

// NewBatcherGroup 返回BatcherGroup实例.
func NewBatcherGroup(cfg *BatcherGroupCfg) BatcherGroup {
	if cfg == nil {
		log.Error().Msg("no batcher")
		return nil
	}
	if cfg.BatcherNum == 0 {
		log.Warn().Msgf("since BatcherNum is zero, we set the default value %d", _DefaultBatcherNum)
		cfg.BatcherNum = _DefaultBatcherNum
	}
	if cfg.BatcherConcurrencyNum == 0 {
		n := runtime.NumCPU()
		log.Warn().Msgf("since BatcherConcurrencyNum is zero, we set the default value %d", n)
		cfg.BatcherConcurrencyNum = n
	}
	if cfg.MaxBatchedSize == 0 {
		log.Warn().Msgf("since MaxBatchedSize is zero, we set the default value %d", _DefaultMaxBatchedSize)
		cfg.MaxBatchedSize = _DefaultMaxBatchedSize
	}
	if cfg.BatcherFlushTimeMs == 0 {
		log.Warn().Msgf("since BatcherFlushTimeMs is zero, we set the default value %d", _DefaultBatcherFlushTimeMs)
		cfg.BatcherFlushTimeMs = _DefaultBatcherFlushTimeMs
	}
	if cfg.SourceQueueSize == 0 {
		log.Warn().Msgf("since SourceQueueSize is zero, we set the default value %d", _DefaultSourceQueueSize)
		cfg.SourceQueueSize = _DefaultSourceQueueSize
	}
	if cfg.BatchedQueueSize == 0 {
		log.Warn().Msgf("since BatchedQueueSize is zero, we set the default value %d", _DefaultBatchedQueueSize)
		cfg.BatchedQueueSize = _DefaultBatchedQueueSize
	}

	bg := make(BatcherGroup, cfg.BatcherNum)
	for i := 0; i < cfg.BatcherNum; i++ {
		b := NewBatcher(i, cfg)
		bg[i] = b
	}
	return bg
}

// Start 开始运行所有的批处理器.
func (bg BatcherGroup) Start(doBatch DoBatch) {
	for _, b := range bg {
		b.Start(doBatch)
	}
}

// Close 停止运行所有的批处理器.
func (bg BatcherGroup) Close() {
	for _, b := range bg {
		b.Close()
	}
}

// Put 将待处理的事务加入批处理器组, 并按照关键词分发给指定的批处理器处理.
func (bg BatcherGroup) Put(key string, job interface{}) error {
	return bg[FNV1av32(key)%uint32(len(bg))].Put(key, job)
}

// Stat 显示所有批处理器的工作状态.
func (bg BatcherGroup) Stat() {
	var total int64
	log.Info().Msgf("/******************** Stat ********************/")
	for i, b := range bg {
		t := b.Stat()
		if t > 0 {
			log.Info().Msgf("[batcher-%d] processed %d batched requests", i, t)
			total += t
		}
	}
	if total > 0 {
		log.Info().Msgf("totally processed %d batched requests", total)
	}
	log.Info().Msgf("/******************** Stat ********************/")
}

// NewBatcher 返回NewBatcher实例.
func NewBatcher(id int, cfg *BatcherGroupCfg) *Batcher {
	return &Batcher{
		id:              id,
		cfg:             cfg,
		sourceQ:         make(chan *SourceItem, cfg.SourceQueueSize),
		batchedQ:        make(chan *BatchedItem, cfg.BatchedQueueSize),
		flushInterval:   time.Duration(cfg.BatcherFlushTimeMs) * time.Millisecond,
		expiredInterval: time.Duration(cfg.BatcherFlushTimeMs*9/10) * time.Millisecond,
		done:            make(chan struct{}),
	}
}

// Start 开始运行批处理器.
func (b *Batcher) Start(doBatch DoBatch) {
	log.Info().Msgf("start batcher-%d", b.id)
	b.doBatch = doBatch
	go b.source()
	go b.sink()
}

// Close 停止运行批处理器.
func (b *Batcher) Close() {
	if b.sourceQ != nil {
		close(b.sourceQ)
		b.sourceQ = nil
	}

	select {
	case <-b.done:
		log.Info().Msgf("batcher-%d has done", b.id)
	case <-time.After(_DefaultBatcherCloseTimeout):
		log.Warn().Msgf("timeout to close batcher-%d", b.id)
	}
}

// Put 将待处理的事务加入批处理器.
func (b *Batcher) Put(key string, job interface{}) error {
	item := &SourceItem{
		key:  key,
		item: job,
	}
	select {
	case b.sourceQ <- item:
	case <-time.After(_DefaultBatcherPutTimeout):
		log.Warn().Msgf("timeout to put item for batcher-%d", b.id)
		return fmt.Errorf("timeout to put item for batcher-%d", b.id)
	}
	return nil
}

func (b *Batcher) source() {
	ticker := time.NewTicker(b.flushInterval)
	defer ticker.Stop()

	batchedTable := make(map[uint32]*BatchedItem)
BATCHER_LOOP:
	for {
		select {
		case item, ok := <-b.sourceQ:
			if !ok {
				log.Warn().Msgf("batcher-%d's source channel has been closed", b.id)
				break BATCHER_LOOP
			}
			k := FNV1av32(item.key) % uint32(b.cfg.BatcherConcurrencyNum)
			if _, exist := batchedTable[k]; !exist {
				batchedTable[k] = &BatchedItem{
					CreatedTime: time.Now(),
					Items:       make([]interface{}, 0, b.cfg.MaxBatchedSize),
				}
			}
			batchedTable[k] = b.appendItemWithFlushOp(batchedTable[k], item)
		case <-ticker.C:
			batchedTable = b.flush(batchedTable, false /* flush */)
		}
	}

	b.flush(batchedTable, true /* flush */)
	// TODO: 在退出前, 如何优雅得处理剩下的事务?
	close(b.batchedQ)
	log.Warn().Msgf("batcher-%d's source goroutine has been closed", b.id)
}

func (b *Batcher) appendItemWithFlushOp(batched *BatchedItem, item *SourceItem) *BatchedItem {
	if batched != nil && len(batched.Items) >= b.cfg.MaxBatchedSize {
		b.batchedQ <- batched
		atomic.AddInt64(&b.totalBatched, 1)

		newBatched := BatchedItem{
			CreatedTime: time.Now(),
		}
		newBatched.Items = batched.Items[:]
		return &newBatched
	}
	batched.Items = append(batched.Items, item.item)
	return batched
}

func (b *Batcher) flush(batchedTable map[uint32]*BatchedItem, flush bool) map[uint32]*BatchedItem {
	now := time.Now()
	for key, batched := range batchedTable {
		if batched != nil && len(batched.Items) > 0 &&
			(flush || batched.CreatedTime.Add(b.expiredInterval).Before(now)) {
			b.batchedQ <- batched
			delete(batchedTable, key)
			atomic.AddInt64(&b.totalBatched, 1)
		}
	}
	return batchedTable
}

func (b *Batcher) sink() {
	var wg sync.WaitGroup
	for i := 0; i < b.cfg.BatcherConcurrencyNum; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for item := range b.batchedQ {
				if err := b.doBatch(item); err != nil {
					log.Error().Err(err).Msgf("batcher-%d does batch job failed", b.id)
				}
			}
			log.Warn().Msgf("batcher-%d's sink channel has been closed", b.id)
		}()
	}
	wg.Wait()
	close(b.done)
	log.Warn().Msgf("batcher-%d's sink goroutine has been closed", b.id)
}

// Stat 返回批处理器已经处理的批次.
func (b *Batcher) Stat() int64 {
	return atomic.LoadInt64(&b.totalBatched)
}

// FNV1av32 哈希函数.
// 参考 http://www.isthe.com/chongo/tech/comp/fnv/index.html#FNV-source
func FNV1av32(key string) uint32 {
	hash := uint32(2166136261)
	const prime32 = uint32(16777619)
	for i := 0; i < len(key); i++ {
		hash ^= uint32(key[i])
		hash *= prime32
	}
	return hash
}
