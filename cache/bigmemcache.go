package bigmemcache

import (
	"time"

	"github.com/allegro/bigcache"
)

const (
	_DefaultEvictionTime   = 100 * 365 * 24 * time.Hour
	_DefaultShardsFactor   = 100
	_DefaultMaxNumsOfShard = 128
	_OneMB                 = 1024 * 1024
)

// BigMemCacheCfg BigMemCache配置
type BigMemCacheCfg struct {
	MaxNumsOfCacheItem uint64 // 最大可缓存的对象数量
	MaxSizeOfCacheItem uint64 // 对象大小, unit is byte
}

func (cfg *BigMemCacheCfg) defaultBigCacheCfg() bigcache.Config {
	bcCfg := bigcache.DefaultConfig(_DefaultEvictionTime)
	bcCfg.Verbose = false

	shardsUpLimit := uint(cfg.MaxNumsOfCacheItem/_DefaultShardsFactor) + 1
	bcCfg.Shards = int(findNearestPowerOf2Num(shardsUpLimit))
	if bcCfg.Shards > _DefaultMaxNumsOfShard {
		bcCfg.Shards = _DefaultMaxNumsOfShard
	}

	// init 10 entries for each shard.
	bcCfg.MaxEntriesInWindow = 10 * bcCfg.Shards
	bcCfg.MaxEntrySize = int(cfg.MaxSizeOfCacheItem)

	bcCfg.HardMaxCacheSize = int((cfg.MaxNumsOfCacheItem*cfg.MaxSizeOfCacheItem)/_OneMB) + 1
	return bcCfg
}

// Feature 范指代AI领域的特征对象.
type Feature struct {
	Version     int32
	ID          int64
	Meta        []byte
	Blob        []byte
	CreatedTime int64
}

// BigMemCache stores serialized item (feature as example) in memory.
// Items are serialized as []byte to avoid excessive GC stress and extra memory footprint.
type BigMemCache struct {
	cache *bigcache.BigCache
}

// NewBigMemCache 返回BigMemCache实例.
func NewBigMemCache(cfg *BigMemCacheCfg) (*BigMemCache, error) {
	cache, err := bigcache.NewBigCache(cfg.defaultBigCacheCfg())
	if err != nil {
		return nil, err
	}
	bmc := BigMemCache{
		cache: cache,
	}
	return &bmc, nil
}

// Add 将特征对象添加进BigMemCache.
func (bmc *BigMemCache) Add(fe *Feature) error {
	k := featureID2HashKey(fe.ID)
	encoded, err := bmc.encode(fe)
	if err != nil {
		return err
	}
	return bmc.cache.Set(k, encoded)
}

// BatchAdd 将批量特征对象添加进BigMemCache.
func (bmc *BigMemCache) BatchAdd(fes []Feature) []Feature {
	failed := make([]Feature, 0, len(fes))
	for _, fe := range fes {
		if err := bmc.Add(&fe); err != nil {
			failed = append(failed, fe)
		}
	}
	return failed
}

// Del 将特征对象从BigMemCache删除.
func (bmc *BigMemCache) Del(id int64) error {
	k := featureID2HashKey(id)
	// Delete() is mark-deletion in bigcache
	return bmc.cache.Delete(k)
}

// BatchDel 将批量特征对象从BigMemCache删除.
func (bmc *BigMemCache) BatchDel(ids []int64) []int64 {
	failed := make([]int64, 0, len(ids))
	for _, id := range ids {
		if err := bmc.Del(id); err != nil {
			failed = append(failed, id)
		}
	}
	return failed
}

// Get 从BigMemCache中获取特征对象.
func (bmc *BigMemCache) Get(id int64) *Feature {
	k := featureID2HashKey(id)
	v, err := bmc.cache.Get(k)
	if err != nil || v == nil {
		return nil
	}
	fe, err := bmc.decode(v)
	if err != nil {
		return nil
	}
	return fe
}

// Size 返回BigMemCache当前缓存的对象数量.
func (bmc *BigMemCache) Size() int {
	return bmc.cache.Len()
}

// Reset 真正意义上去清理缓存.
func (bmc *BigMemCache) Reset() error {
	return bmc.cache.Reset()
}
