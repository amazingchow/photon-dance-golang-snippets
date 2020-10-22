package tpsctrl

import (
	"time"

	"github.com/juju/ratelimit"
	"github.com/rs/zerolog/log"
)

// TPSController 用于控制事务的处理速度
type TPSController struct {
	quotaLimit int
	bucket     *ratelimit.Bucket
}

// NewTPSController 返回TPSController实例.
func NewTPSController(quotalimit int) *TPSController {
	c := TPSController{}
	c.quotaLimit = quotalimit

	interval := time.Second / time.Duration(c.quotaLimit)
	if interval <= 0 {
		interval = time.Nanosecond
	}
	c.bucket = ratelimit.NewBucket(interval, int64(c.quotaLimit))

	return &c
}

// TPSCtrl 从事务桶中取x个令牌, 如果当前无可用令牌, 等待y秒时间, 直到出现可用令牌.
func (c *TPSController) TPSCtrl(count int) {
	if c.bucket == nil {
		return
	}
	waitUntilAvailable := c.bucket.Take(int64(count))
	if waitUntilAvailable != 0 {
		log.Warn().Msgf("tps limit exceeds, wait %s secs until resource turns to be available", waitUntilAvailable.String())
		time.Sleep(waitUntilAvailable)
	}
}
