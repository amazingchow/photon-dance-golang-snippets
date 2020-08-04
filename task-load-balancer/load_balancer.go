package taskloadbalancer

import (
	"math"
	"sync"

	log "github.com/rs/zerolog/log"
)

// TaskLoadBalancer 用于分配任务执行单元的负载均衡器
type TaskLoadBalancer struct {
	mu      sync.Mutex
	workers map[interface{}]int32
}

// NewTaskLoadBalancer 新建TaskLoadBalancer实例.
func NewTaskLoadBalancer(limit uint32) *TaskLoadBalancer {
	return &TaskLoadBalancer{
		workers: make(map[interface{}]int32, limit),
	}
}

// AddWorker 往负载均衡器中添加任务执行单元.
func (lb *TaskLoadBalancer) AddWorker(worker interface{}) {
	lb.mu.Lock()
	defer lb.mu.Unlock()

	lb.workers[worker] = 0
}

// GetWorker 从负载均衡器内获取可用的任务执行单元.
func (lb *TaskLoadBalancer) GetWorker() interface{} {
	lb.mu.Lock()
	defer lb.mu.Unlock()

	var selectedWorker interface{}

	var minUsage = int32(math.MaxInt32)
	for worker, usage := range lb.workers {
		if usage < minUsage {
			selectedWorker = worker
			minUsage = usage
		}
	}

	lb.workers[selectedWorker]++

	return selectedWorker
}

// ReleaseWorkerUsage 使用者归还任务执行单元.
func (lb *TaskLoadBalancer) ReleaseWorkerUsage(worker interface{}) {
	lb.mu.Lock()
	defer lb.mu.Unlock()

	lb.workers[worker]--
	if lb.workers[worker] < 0 {
		lb.workers[worker] = 0
		log.Warn().Msg("no need to release worker usage")
	}
}
