package pool

import (
	"context"
	"fmt"
	"sync"
	"time"
)

type Pool struct {
	mu          sync.Mutex
	concurrency int
	q           chan int
	join        bool
	pend        int
}

func NewPool(concurrency int) *Pool {
	p := Pool{
		concurrency: concurrency,
		q:           make(chan int, concurrency+1),
		join:        false,
		pend:        0,
	}
	for i := 0; i < concurrency; i++ {
		p.q <- i
		p.pend++
	}
	return &p
}

func (p *Pool) Acquire(ctx context.Context) (int, error) {
	p.mu.Lock()
	if p.join {
		return -1, fmt.Errorf("pool is abort to close")
	}
	p.mu.Unlock()

	select {
	case <-ctx.Done():
		return -1, ctx.Err()
	case token := <-p.q:
		p.mu.Lock()
		p.pend--
		p.mu.Unlock()
		return token, nil
	}
}

func (p *Pool) Release(token int) {
	p.mu.Lock()
	p.pend++
	p.mu.Unlock()
	p.q <- token
}

func (p *Pool) Join() {
	p.mu.Lock()
	p.join = true
	p.mu.Unlock()

	for {
		time.Sleep(time.Second * 1)
		p.mu.Lock()
		if p.pend == p.concurrency { // wait until all workers release token
			p.mu.Unlock()
			break
		} else {
			p.mu.Unlock()
		}
	}
}
