package pool

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestBasicJob(t *testing.T) {
	ctx := context.Background()
	p := NewPool(10)

	token, err := p.Acquire(ctx)
	assert.Empty(t, err)
	assert.Equal(t, token, 0)

	go func() {
		time.Sleep(2 * time.Second)
		p.Release(token)
	}()

	p.Join()
}

func TestComplexJob(t *testing.T) {
	ctx := context.Background()
	p := NewPool(5)

	for i := 0; i < 10; i++ {
		go func(i int) {
			token, err := p.Acquire(ctx)
			assert.Empty(t, err)
			time.Sleep(3 * time.Second)
			p.Release(token)
		}(i)
	}

	time.Sleep(1 * time.Second)
	p.Join()
}
