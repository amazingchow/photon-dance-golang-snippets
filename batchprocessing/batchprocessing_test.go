package batchprocessing

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func defaultBatcherGroupCfg() *BatcherGroupCfg {
	return &BatcherGroupCfg{
		BatcherNum: 4,
	}
}

type executor struct{}

func (e *executor) run(item *BatchedItem) error {
	return nil
}

func TestBatcherGroup(t *testing.T) {
	testCases := make([]SourceItem, 1024)
	for i := 0; i < 1024; i++ {
		testCases[i] = SourceItem{
			key:  fmt.Sprintf("key-%d", i%64),
			item: fmt.Sprintf("job-%d", i),
		}
	}
	e := &executor{}

	bg := NewBatcherGroup(defaultBatcherGroupCfg())
	bg.Start(e.run)

	for _, tc := range testCases {
		err := bg.Put(tc.key, tc.item)
		assert.Empty(t, err)
	}

	time.Sleep(5)

	bg.Stat()
	bg.Close()
}
