package microservice

import (
	"reflect"
)

// OrDone 一种信号通知模式,
// 如果有多个同质微服务任务要执行, 只要其中任意一个任务执行完毕, 就通知上游/下游服务.
func OrDone(notifyChans ...<-chan interface{}) <-chan interface{} {
	// 特殊情况, 只有0个或者1个微服务
	switch len(notifyChans) {
	case 0:
		return nil
	case 1:
		return notifyChans[0]
	}

	orDone := make(chan interface{})
	go func() {
		defer close(orDone)

		// 利用反射构建SelectCase
		var cases []reflect.SelectCase
		for _, ch := range notifyChans {
			cases = append(cases, reflect.SelectCase{
				Dir:  reflect.SelectRecv,
				Chan: reflect.ValueOf(ch),
			})
		}

		// 随机选择一个就绪的case
		reflect.Select(cases)
	}()

	return orDone
}
