package tx

import (
	"context"
	"sync"
)

// Task 任务处理方法
type Task func(context.Context)

// Executor 执行器接口
type Executor interface {
	// Execute 执行任务
	Execute(Task)
	// Shutdown 关闭执行器
	Shutdown()
}

type pooledExecutor struct {
	size   int
	taskCh chan Task
	wg     sync.WaitGroup
	ctx    context.Context
	cancel func()
}

// NewPooledExecutor 池化的执行器
func NewPooledExecutor(size int) Executor {
	ctx, cancel := context.WithCancel(context.Background())
	pe := &pooledExecutor{
		size:   size,
		ctx:    ctx,
		cancel: cancel,
		taskCh: make(chan Task, size),
	}
	pe.init()
	return pe
}

func (pe *pooledExecutor) init() {
	pe.wg.Add(pe.size)
	for i := pe.size; i > 0; i-- {
		go pe.taskLoop()
	}
}

func (pe *pooledExecutor) taskLoop() {
	defer pe.wg.Done()
	for {
		select {
		case fn := <-pe.taskCh:
			fn(pe.ctx)
		case <-pe.ctx.Done():
			return
		}
	}
}

// Shutdown 关闭执行器
func (pe *pooledExecutor) Shutdown() {
	pe.cancel()
	pe.wg.Wait()
}

// Execute 提交任务
func (pe *pooledExecutor) Execute(task Task) {
	pe.taskCh <- task
}
