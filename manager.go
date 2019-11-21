package tx

import (
	"context"
	"sync"
	"time"
)

// Fetcher 获取
type Fetcher func(context.Context) (string, []interface{}, error)

// Handler 处理方法
type Handler func(context.Context, interface{}) error

// Manager 事务管理器
type Manager struct {
	repo          Repo
	topic2Fetcher map[string]Fetcher
	topic2Handler map[string]Handler
	executor      Executor
	sync.Mutex
}

// Repo 存储
type Repo interface {
	Begin() (RepoTx, error)
	Finish(interface{}) error
}

// RepoTx 事务存储
type RepoTx interface {
	Store(interface{}) error
	Commit() error
}

// NewManager 初始化事务管理器
func NewManager(repo Repo) *Manager {
	return &Manager{
		repo:          repo,
		topic2Fetcher: make(map[string]Fetcher),
		topic2Handler: make(map[string]Handler),
		executor:      NewPooledExecutor(100),
	}
}

// RegisterHandler 注册处理方法
func (m *Manager) RegisterHandler(topic string, handler Handler) *Manager {
	m.Lock()
	defer m.Unlock()
	m.topic2Handler[topic] = handler
	return m
}

// Recovery 恢复
func (m *Manager) Recovery(ctx context.Context, fetcher Fetcher) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		topic, events, err := fetcher(ctx)
		if err != nil {
			time.Sleep(time.Second)
			continue
		}
		for _, event := range events {
			m.pubHandle(topic, event)
		}
	}
}

// Tx 事务
func (m *Manager) pubHandle(topic string, event interface{}) {
	handler, exist := m.topic2Handler[topic]
	if !exist {
		m.pubFinish(event)
		return
	}
	task := func(ctx context.Context) {
		if err := handler(ctx, event); err != nil {
			m.pubHandle(topic, event)
			return
		}
		m.pubFinish(event)
	}
	m.executor.Execute(task)
	return
}

func (m *Manager) pubFinish(event interface{}) {
	task := func(ctx context.Context) {
		if err := m.repo.Finish(event); err != nil {
			m.pubFinish(event)
			return
		}
	}
	m.executor.Execute(task)
}

// Tx 事务
func (m *Manager) Tx() *Tx {
	tx := &Tx{m: m, topic2Event: make(map[string]interface{})}
	tx.tx, tx.err = m.repo.Begin()
	return tx
}

// Shutdown 关闭
func (m *Manager) Shutdown() {
	m.executor.Shutdown()
}
