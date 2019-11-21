package tx

// Tx 事务
type Tx struct {
	m           *Manager
	topic2Event map[string]interface{}
	tx          RepoTx
	err         error
}

// Then 下一步
func (tx *Tx) Then(topic string, event interface{}) *Tx {
	if tx.err != nil {
		return tx
	}
	if err := tx.tx.Store(event); err != nil {
		tx.err = err
		return tx
	}
	tx.topic2Event[topic] = event
	return tx
}

// Do 执行
func (tx *Tx) Do() error {
	if tx.err != nil {
		return tx.err
	}
	if err := tx.tx.Commit(); err != nil {
		return err
	}

	for topic, arg := range tx.topic2Event {
		tx.m.pubHandle(topic, arg)
	}
	return nil
}
