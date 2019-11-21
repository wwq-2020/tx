package tx

import (
	"context"
	"testing"
)

type fakeRepo struct {
}

func (frp fakeRepo) Begin() (RepoTx, error) {
	return &fakeTx{}, nil
}

func (frp fakeRepo) Finish(interface{}) error {
	return nil
}

type fakeTx struct {
}

func (ftx *fakeTx) Commit() error {
	return nil
}

func (ftx *fakeTx) Store(interface{}) error {
	return nil
}

func step1Handler(context.Context, interface{}) error {
	return nil
}

func step2Handler(context.Context, interface{}) error {
	return nil
}
func TestManager(t *testing.T) {
	rp := &fakeRepo{}
	m := NewManager(rp)
	m.RegisterHandler("step1", step1Handler)
	m.RegisterHandler("step2", step2Handler)
	if err := m.Tx().
		Then("step1", "arg1").
		Then("step2", "arg2").
		Do(); err != nil {
		t.Fatalf("do err, expected:nil,got:%+v", err)
	}
}
