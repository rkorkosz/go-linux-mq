package mq

import (
	"bytes"
	"context"
	"testing"
)

func TestMQOpen(t *testing.T) {
	mq, err := New(Config{
		Name:    "testopen",
		MaxMsg:  2,
		MsgSize: 10,
	})

	assertNoError(t, err)

	defer mq.Close()

	if mq == nil {
		t.Errorf("mq should not be nil")
		t.FailNow()
	}
}

func TestMQSendReceive(t *testing.T) {
	mq, err := New(Config{
		Name:    "testopen",
		MaxMsg:  2,
		MsgSize: 10,
	})

	requireNoError(t, err)
	if mq == nil {
		t.Errorf("mq should not be nil")
		t.FailNow()
	}

	defer mq.Close()

	ctx := context.Background()

	msg := []byte("test")

	err = mq.Send(ctx, msg, 0)
	requireNoError(t, err)

	received, err := mq.Receive(ctx, 0)
	requireNoError(t, err)

	if !bytes.Equal(msg, received) {
		t.Errorf("want %s, got %s", string(msg), string(received))
	}
}

func requireNoError(t *testing.T, err error) {
	t.Helper()
	if !assertNoError(t, err) {
		t.FailNow()
	}
}

func assertNoError(t *testing.T, err error) bool {
	t.Helper()
	if err != nil {
		t.Errorf("Error is not nil: %s", err)
		return false
	}
	return true
}
