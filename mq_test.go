package mq

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"testing"
)

func TestMQOpen(t *testing.T) {
	mq, err := New("testopen")

	assertNoError(t, err)

	t.Cleanup(func() {
		mq.Close()
	})

	if mq == nil {
		t.Errorf("mq should not be nil")
		t.FailNow()
	}
}

func TestMQSendReceive(t *testing.T) {
	mq, err := New("testopen")

	requireNoError(t, err)
	if mq == nil {
		t.Errorf("mq should not be nil")
		t.FailNow()
	}

	t.Cleanup(func() {
		mq.Close()
	})

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

func TestMQConcurrentSendReceive(t *testing.T) {
	mq, err := New("testconcurrent")
	requireNoError(t, err)
	t.Cleanup(func() {
		mq.Close()
	})

	ctx := context.Background()
	msgCount := 1000

	var wg sync.WaitGroup
	wg.Add(2)
	genMsg := func(i int) []byte {
		return fmt.Appendf(nil, "message %d", i)
	}

	go func() {
		defer wg.Done()
		for i := range msgCount {
			msg := genMsg(i)
			requireNoError(t, mq.Send(ctx, msg, 0))
		}
	}()

	go func() {
		defer wg.Done()
		for i := range msgCount {
			received, err := mq.Receive(ctx, 0)
			requireNoError(t, err)
			expected := genMsg(i)
			if !bytes.Equal(expected, received) {
				t.Errorf("expected %s, got %s", string(expected), string(received))
			}
		}
	}()

	wg.Wait()
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
