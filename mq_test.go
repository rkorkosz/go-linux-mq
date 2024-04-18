package mq

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMQOpen(t *testing.T) {
	mq, err := New(Config{
		Name:    "testopen",
		MaxMsg:  2,
		MsgSize: 10,
	})
	assert.NoError(t, err)
	defer mq.Close()
	assert.NotNil(t, mq)
}

func TestMQSendReceive(t *testing.T) {
	mq, err := New(Config{
		Name:    "testopen",
		MaxMsg:  2,
		MsgSize: 10,
	})
	require.NoError(t, err)
	require.NotNil(t, mq)
	defer mq.Close()
	ctx := context.Background()
	msg := []byte("test")
	err = mq.Send(ctx, msg, 0)
	require.NoError(t, err)
	received, err := mq.Receive(ctx, 0)
	require.NoError(t, err)
	assert.Equal(t, msg, received)
}
