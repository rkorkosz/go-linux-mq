package mq

import (
	"context"
	"sync"
	"time"
	"unsafe"

	"golang.org/x/sys/unix"
)

type MQ struct {
	Name    string
	ptr     uintptr
	MsgSize int64
	MaxMsg  int64
	BufPool *sync.Pool
}

type mqOpenAttrs struct {
	_       int64
	MaxMsg  int64
	MsgSize int64
	_       int64
}

// New creates a new message queue with provided options.
// It returns an error if the queue could not be created.
func New(name string, opts ...func(*MQ)) (*MQ, error) {
	pname, err := unix.BytePtrFromString(name)
	if err != nil {
		return nil, err
	}
	mq := &MQ{
		MaxMsg:  10,
		MsgSize: 8192,
		BufPool: &sync.Pool{
			New: func() any {
				return make([]byte, 8192)
			},
		},
	}
	for _, opt := range opts {
		opt(mq)
	}
	smq, _, errno := unix.Syscall6(
		unix.SYS_MQ_OPEN,
		uintptr(unsafe.Pointer(pname)),
		unix.O_RDWR|unix.O_CREAT,
		0o600,
		uintptr(unsafe.Pointer(&mqOpenAttrs{
			MaxMsg:  mq.MaxMsg,
			MsgSize: mq.MsgSize,
		})),
		0,
		0,
	)
	if errno != 0 {
		return nil, errno
	}
	mq.ptr = smq

	return mq, nil
}

func WithMaxMessage(maxMessage int64) func(*MQ) {
	return func(mq *MQ) {
		mq.MaxMsg = maxMessage
	}
}

func WithMessageSize(messageSize int64) func(*MQ) {
	return func(mq *MQ) {
		mq.MsgSize = messageSize
	}
}

func WithBufferPool(pool *sync.Pool) func(*MQ) {
	return func(mq *MQ) {
		mq.BufPool = pool
	}
}

// Close closes connection to the queue
func (mq *MQ) Close() error {
	return unix.Close(int(mq.ptr))
}

// Send sends a message to the queue with the given priority.
// If the context is cancelled, the operation is aborted.
// It returns an error if the message could not be sent
func (mq *MQ) Send(ctx context.Context, data []byte, priority int) error {
	timeout, ok := ctx.Deadline()
	if !ok {
		// sending immediately
		timeout = time.Now().Add(-1)
	}

	t, err := unix.TimeToTimespec(timeout)
	if err != nil {
		return err
	}

	for {
		_, _, errno := unix.Syscall6(
			unix.SYS_MQ_TIMEDSEND,
			mq.ptr,
			uintptr(unsafe.Pointer(&data[0])),
			uintptr(len(data)),
			uintptr(priority),
			uintptr(unsafe.Pointer(&t)),
			0,
		)
		if errno == 0 {
			return nil
		}
		if errno != 0 {
			return errno
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			// continue retrying
		}
	}
}

// Receive receives a message from the queue with the given priority.
// If the context is cancelled, the operation is aborted.
// It returns message body and error if the message could not be sent
func (mq *MQ) Receive(ctx context.Context, priority int) ([]byte, error) {
	var tm uintptr

	timeout, ok := ctx.Deadline()
	if ok {
		t, err := unix.TimeToTimespec(timeout)
		if err != nil {
			return nil, err
		}
		tm = uintptr(unsafe.Pointer(&t))
	}

	msgBuf := mq.BufPool.Get().([]byte)
	defer mq.BufPool.Put(msgBuf)

	for {
		n, _, errno := unix.Syscall6(
			unix.SYS_MQ_TIMEDRECEIVE,
			mq.ptr,
			uintptr(unsafe.Pointer(&msgBuf[0])),
			uintptr(mq.MsgSize),
			uintptr(priority),
			tm,
			0,
		)
		if errno == 0 {
			return msgBuf[:n], nil
		}
		if errno != 0 {
			return nil, errno
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
			// continue retrying
		}
	}
}
