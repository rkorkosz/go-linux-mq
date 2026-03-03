package mq

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
	"unsafe"

	"golang.org/x/sys/unix"
)

var ErrInvalidQueueName = errors.New("invalid queue name")

type MQ struct {
	Name    string
	MsgSize int64
	MaxMsg  int64
	BufPool *sync.Pool
	Retries int
	ptr     uintptr
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
	if !isValidQueueName(name) {
		return nil, ErrInvalidQueueName
	}
	pname, err := unix.BytePtrFromString(name)
	if err != nil {
		return nil, err
	}
	mq := &MQ{
		Name:    name,
		Retries: 2,
		MaxMsg:  10,
		MsgSize: 8192,
	}
	for _, opt := range opts {
		opt(mq)
	}

	if mq.BufPool == nil {
		mq.BufPool = &sync.Pool{
			New: func() any {
				return make([]byte, mq.MsgSize)
			},
		}
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

func WithRetries(retries int) func(*MQ) {
	return func(mq *MQ) {
		mq.Retries = retries
	}
}

// Close closes connection to the queue
func (mq *MQ) Close() error {
	return unix.Close(int(mq.ptr))
}

func (mq *MQ) CloseAndUnlink() error {
	err := mq.Close()
	if err != nil {
		return err
	}
	return os.Remove(filepath.Join("/dev/mqueue", mq.Name[1:]))
}

// Send sends a message to the queue with the given priority.
// If the context is cancelled, the operation is aborted.
// It returns an error if the message could not be sent
func (mq *MQ) Send(ctx context.Context, data []byte, priority int) error {
	if len(data) == 0 {
		return nil
	}
	timeout, ok := ctx.Deadline()
	if !ok {
		// sending immediately
		timeout = time.Now().Add(-1)
	}

	t, err := unix.TimeToTimespec(timeout)
	if err != nil {
		return err
	}
	var retries int

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
		retries++
		if errno != 0 && retries == mq.Retries {
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

// Receive receives a message from the queue.
// If the context is cancelled, the operation is aborted.
// It returns message body and error if the message could not be sent
func (mq *MQ) Receive(ctx context.Context) ([]byte, int, error) {
	var tm uintptr

	timeout, ok := ctx.Deadline()
	if ok {
		t, err := unix.TimeToTimespec(timeout)
		if err != nil {
			return nil, -1, err
		}
		tm = uintptr(unsafe.Pointer(&t))
	}

	msgBuf := mq.BufPool.Get().([]byte)
	defer mq.BufPool.Put(msgBuf)
	var prio uint32

	for {
		n, _, errno := unix.Syscall6(
			unix.SYS_MQ_TIMEDRECEIVE,
			mq.ptr,
			uintptr(unsafe.Pointer(&msgBuf[0])),
			uintptr(mq.MsgSize),
			uintptr(unsafe.Pointer(&prio)),
			tm,
			0,
		)
		if errno == 0 {
			out := make([]byte, n)
			copy(out, msgBuf[:n])
			return out, int(prio), nil
		}
		if errno != 0 {
			return nil, -1, errno
		}
		select {
		case <-ctx.Done():
			return nil, -1, ctx.Err()
		default:
			// continue retrying
		}
	}
}

func isValidQueueName(name string) bool {
	if len(name) < 2 {
		return false
	}
	if !strings.HasPrefix(name, "/") {
		return false
	}
	if strings.Count(name, "/") > 1 {
		return false
	}
	return true
}
