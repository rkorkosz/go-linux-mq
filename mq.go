package mq

import (
	"context"
	"time"
	"unsafe"

	"golang.org/x/sys/unix"
)

type Config struct {
	Name    string
	MaxMsg  int64
	MsgSize int64
}

type MQ struct {
	ptr     uintptr
	MsgSize int64
}

type mqOpenAttrs struct {
	_       int64
	MaxMsg  int64
	MsgSize int64
	_       int64
}

func New(cfg Config) (*MQ, error) {
	name, err := unix.BytePtrFromString(cfg.Name)
	if err != nil {
		return nil, err
	}
	mq, _, errno := unix.Syscall6(
		unix.SYS_MQ_OPEN,
		uintptr(unsafe.Pointer(name)),
		unix.O_RDWR|unix.O_CREAT,
		0o600,
		uintptr(unsafe.Pointer(&mqOpenAttrs{
			MaxMsg:  cfg.MaxMsg,
			MsgSize: cfg.MsgSize,
		})),
		0,
		0,
	)
	if errno != 0 {
		return nil, errno
	}
	return &MQ{ptr: mq, MsgSize: cfg.MsgSize}, nil
}

func (mq *MQ) Close() error {
	return unix.Close(int(mq.ptr))
}

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
	_, _, errno := unix.Syscall6(
		unix.SYS_MQ_TIMEDSEND,
		mq.ptr,
		uintptr(unsafe.Pointer(&data[0])),
		uintptr(len(data)),
		uintptr(priority),
		uintptr(unsafe.Pointer(&t)),
		0,
	)
	if errno != 0 {
		return errno
	}
	return nil
}

func (mq *MQ) Receive(ctx context.Context, priority int) ([]byte, error) {
	timeout, ok := ctx.Deadline()
	if !ok {
		timeout = time.Now().Add(-1)
	}
	t, err := unix.TimeToTimespec(timeout)
	if err != nil {
		return nil, err
	}
	msgBuf := make([]byte, mq.MsgSize)
	n, _, errno := unix.Syscall6(
		unix.SYS_MQ_TIMEDRECEIVE,
		mq.ptr,
		uintptr(unsafe.Pointer(&msgBuf[0])),
		uintptr(mq.MsgSize),
		uintptr(priority),
		uintptr(unsafe.Pointer(&t)),
		0,
	)
	if errno != 0 {
		return nil, errno
	}
	return msgBuf[:n], nil
}
