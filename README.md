[![Go](https://github.com/rkorkosz/go-linux-mq/actions/workflows/go.yml/badge.svg)](https://github.com/rkorkosz/go-linux-mq/actions/workflows/go.yml)

# go-linux-mq
Golang mqueue implementation

## Usage

```go
package main

import (
    "context"
    "github.com/rkorkosz/go-linux-mq"
)

func main() {
    q, err := mq.New(mq.Config{
        Name: "myqueue",
        MaxMsg: 10,
        MsgSize: 1 << 20, // 1 MB
    })
    if err != nil {
        panic(err)
    }
    defer q.Close()

    ctx := context.Background()

    priority := 0

    err = mq.Send(ctx, []byte("my new msg"), priority)
    if err != nil {
        panic(err)
    }

    received, err := mq.Receive(ctx, priority)
    if err != nil {
        panic(err)
    }
    fmt.Println(string(received))
}

```
