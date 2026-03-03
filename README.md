[![Go](https://github.com/rkorkosz/go-linux-mq/actions/workflows/go.yml/badge.svg)](https://github.com/rkorkosz/go-linux-mq/actions/workflows/go.yml)

# go-linux-mq
Golang mqueue implementation. Linux only.
`/dev/mqueue` kernel module required.

## Usage

```go
package main

import (
    "context"
    "fmt"
    mq "github.com/rkorkosz/go-linux-mq"
)

func main() {
    q, err := mq.New("/myqueue")
    if err != nil {
        panic(err)
    }
    defer q.Close()

    ctx := context.Background()

    priority := 0

    err = q.Send(ctx, []byte("my new msg"), priority)
    if err != nil {
        panic(err)
    }

    received, priority, err := q.Receive(ctx)
    if err != nil {
        panic(err)
    }
    fmt.Println(string(received))
}

```
