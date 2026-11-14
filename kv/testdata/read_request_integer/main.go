package main

import (
	"context"
	"github.com/bootjp/elastickv/kv"
)

func main() {
	var request *kv.ReadRequest
	ctx := context.Background()
	_, _ = request.Get(ctx, uint64(42), []byte("key"))
	_, _ = request.Scan(ctx, uint64(42), nil, nil, 10)
}
