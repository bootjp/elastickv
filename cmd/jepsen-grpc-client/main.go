// jepsen-grpc-client bridges Jepsen's JSON-line operations to the public gRPC APIs.
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strconv"
	"time"

	internalutil "github.com/bootjp/elastickv/internal"
	pb "github.com/bootjp/elastickv/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	apiRaw             = "raw"
	apiTransactional   = "transactional"
	outcomeOK          = "ok"
	outcomeInfo        = "info"
	outcomeFail        = "fail"
	maxOperationBytes  = 64 * 1024
	initialBufferBytes = 4096
	defaultTimeout     = 3 * time.Second
)

type request struct {
	Op    string `json:"op"`
	Key   string `json:"key"`
	Value *int64 `json:"value"`
}

type response struct {
	Type  string `json:"type"`
	Value any    `json:"value"`
	Error string `json:"error,omitempty"`
}

type rpcClient struct {
	raw pb.RawKVClient
	txn pb.TransactionalKVClient
	api string
}

func (c rpcClient) read(ctx context.Context, key []byte) ([]byte, error) {
	if c.api == apiRaw {
		result, err := c.raw.RawGet(ctx, &pb.RawGetRequest{Key: key}, grpc.WaitForReady(true))
		if err != nil {
			return nil, fmt.Errorf("read register: %w", err)
		}
		if !result.GetExists() {
			return nil, nil
		}
		if result.GetValue() == nil {
			return []byte{}, nil
		}
		return result.GetValue(), nil
	}
	result, err := c.txn.Get(ctx, &pb.GetRequest{Key: key}, grpc.WaitForReady(true))
	if err != nil {
		return nil, fmt.Errorf("read register: %w", err)
	}
	return result.GetValue(), nil
}

func (c rpcClient) write(ctx context.Context, key, value []byte) error {
	var success bool
	if c.api == apiRaw {
		result, err := c.raw.RawPut(ctx, &pb.RawPutRequest{Key: key, Value: value})
		if err != nil {
			return fmt.Errorf("write register: %w", err)
		}
		success = result.GetSuccess()
	} else {
		result, err := c.txn.Put(ctx, &pb.PutRequest{Key: key, Value: value})
		if err != nil {
			return fmt.Errorf("write register: %w", err)
		}
		success = result.GetSuccess()
	}
	if !success {
		return errors.New("write response did not confirm success")
	}
	return nil
}

func (c rpcClient) invoke(ctx context.Context, req request) response {
	if req.Key == "" {
		return response{Type: outcomeFail, Error: "key is required"}
	}
	switch req.Op {
	case "write":
		if req.Value == nil {
			return response{Type: outcomeFail, Error: "write value is required"}
		}
		if err := c.write(ctx, []byte(req.Key), []byte(strconv.FormatInt(*req.Value, 10))); err != nil {
			// An RPC error can follow a successful commit. Never retry a write
			// or classify its outcome as a definite abort.
			return response{Type: outcomeInfo, Error: err.Error()}
		}
		return response{Type: outcomeOK, Value: *req.Value}
	case "read":
		value, err := c.read(ctx, []byte(req.Key))
		if err != nil {
			return response{Type: outcomeInfo, Error: err.Error()}
		}
		if value == nil {
			return response{Type: outcomeOK}
		}
		n, err := strconv.ParseInt(string(value), 10, 64)
		if err != nil {
			// Preserve corrupt/unexpected data as an observed value. Turning
			// it into a failed read would hide it from the register checkers.
			return response{Type: outcomeOK, Value: "invalid integer: " + string(value)}
		}
		return response{Type: outcomeOK, Value: n}
	default:
		return response{Type: outcomeFail, Error: "unsupported operation"}
	}
}

func serve(ctx context.Context, client rpcClient, input io.Reader, output io.Writer, timeout time.Duration) error {
	scanner := bufio.NewScanner(input)
	scanner.Buffer(make([]byte, initialBufferBytes), maxOperationBytes)
	encoder := json.NewEncoder(output)
	for scanner.Scan() {
		var req request
		if err := json.Unmarshal(scanner.Bytes(), &req); err != nil {
			return fmt.Errorf("decode operation: %w", err)
		}
		operationCtx, cancel := context.WithTimeout(ctx, timeout)
		result := client.invoke(operationCtx, req)
		cancel()
		if err := encoder.Encode(result); err != nil {
			return fmt.Errorf("encode result: %w", err)
		}
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("read operation stream: %w", err)
	}
	return nil
}

func run() error {
	address := flag.String("address", "", "Public gRPC endpoint")
	api := flag.String("api", apiRaw, "raw or transactional")
	timeout := flag.Duration("timeout", defaultTimeout, "Deadline per operation")
	ca := flag.String("tls-ca-file", "", "CA certificate for verified gRPC TLS")
	flag.Parse()
	if *address == "" || (*api != apiRaw && *api != apiTransactional) || *timeout <= 0 {
		return errors.New("require address, api=raw|transactional, and positive timeout")
	}
	creds := insecure.NewCredentials()
	if *ca != "" {
		verified, err := internalutil.GRPCClientTLSCredentials(*ca)
		if err != nil {
			return fmt.Errorf("load gRPC CA: %w", err)
		}
		creds = verified
	}
	conn, err := grpc.NewClient(*address, grpc.WithTransportCredentials(creds))
	if err != nil {
		return fmt.Errorf("connect gRPC: %w", err)
	}
	defer conn.Close()
	client := rpcClient{raw: pb.NewRawKVClient(conn), txn: pb.NewTransactionalKVClient(conn), api: *api}
	return serve(context.Background(), client, os.Stdin, os.Stdout, *timeout)
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
