package main

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"time"

	_ "github.com/Jille/grpc-multi-resolver"
	pb "github.com/bootjp/elastickv/proto"
	retry "github.com/grpc-ecosystem/go-grpc-middleware/retry"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	_ "google.golang.org/grpc/health"
)

const retryCount = 3

var backoffDuration = 100 * time.Millisecond

func main() {
	serviceConfig := `{"healthCheckConfig": {"serviceName": "RawKV"}, "loadBalancingConfig": [ { "round_robin": {} } ]}`
	retryOpts := []retry.CallOption{
		retry.WithBackoff(retry.BackoffExponential(backoffDuration)),
		retry.WithMax(retryCount),
	}
	conn, err := grpc.Dial("multi:///localhost:50051,localhost:50052,localhost:50053",
		grpc.WithDefaultServiceConfig(serviceConfig),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.WaitForReady(true)),
		grpc.WithUnaryInterceptor(retry.UnaryClientInterceptor(retryOpts...)),
	)
	if err != nil {
		log.Fatalf("dialing failed: %v", err)
	}
	defer conn.Close()
	c := pb.NewRawKVClient(conn)

	for i := 0; 10 > i; i++ {
		resp, err := c.Put(context.Background(), &pb.PutRequest{
			Key:   []byte("key-" + strconv.Itoa(i)),
			Value: []byte(time.Now().String()),
		})
		if err != nil {
			log.Fatalf("Put RPC failed: %v", err)
		}
		fmt.Print("Put key-" + strconv.Itoa(i) + " ")
		fmt.Println(resp)
	}

	for i := 0; 10 > i; i++ {
		resp, err := c.Get(context.Background(), &pb.GetRequest{
			Key: []byte("key-" + strconv.Itoa(i)),
		})
		if err != nil {
			log.Fatalf("Get RPC failed: %v", err)
		}
		fmt.Print("Get key-" + strconv.Itoa(i) + " ")
		fmt.Printf("%s\n", resp.Value)
	}

}
