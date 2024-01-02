package cmd

import (
	"context"
	"fmt"
	"strconv"
	"time"

	_ "github.com/Jille/grpc-multi-resolver"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	_ "google.golang.org/grpc/health"
)

func Run() error {
	serviceConfig := `{"healthCheckConfig": {"serviceName": "RawKV"}, "loadBalancingConfig": [ { "round_robin": {} } ]}`
	conn, err := grpc.Dial("multi:///localhost:50051,localhost:50052,localhost:50053",
		grpc.WithDefaultServiceConfig(serviceConfig),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.WaitForReady(true)),
	)

	if err != nil {
		return errors.WithStack(err)
	}
	defer conn.Close()
	c := pb.NewRawKVClient(conn)

	for i := 0; 100 > i; i++ {
		resp, err := c.RawPut(context.Background(), &pb.RawPutRequest{
			Key:   []byte("key-" + strconv.Itoa(i)),
			Value: []byte(time.Now().String()),
		})
		if err != nil {
			return errors.WithStack(err)
		}
		fmt.Print("Put key-" + strconv.Itoa(i) + " ")
		fmt.Println(resp)
	}

	for i := 0; 100 > i; i++ {
		resp, err := c.RawGet(context.Background(), &pb.RawGetRequest{
			Key: []byte("key-" + strconv.Itoa(i)),
		})
		if err != nil {
			return errors.WithStack(err)
		}
		fmt.Print("Get key-" + strconv.Itoa(i) + " ")
		fmt.Printf("%s\n", resp.Value)
	}

	return nil
}
