package main

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	pb "github.com/bootjp/elastickv/proto"
	"github.com/cockroachdb/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	defaultRPCTimeoutSeconds = 5
	minCommandArgs           = 2
	addVoterArgCount         = 2
	transferArgCount         = 2
	addVoterPrevIndex        = 2
	removePrevIndex          = 1
	timeoutEnv               = "RAFTADMIN_RPC_TIMEOUT_SECONDS"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) < minCommandArgs {
		return usageError("")
	}

	addr := args[0]
	command := args[1]
	commandArgs := args[2:]

	client, closeConn, err := dialClient(addr)
	if err != nil {
		return err
	}
	defer closeConn()

	timeout, err := rpcTimeout()
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return executeCommand(ctx, client, command, commandArgs)
}

func dialClient(addr string) (pb.RaftAdminClient, func(), error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, func() {}, errors.Wrap(err, "dial raft admin")
	}
	return pb.NewRaftAdminClient(conn), func() { _ = conn.Close() }, nil
}

func executeCommand(ctx context.Context, client pb.RaftAdminClient, command string, commandArgs []string) error {
	switch command {
	case "leader":
		return printLeader(ctx, client)
	case "state":
		return printState(ctx, client)
	case "configuration", "config":
		return printConfiguration(ctx, client)
	case "add_voter":
		return addVoter(ctx, client, commandArgs)
	case "remove_server":
		return removeServer(ctx, client, commandArgs)
	case "leadership_transfer":
		return transferLeadership(ctx, client, nil)
	case "leadership_transfer_to_server":
		req, err := parseTransferTarget(commandArgs)
		if err != nil {
			return err
		}
		return transferLeadership(ctx, client, req)
	default:
		return usageError(command)
	}
}

func printLeader(ctx context.Context, client pb.RaftAdminClient) error {
	resp, err := client.Status(ctx, &pb.RaftAdminStatusRequest{})
	if err != nil {
		return errors.Wrap(err, "get leader")
	}
	fmt.Printf("id: %q\n", resp.LeaderId)
	fmt.Printf("address: %q\n", resp.LeaderAddress)
	return nil
}

func printState(ctx context.Context, client pb.RaftAdminClient) error {
	resp, err := client.Status(ctx, &pb.RaftAdminStatusRequest{})
	if err != nil {
		return errors.Wrap(err, "get state")
	}
	fmt.Printf("state: %s\n", formatState(resp.State))
	return nil
}

func printConfiguration(ctx context.Context, client pb.RaftAdminClient) error {
	resp, err := client.Configuration(ctx, &pb.RaftAdminConfigurationRequest{})
	if err != nil {
		return errors.Wrap(err, "get configuration")
	}
	for _, server := range resp.Servers {
		fmt.Println("servers {")
		fmt.Printf("  id: %q\n", server.Id)
		fmt.Printf("  address: %q\n", server.Address)
		fmt.Printf("  suffrage: %q\n", server.Suffrage)
		fmt.Println("}")
	}
	return nil
}

func addVoter(ctx context.Context, client pb.RaftAdminClient, args []string) error {
	if len(args) < addVoterArgCount || len(args) > addVoterArgCount+1 {
		return usageError("add_voter")
	}
	prevIndex, err := parseOptionalUint64(args, addVoterPrevIndex)
	if err != nil {
		return err
	}
	resp, err := client.AddVoter(ctx, &pb.RaftAdminAddVoterRequest{
		Id:            args[0],
		Address:       args[1],
		PreviousIndex: prevIndex,
	})
	if err != nil {
		return errors.Wrap(err, "add voter")
	}
	fmt.Printf("index: %d\n", resp.Index)
	return nil
}

func removeServer(ctx context.Context, client pb.RaftAdminClient, args []string) error {
	if len(args) < 1 || len(args) > removePrevIndex+1 {
		return usageError("remove_server")
	}
	prevIndex, err := parseOptionalUint64(args, removePrevIndex)
	if err != nil {
		return err
	}
	resp, err := client.RemoveServer(ctx, &pb.RaftAdminRemoveServerRequest{
		Id:            args[0],
		PreviousIndex: prevIndex,
	})
	if err != nil {
		return errors.Wrap(err, "remove server")
	}
	fmt.Printf("index: %d\n", resp.Index)
	return nil
}

func transferLeadership(ctx context.Context, client pb.RaftAdminClient, req *pb.RaftAdminTransferLeadershipRequest) error {
	if req == nil {
		req = &pb.RaftAdminTransferLeadershipRequest{}
	}
	if _, err := client.TransferLeadership(ctx, req); err != nil {
		return errors.Wrap(err, "transfer leadership")
	}
	fmt.Println("ok: true")
	return nil
}

func parseTransferTarget(args []string) (*pb.RaftAdminTransferLeadershipRequest, error) {
	if len(args) != transferArgCount {
		return nil, usageError("leadership_transfer_to_server")
	}
	return &pb.RaftAdminTransferLeadershipRequest{
		TargetId:      args[0],
		TargetAddress: args[1],
	}, nil
}

func parseOptionalUint64(args []string, index int) (uint64, error) {
	if len(args) <= index {
		return 0, nil
	}
	value, err := strconv.ParseUint(args[index], 10, 64)
	if err != nil {
		return 0, errors.Wrap(err, "parse uint64")
	}
	return value, nil
}

func rpcTimeout() (time.Duration, error) {
	raw := strings.TrimSpace(os.Getenv(timeoutEnv))
	if raw == "" {
		return defaultRPCTimeoutSeconds * time.Second, nil
	}
	seconds, err := strconv.Atoi(raw)
	if err != nil {
		return 0, errors.Wrap(err, "parse raft admin timeout seconds")
	}
	if seconds <= 0 {
		return 0, errors.New("raft admin timeout seconds must be positive")
	}
	return time.Duration(seconds) * time.Second, nil
}

func formatState(state pb.RaftAdminState) string {
	raw := strings.TrimPrefix(state.String(), "RAFT_ADMIN_STATE_")
	if raw == "" {
		return "UNKNOWN"
	}
	return raw
}

func usageError(command string) error {
	return fmt.Errorf("usage: %s", usage(command))
}

func usage(command string) string {
	switch command {
	case "leader":
		return "raftadmin <addr> leader"
	case "state":
		return "raftadmin <addr> state"
	case "configuration", "config":
		return "raftadmin <addr> configuration"
	case "add_voter":
		return "raftadmin <addr> add_voter <id> <address> [previous_index]"
	case "remove_server":
		return "raftadmin <addr> remove_server <id> [previous_index]"
	case "leadership_transfer":
		return "raftadmin <addr> leadership_transfer"
	case "leadership_transfer_to_server":
		return "raftadmin <addr> leadership_transfer_to_server <id> <address>"
	default:
		return "raftadmin <addr> <leader|state|configuration|add_voter|remove_server|leadership_transfer|leadership_transfer_to_server> [args]"
	}
}
