package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	pb "github.com/bootjp/elastickv/proto"
	"github.com/cockroachdb/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	defaultRPCTimeoutSeconds   = 5
	minCommandArgs             = 2
	addVoterArgCount           = 2
	addLearnerArgCount         = 2
	promoteLearnerArgCount     = 1
	transferArgCount           = 2
	addVoterPrevIndex          = 2
	addLearnerPrevIndex        = 2
	promoteLearnerPrevIndex    = 1
	promoteLearnerMinApplied   = 2
	promoteLearnerSkipCheckArg = 3
	removePrevIndex            = 1
	timeoutEnv                 = "RAFTADMIN_RPC_TIMEOUT_SECONDS"
	allowInsecureEnv           = "RAFTADMIN_ALLOW_INSECURE"
	tlsEnv                     = "RAFTADMIN_TLS"
	tlsCACertEnv               = "RAFTADMIN_TLS_CA_CERT"
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
	creds, err := transportCredentialsFor(addr)
	if err != nil {
		return nil, func() {}, err
	}
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(creds))
	if err != nil {
		return nil, func() {}, errors.Wrap(err, "dial raft admin")
	}
	return pb.NewRaftAdminClient(conn), func() { _ = conn.Close() }, nil
}

func transportCredentialsFor(addr string) (credentials.TransportCredentials, error) {
	useTLS, err := boolEnv(tlsEnv)
	if err != nil {
		return nil, err
	}
	if useTLS {
		tlsConfig, err := tlsConfigFromEnv()
		if err != nil {
			return nil, err
		}
		return credentials.NewTLS(tlsConfig), nil
	}

	allowInsecure, err := allowInsecurePlaintext(addr)
	if err != nil {
		return nil, err
	}
	if !allowInsecure {
		return nil, errors.New("plaintext raftadmin to non-loopback address requires RAFTADMIN_ALLOW_INSECURE=true or RAFTADMIN_TLS=true")
	}
	return insecure.NewCredentials(), nil
}

func tlsConfigFromEnv() (*tls.Config, error) {
	cfg := &tls.Config{MinVersion: tls.VersionTLS12}

	caPath := strings.TrimSpace(os.Getenv(tlsCACertEnv))
	if caPath == "" {
		return cfg, nil
	}

	pemBytes, err := os.ReadFile(caPath)
	if err != nil {
		return nil, errors.Wrapf(err, "read %s", tlsCACertEnv)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pemBytes) {
		return nil, errors.Errorf("parse %s: %q", tlsCACertEnv, caPath)
	}
	cfg.RootCAs = pool
	return cfg, nil
}

func allowInsecurePlaintext(addr string) (bool, error) {
	allowInsecure, err := boolEnv(allowInsecureEnv)
	if err != nil {
		return false, err
	}
	if allowInsecure {
		return true, nil
	}
	return isLocalAdminAddr(addr), nil
}

func boolEnv(name string) (bool, error) {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return false, nil
	}
	switch strings.ToLower(raw) {
	case "1", "true", "yes", "on":
		return true, nil
	case "0", "false", "no", "off":
		return false, nil
	default:
		return false, errors.Errorf("invalid boolean value for %s: %q", name, raw)
	}
}

func isLocalAdminAddr(addr string) bool {
	if strings.HasPrefix(addr, "unix:") {
		return true
	}
	if strings.Contains(addr, "://") {
		u, err := url.Parse(addr)
		if err == nil {
			switch u.Scheme {
			case "unix", "passthrough":
				return true
			}
			if u.Host != "" {
				return isLoopbackHost(hostOnly(u.Host))
			}
		}
	}
	return isLoopbackHost(hostOnly(addr))
}

func hostOnly(addr string) string {
	host, _, err := net.SplitHostPort(addr)
	if err == nil {
		return strings.Trim(host, "[]")
	}
	return strings.Trim(addr, "[]")
}

func isLoopbackHost(host string) bool {
	if host == "" || strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func readOnlyCommands() map[string]func(context.Context, pb.RaftAdminClient) error {
	return map[string]func(context.Context, pb.RaftAdminClient) error{
		"leader":        printLeader,
		"state":         printState,
		"configuration": printConfiguration,
		"config":        printConfiguration,
	}
}

func executeCommand(ctx context.Context, client pb.RaftAdminClient, command string, commandArgs []string) error {
	if handler, ok := readOnlyCommands()[command]; ok {
		return handler(ctx, client)
	}
	switch command {
	case "add_voter":
		return addVoter(ctx, client, commandArgs)
	case "add_learner":
		return addLearner(ctx, client, commandArgs)
	case "promote_learner":
		return promoteLearner(ctx, client, commandArgs)
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

func addLearner(ctx context.Context, client pb.RaftAdminClient, args []string) error {
	if len(args) < addLearnerArgCount || len(args) > addLearnerArgCount+1 {
		return usageError("add_learner")
	}
	prevIndex, err := parseOptionalUint64(args, addLearnerPrevIndex)
	if err != nil {
		return err
	}
	resp, err := client.AddLearner(ctx, &pb.RaftAdminAddLearnerRequest{
		Id:            args[0],
		Address:       args[1],
		PreviousIndex: prevIndex,
	})
	if err != nil {
		return errors.Wrap(err, "add learner")
	}
	fmt.Printf("index: %d\n", resp.Index)
	return nil
}

func promoteLearner(ctx context.Context, client pb.RaftAdminClient, args []string) error {
	if len(args) < promoteLearnerArgCount || len(args) > promoteLearnerArgCount+3 {
		return usageError("promote_learner")
	}
	prevIndex, err := parseOptionalUint64(args, promoteLearnerPrevIndex)
	if err != nil {
		return err
	}
	minApplied, err := parseOptionalUint64(args, promoteLearnerMinApplied)
	if err != nil {
		return err
	}
	skipMinApplied, err := parseOptionalBool(args, promoteLearnerSkipCheckArg)
	if err != nil {
		return err
	}
	resp, err := client.PromoteLearner(ctx, &pb.RaftAdminPromoteLearnerRequest{
		Id:                  args[0],
		PreviousIndex:       prevIndex,
		MinAppliedIndex:     minApplied,
		SkipMinAppliedCheck: skipMinApplied,
	})
	if err != nil {
		return errors.Wrap(err, "promote learner")
	}
	fmt.Printf("index: %d\n", resp.Index)
	return nil
}

func parseOptionalBool(args []string, index int) (bool, error) {
	if len(args) <= index {
		return false, nil
	}
	switch args[index] {
	case "true", "1", "yes":
		return true, nil
	case "false", "0", "no", "":
		return false, nil
	default:
		return false, errors.Errorf("invalid bool argument %q (want true/false)", args[index])
	}
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

var usageStrings = map[string]string{
	"leader":                        "raftadmin <addr> leader",
	"state":                         "raftadmin <addr> state",
	"configuration":                 "raftadmin <addr> configuration",
	"config":                        "raftadmin <addr> configuration",
	"add_voter":                     "raftadmin <addr> add_voter <id> <address> [previous_index]",
	"add_learner":                   "raftadmin <addr> add_learner <id> <address> [previous_index]",
	"promote_learner":               "raftadmin <addr> promote_learner <id> [previous_index] [min_applied_index] [skip_min_applied_check]",
	"remove_server":                 "raftadmin <addr> remove_server <id> [previous_index]",
	"leadership_transfer":           "raftadmin <addr> leadership_transfer",
	"leadership_transfer_to_server": "raftadmin <addr> leadership_transfer_to_server <id> <address>",
}

const usageFallback = "raftadmin <addr> <leader|state|configuration|add_voter|add_learner|promote_learner|remove_server|leadership_transfer|leadership_transfer_to_server> [args]"

func usage(command string) string {
	if u, ok := usageStrings[command]; ok {
		return u
	}
	return usageFallback
}
