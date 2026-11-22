// loadtest runs paced RawKV stages and audits every acknowledged unique write.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	internalutil "github.com/bootjp/elastickv/internal"
	"github.com/bootjp/elastickv/internal/loadtest"
	pb "github.com/bootjp/elastickv/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	err := run(ctx, os.Args[1:], os.Stdout)
	cancel()
	if err != nil && !errors.Is(err, flag.ErrHelp) {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string, out io.Writer) error {
	options, err := parseOptions(args)
	if err != nil {
		return err
	}
	clients, closeClients, err := connectClients(options)
	if err != nil {
		return err
	}
	defer closeClients()
	runner, err := loadtest.New(options.Config, clients...)
	if err != nil {
		return err
	}
	encoder := json.NewEncoder(out)
	if err = encoder.Encode(map[string]any{"type": "configuration", "config": options.Config, "addresses": options.Addresses,
		"profile": "separate process; server profiles must be enabled explicitly"}); err != nil {
		return err
	}
	if err = checkAuditMarker(options); err != nil {
		return err
	}
	return runLoad(ctx, runner, encoder, options, len(clients))
}

func connectClients(o options) ([]pb.RawKVClient, func(), error) {
	dial, err := dialOptions(o.Plain, o.CA, o.TokenFile)
	if err != nil {
		return nil, nil, err
	}
	var clients []pb.RawKVClient
	var connections []*grpc.ClientConn
	closeClients := func() {
		for _, conn := range connections {
			_ = conn.Close()
		}
	}
	for _, address := range strings.Split(o.Addresses, ",") {
		address = strings.TrimSpace(address)
		if address == "" {
			closeClients()
			return nil, nil, fmt.Errorf("empty ingress address")
		}
		conn, err := grpc.NewClient(address, dial...)
		if err != nil {
			closeClients()
			return nil, nil, err
		}
		connections = append(connections, conn)
		clients = append(clients, pb.NewRawKVClient(conn))
	}
	return clients, closeClients, nil
}

func runLoad(ctx context.Context, runner *loadtest.Runner, encoder *json.Encoder, options options, ingresses int) error {
	config := options.Config
	if err := runner.Seed(ctx); err != nil {
		return err
	}
	if err := encoder.Encode(map[string]any{"type": "seeded", "keys": config.Keys, "time": time.Now().UTC()}); err != nil {
		return err
	}
	var outputErr error
	runner.OnStageStart = func(index, rate int) {
		if outputErr == nil {
			outputErr = encoder.Encode(map[string]any{"type": "stage_start", "index": index, "rate": rate, "time": time.Now().UTC()})
		}
	}
	stages, err := runner.Run(ctx, func(stage loadtest.Stage) {
		if outputErr == nil {
			outputErr = encoder.Encode(map[string]any{"type": "stage", "stage": stage})
		}
	})
	if err != nil {
		return err
	}
	if outputErr != nil {
		return outputErr
	}
	if err = waitForAudit(ctx, options, encoder); err != nil {
		return err
	}
	return auditAndReport(ctx, runner, encoder, stages, options.Config.ReadPercent, ingresses)
}

func auditAndReport(ctx context.Context, runner *loadtest.Runner, encoder *json.Encoder, stages []loadtest.Stage, readPercent, ingresses int) error {
	acknowledged := 0
	for _, stage := range stages {
		acknowledged += stage.Write.Success
	}
	if err := runner.ValidateLedger(acknowledged); err != nil {
		return err
	}
	audit := runner.Audit(ctx)
	expected := acknowledged * ingresses
	if err := encoder.Encode(map[string]any{"type": "audit", "acknowledged_writes": runner.Acknowledged(),
		"expected_observations": expected, "audit": audit, "complete": audit.Attempts == expected,
		"verified": expected > 0 && audit.Attempts == expected && audit.Success == expected}); err != nil {
		return err
	}
	return validateResult(stages, audit, expected, readPercent)
}

func validateResult(stages []loadtest.Stage, audit loadtest.Metric, expected, readPercent int) error {
	if (expected == 0 && readPercent < 100) || audit.Attempts != expected || audit.Success != expected {
		return fmt.Errorf("acknowledged-write audit failed: %d/%d observations", audit.Success, expected)
	}
	for _, stage := range stages {
		if stage.Cancelled || stage.DroppedCapacity+stage.DroppedLate > 0 || stage.Logical.Success != stage.Logical.Attempts {
			return fmt.Errorf("load degraded: inspect stage errors/drops; audit is independent of availability")
		}
	}
	return nil
}

func parseRates(text string) ([]int, error) {
	var rates []int
	for _, part := range strings.Split(text, ",") {
		value, err := strconv.Atoi(strings.TrimSpace(part))
		if err != nil || value <= 0 {
			return nil, fmt.Errorf("invalid rate %q", part)
		}
		rates = append(rates, value)
	}
	return rates, nil
}

func dialOptions(plain bool, ca, tokenFile string) ([]grpc.DialOption, error) {
	options := []grpc.DialOption{grpc.WithDisableRetry()}
	if plain {
		if ca != "" || tokenFile != "" {
			return nil, fmt.Errorf("TLS CA/token cannot be used with plaintext")
		}
		return append(options, grpc.WithTransportCredentials(insecure.NewCredentials())), nil
	}
	creds, err := internalutil.GRPCClientTLSCredentials(ca)
	if err != nil {
		return nil, err
	}
	options = append(options, grpc.WithTransportCredentials(creds))
	if tokenFile != "" {
		token, err := os.ReadFile(tokenFile)
		if err != nil {
			return nil, err
		}
		value := strings.TrimSpace(string(token))
		if value == "" {
			return nil, fmt.Errorf("empty token file")
		}
		options = append(options, grpc.WithPerRPCCredentials(internalutil.GRPCBearerToken(value)))
	}
	return options, nil
}
