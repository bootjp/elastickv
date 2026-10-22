// elastickv-split is a single-shot CLI that invokes the
// Distribution.SplitRange RPC on a running elastickv cluster.  Per
// the Composed-1 M5 design doc
// (docs/design/2026_06_02_implemented_composed1_m5_jepsen_route_shuffle.md
// §3.1), the Jepsen route-shuffle nemesis shells out to this tool
// rather than re-implementing the gRPC client in Clojure: keeping
// the request construction and the SplitRangeRequest field
// encoding in Go avoids the silent-mis-routing trap flagged on PR #905
// (base64 RawURLEncoding for table-route keys).
//
// Usage:
//
//	elastickv-split \
//	    --address 127.0.0.1:50051 \
//	    --route-id 100 \
//	    --split-key '!ddb|route|table|am...' \
//	    --expected-version 7
//
// Non-zero exit on any error so the Jepsen nemesis sees the
// failure verbatim.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	internalutil "github.com/bootjp/elastickv/internal"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/cockroachdb/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	rpcTimeout             = 10 * time.Second
	maxMigrationTokenBytes = 4 << 10
)

var (
	address         = flag.String("address", "127.0.0.1:50051", "gRPC address of an elastickv server (typically the Distribution leader)")
	routeID         = flag.Uint64("route-id", 0, "RouteID of the route to split (required)")
	splitKey        = flag.String("split-key", "", "Split key — must lie strictly inside the route's [Start, End) range; rejected if == Start or == End by validateSplitKey (required)")
	expectedVersion = flag.Uint64("expected-version", 0, "Expected catalog version for OCC; obtain by calling ListRoutes first (required, must be >= 1 — catalog version is 1-based)")
	targetGroupID   = flag.Uint64("target-group-id", 0, "Target Raft group for an asynchronous cross-group migration; zero uses the existing same-group split")
	abandonJobID    = flag.Uint64("abandon-job-id", 0, "Abandon a pre-cutover migration job; mutually exclusive with split flags")
	retryJobID      = flag.Uint64("retry-job-id", 0, "Resume a FAILED migration at its recorded phase; mutually exclusive with other job/split flags")
	getJobID        = flag.Uint64("get-job-id", 0, "Print one migration job; mutually exclusive with mutation flags")
	tokenFile       = flag.String("token-file", "", "File containing the dedicated migration controller bearer token; not the read-only Admin token")
	tlsCAFile       = flag.String("tls-ca-file", "", "PEM CA certificates for verified gRPC TLS; token-file always requires TLS (system roots if omitted)")
)

func main() {
	flag.Parse()
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "elastickv-split: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	if err := validateFlags(); err != nil {
		return err
	}

	dialOpts, err := splitDialOptions()
	if err != nil {
		return err
	}
	conn, err := grpc.NewClient(*address, dialOpts...)
	if err != nil {
		return errors.Wrapf(err, "dial %s", *address)
	}
	defer func() {
		// Surface gRPC close errors on stderr so a resource-leak
		// or half-closed-stream condition is visible (review feedback
		// on PR #911).  Don't promote to a process-level error
		// since the SplitRange result is already in hand by this
		// point; a noisy close shouldn't mask a successful RPC.
		if cerr := conn.Close(); cerr != nil {
			fmt.Fprintf(os.Stderr, "elastickv-split: close: %v\n", cerr)
		}
	}()

	client := pb.NewDistributionClient(conn)

	rpcCtx, rpcCancel := context.WithTimeout(context.Background(), rpcTimeout)
	defer rpcCancel()
	if *getJobID != 0 {
		return runGetSplitJob(rpcCtx, client)
	}
	if *abandonJobID != 0 {
		return runAbandonSplitJob(rpcCtx, client)
	}
	if *retryJobID != 0 {
		return runRetrySplitJob(rpcCtx, client)
	}

	if *targetGroupID != 0 {
		return runStartSplitMigration(rpcCtx, client)
	}
	req := &pb.SplitRangeRequest{
		ExpectedCatalogVersion: *expectedVersion,
		RouteId:                *routeID,
		SplitKey:               []byte(*splitKey),
	}
	resp, err := client.SplitRange(rpcCtx, req)
	if err != nil {
		return errors.Wrap(err, "SplitRange")
	}
	printResponse(resp)
	return nil
}

func splitDialOptions() ([]grpc.DialOption, error) {
	opts := []grpc.DialOption{internalutil.GRPCCallOptions()}
	if *tokenFile == "" && *tlsCAFile == "" {
		return append(opts, grpc.WithTransportCredentials(insecure.NewCredentials())), nil
	}
	creds, err := internalutil.GRPCClientTLSCredentials(*tlsCAFile)
	if err != nil {
		return nil, errors.Wrap(err, "load split TLS credentials")
	}
	opts = append(opts, grpc.WithTransportCredentials(creds))
	if *tokenFile != "" {
		token, err := internalutil.LoadBearerTokenFile(*tokenFile, maxMigrationTokenBytes, "migration token")
		if err != nil {
			return nil, errors.Wrap(err, "load split migration token")
		}
		opts = append(opts, grpc.WithPerRPCCredentials(internalutil.GRPCBearerToken(token)))
	}
	return opts, nil
}

func runGetSplitJob(ctx context.Context, client pb.DistributionClient) error {
	resp, err := client.GetSplitJob(ctx, &pb.GetSplitJobRequest{JobId: *getJobID})
	if err != nil {
		return errors.Wrap(err, "GetSplitJob")
	}
	job := resp.GetJob()
	fmt.Printf("job_id: %d\nphase: %s\n", job.GetJobId(), job.GetPhase())
	return nil
}

func runAbandonSplitJob(ctx context.Context, client pb.DistributionClient) error {
	if _, err := client.AbandonSplitJob(ctx, &pb.AbandonSplitJobRequest{JobId: *abandonJobID}); err != nil {
		return errors.Wrap(err, "AbandonSplitJob")
	}
	fmt.Printf("abandoned_job_id: %d\n", *abandonJobID)
	return nil
}

func runRetrySplitJob(ctx context.Context, client pb.DistributionClient) error {
	if _, err := client.RetrySplitJob(ctx, &pb.RetrySplitJobRequest{JobId: *retryJobID}); err != nil {
		return errors.Wrap(err, "RetrySplitJob")
	}
	fmt.Printf("retried_job_id: %d\n", *retryJobID)
	return nil
}

func runStartSplitMigration(ctx context.Context, client pb.DistributionClient) error {
	resp, err := client.StartSplitMigration(ctx, &pb.StartSplitMigrationRequest{
		ExpectedCatalogVersion: *expectedVersion,
		RouteId:                *routeID,
		SplitKey:               []byte(*splitKey),
		TargetGroupId:          *targetGroupID,
	})
	if err != nil {
		return errors.Wrap(err, "StartSplitMigration")
	}
	fmt.Printf("catalog_version: %d\njob_id: %d\n", resp.GetCatalogVersion(), resp.GetJobId())
	return nil
}

func validateFlags() error {
	jobMode, err := validateJobModeFlags()
	if err != nil || jobMode {
		return err
	}
	switch {
	case *routeID == 0:
		return errors.New("--route-id is required and must be > 0")
	case *splitKey == "":
		return errors.New("--split-key is required")
	case *expectedVersion == 0:
		return errors.New("--expected-version is required and must be > 0")
	}
	return nil
}

func validateJobModeFlags() (bool, error) {
	modes := 0
	for _, id := range []uint64{*getJobID, *abandonJobID, *retryJobID} {
		if id != 0 {
			modes++
		}
	}
	if modes == 0 {
		return false, nil
	}
	if modes > 1 || splitMutationFlagsSet() {
		return false, errors.New("--get-job-id, --abandon-job-id and --retry-job-id are mutually exclusive with each other and split flags")
	}
	return true, nil
}

func splitMutationFlagsSet() bool {
	return *routeID != 0 || *splitKey != "" || *expectedVersion != 0 || *targetGroupID != 0
}

func printResponse(resp *pb.SplitRangeResponse) {
	fmt.Printf("catalog_version: %d\n", resp.GetCatalogVersion())
	printRoute("left:  ", resp.GetLeft())
	printRoute("right: ", resp.GetRight())
}

func printRoute(label string, r *pb.RouteDescriptor) {
	if r == nil {
		fmt.Printf("%s<nil>\n", label)
		return
	}
	fmt.Printf("%sroute_id=%d group=%d [%q, %q)\n", label, r.GetRouteId(), r.GetRaftGroupId(), r.GetStart(), r.GetEnd())
}
