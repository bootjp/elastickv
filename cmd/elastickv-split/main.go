// elastickv-split is a single-shot CLI that invokes the
// Distribution.SplitRange RPC on a running elastickv cluster.  Per
// the Composed-1 M5 design doc
// (docs/design/2026_06_02_proposed_composed1_m5_jepsen_route_shuffle.md
// §3.1), the Jepsen route-shuffle nemesis shells out to this tool
// rather than re-implementing the gRPC client in Clojure: keeping
// the request construction and the SplitRangeRequest field
// encoding in Go avoids the silent-mis-routing trap codex P1 #1
// flagged on PR #905 (base64 RawURLEncoding for table-route keys).
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

	pb "github.com/bootjp/elastickv/proto"
	"github.com/cockroachdb/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	rpcTimeout = 10 * time.Second
)

var (
	address         = flag.String("address", "127.0.0.1:50051", "gRPC address of an elastickv server (typically the Distribution leader)")
	routeID         = flag.Uint64("route-id", 0, "RouteID of the route to split (required)")
	splitKey        = flag.String("split-key", "", "Split key — must lie strictly inside the route's [Start, End) range; rejected if == Start or == End by validateSplitKey (required)")
	expectedVersion = flag.Uint64("expected-version", 0, "Expected catalog version for OCC; obtain by calling ListRoutes first (required)")
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

	conn, err := grpc.NewClient(*address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return errors.Wrapf(err, "dial %s", *address)
	}
	defer func() {
		// Surface gRPC close errors on stderr so a resource-leak
		// or half-closed-stream condition is visible (gemini medium
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

func validateFlags() error {
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
