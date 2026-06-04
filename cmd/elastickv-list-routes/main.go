// elastickv-list-routes prints the cluster's current route catalog as
// JSON.  Used by the Composed-1 M5a Jepsen setup-hook verification to
// assert the launch script's --shardRanges actually placed the M5
// table-route keys on the expected Raft groups before any workload
// op runs.
//
// Per the design doc (docs/design/2026_06_02_proposed_composed1_m5_jepsen_route_shuffle.md
// §3.3), the Jepsen client's setup! shells out to this tool rather
// than re-implementing the gRPC client in Clojure: a JSON contract is
// stable across versions and a future ListRoutes schema change shows
// up as an unmarshal failure rather than as a silent mis-routing
// during a Jepsen run.
//
// Usage:
//
//	elastickv-list-routes --address 127.0.0.1:50051
//
// Output (stdout, one JSON object):
//
//	{
//	  "catalog_version": 7,
//	  "routes": [
//	    {"route_id": 100, "raft_group_id": 1, "start": "...", "end": "...", "state": "ACTIVE"},
//	    ...
//	  ]
//	}
//
// `start` and `end` are base64-encoded raw bytes so any byte
// sequence (including unprintables in the routing keyspace) survives
// the JSON round-trip without quoting issues.  Non-zero exit on any
// error so the Jepsen setup-hook sees the failure verbatim.
package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	pb "github.com/bootjp/elastickv/proto"
	"github.com/cockroachdb/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const rpcTimeout = 10 * time.Second

var address = flag.String("address", "127.0.0.1:50051", "gRPC address of an elastickv server (any node works — the Distribution service is replicated)")

// routeJSON is the on-the-wire shape this tool emits.  Field names
// are snake_case to match the proto's wire format; Start/End are
// base64-encoded so arbitrary byte ranges survive JSON.
type routeJSON struct {
	RouteID     uint64 `json:"route_id"`
	RaftGroupID uint64 `json:"raft_group_id"`
	Start       string `json:"start"` // base64(raw bytes)
	End         string `json:"end"`   // base64(raw bytes); empty == +infinity
	State       string `json:"state"`
}

type responseJSON struct {
	CatalogVersion uint64      `json:"catalog_version"`
	Routes         []routeJSON `json:"routes"`
}

func main() {
	flag.Parse()
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "elastickv-list-routes: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	conn, err := grpc.NewClient(*address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return errors.Wrapf(err, "dial %s", *address)
	}
	defer func() {
		// Mirror cmd/elastickv-split's close pattern: surface close
		// errors on stderr without failing the process — by this
		// point the response has been printed.
		if cerr := conn.Close(); cerr != nil {
			fmt.Fprintf(os.Stderr, "elastickv-list-routes: close: %v\n", cerr)
		}
	}()

	client := pb.NewDistributionClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
	defer cancel()

	resp, err := client.ListRoutes(ctx, &pb.ListRoutesRequest{})
	if err != nil {
		return errors.Wrap(err, "ListRoutes")
	}
	return emit(resp, os.Stdout)
}

// emit serialises resp as JSON to w.  Extracted from run() so the
// encoding is testable with bytes.Buffer rather than a real
// temp file (gemini medium on PR #925).  io.Writer is the
// idiomatic Go return shape for "I write structured output to
// something."
func emit(resp *pb.ListRoutesResponse, w io.Writer) error {
	out := responseJSON{
		CatalogVersion: resp.GetCatalogVersion(),
		Routes:         make([]routeJSON, 0, len(resp.GetRoutes())),
	}
	for _, r := range resp.GetRoutes() {
		out.Routes = append(out.Routes, routeJSON{
			RouteID:     r.GetRouteId(),
			RaftGroupID: r.GetRaftGroupId(),
			Start:       base64.StdEncoding.EncodeToString(r.GetStart()),
			End:         base64.StdEncoding.EncodeToString(r.GetEnd()),
			State:       r.GetState().String(),
		})
	}
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	if err := enc.Encode(out); err != nil {
		return errors.Wrap(err, "encode")
	}
	return nil
}
