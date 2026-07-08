// elastickv-route-key prints the byte-string of a DynamoDB
// table-route key for a given table name.  Used by the Composed-1
// M5 launch script (`scripts/run-jepsen-local.sh`) and OQ-7
// (docs/design/2026_06_02_implemented_composed1_m5_jepsen_route_shuffle.md)
// to compute `--shardRanges` boundary keys without inlining the
// base64-encoding logic in shell — the encoding is part of the
// routing surface and any drift would silently mis-route.
//
// Usage:
//
//	elastickv-route-key jepsen_append_t1
//	# → !ddb|route|table|amVwc2VuX2FwcGVuZF90MQ
//
// The output matches `dynamoRouteTableKey(encodeDynamoSegment(name))`
// (kv/shard_key.go:117 + adapter/dynamodb.go:8441) byte-for-byte.
// A test pin (main_test.go) covers the contract against
// representative names.
//
// The output is written WITHOUT a trailing newline (raw bytes) so
// shell composition (e.g. `--shardRanges "$(elastickv-route-key
// jepsen_append_t2)=2"`) works without `tr -d '\n'`.
package main

import (
	"encoding/base64"
	"fmt"
	"os"
)

// dynamoRoutePrefix mirrors kv.dynamoRoutePrefix (unexported in
// the kv package).  Keep this in sync — the M5 launch script's
// --shardRanges boundary keys must match the routing layer's
// expectations exactly.
const (
	dynamoRoutePrefix = "!ddb|route|table|"
	// usageExitCode is the conventional shell exit code for
	// "command-line usage error" (sysexits.h EX_USAGE = 64; many
	// CLIs simplify to 2).  Distinct from exit 1 used for runtime
	// failures so shell callers can branch.
	usageExitCode = 2
)

func main() {
	if len(os.Args) != 2 || os.Args[1] == "-h" || os.Args[1] == "--help" {
		fmt.Fprintf(os.Stderr, "usage: %s <dynamodb-table-name>\n", os.Args[0])
		os.Exit(usageExitCode)
	}
	fmt.Print(routeKey(os.Args[1]))
}

// routeKey returns the table-route key for tableName.  It mirrors
// `dynamoRouteTableKey(encodeDynamoSegment(tableName))` from the
// adapter (adapter/dynamodb.go:8441 — base64.RawURLEncoding,
// URL-safe charset, no '=' padding).  Extracted from main() for
// testability.
func routeKey(tableName string) string {
	return dynamoRoutePrefix + base64.RawURLEncoding.EncodeToString([]byte(tableName))
}
