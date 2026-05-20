package admin

import (
	"context"
	"errors"
	"sync"
	"time"

	pb "github.com/bootjp/elastickv/proto"
	pkgerrors "github.com/cockroachdb/errors"
)

// errCapabilityFanoutBadInput is the sentinel wrapped by every
// input-validation refusal in this file so callers can errors.Is()
// against it without parsing strings. The concrete failure adds a
// %w-wrapped detail.
var errCapabilityFanoutBadInput = errors.New("capability fan-out: bad input")

// RouteMember is one peer entry in a Raft group. The fan-out helper
// dials Address and identifies the node by FullNodeID for dedup
// across groups (a node serving multiple groups is probed once).
type RouteMember struct {
	FullNodeID uint64
	Address    string
}

// RouteGroup is one Raft group's membership. Voters and Learners are
// kept separate at the input level so the cutover RPC handler can
// log them distinctly, but CapabilityFanout treats them identically
// per the §4.1 contract "every (voter ∪ learner) of every Raft
// group". The 6D design pins that learner unreachability is a hard
// no the same way voter unreachability is — see §8 / row "One
// learner unreachable during fan-out".
type RouteGroup struct {
	GroupID  uint64
	Voters   []RouteMember
	Learners []RouteMember
}

// RouteSnapshot is the input the cutover RPC handler builds from
// the Raft engine's membership view and passes to CapabilityFanout.
// Independent of the route-catalog `distribution.CatalogSnapshot`,
// which only carries shard→group mappings, not per-group membership.
type RouteSnapshot struct {
	Groups []RouteGroup
}

// CapabilityVerdict is one node's per-call outcome. Reachable=false
// means the dial or the RPC timed out / failed at transport level —
// not a "no" answer from the peer. The cutover RPC handler treats
// both Reachable=false and EncryptionCapable=false as hard refusals
// per the §8 failure-modes table, but the verdict separates them so
// the operator-facing error message can name the precise reason.
type CapabilityVerdict struct {
	FullNodeID        uint64
	EncryptionCapable bool
	BuildSHA          string
	SidecarPresent    bool
	Reachable         bool
	Err               error
}

// CapabilityFanoutResult is the aggregated outcome. OK is true iff
// every verdict has Reachable && EncryptionCapable — there is no
// partial-success mode per §4.3.
//
// Named CapabilityFanoutResult rather than the design-doc's
// `FanoutResult` to avoid a collision with the unrelated
// `admin.FanoutResult` in `keyviz_fanout.go` (KeyViz cluster fan-out
// shipped earlier in the same package).
type CapabilityFanoutResult struct {
	Verdicts []CapabilityVerdict
	OK       bool
}

// DialFunc opens a connection to one node's admin endpoint and
// returns an EncryptionAdmin client plus a cleanup closure. The
// helper invokes the closure exactly once per successful dial,
// regardless of how the RPC subsequently resolved.
//
// The 6D design says "DialFunc reuses the existing admin connection
// pool" — the concrete implementation will reach for whatever
// connection-pool helper the caller already holds (e.g. the
// `internal/admin.ForwardClient` connection pool for TLS-aware
// dials).
type DialFunc func(ctx context.Context, address string) (pb.EncryptionAdminClient, func(), error)

// CapabilityFanout fans GetCapability out to every unique
// (voter ∪ learner) of every group in routes. Concurrent; bounded
// by timeout regardless of how many members respond. Missing
// responses surface as Reachable=false verdicts (no partial-success
// mode — see §4.3).
//
// Dedup key: FullNodeID. A node serving multiple groups is probed
// exactly once. Members with FullNodeID=0 are treated as distinct
// dedup keys per unique address; this case appears in stub catalogs
// where the dedup-by-id contract has not been satisfied — the
// helper still completes by falling back to address-based identity
// rather than silently collapsing every zero-id row into one probe.
//
// Returns (result, nil) on every input. The error slot is reserved
// for input validation failures (zero-member snapshot, etc.) so
// callers can keep their existing `err != nil → refuse` shape.
func CapabilityFanout(
	ctx context.Context,
	routes RouteSnapshot,
	dial DialFunc,
	timeout time.Duration,
) (CapabilityFanoutResult, error) {
	if dial == nil {
		return CapabilityFanoutResult{}, pkgerrors.Wrap(errCapabilityFanoutBadInput, "dial func is nil")
	}
	if timeout <= 0 {
		return CapabilityFanoutResult{}, pkgerrors.Wrapf(errCapabilityFanoutBadInput, "timeout must be positive, got %v", timeout)
	}

	dedupKeys := buildCapabilityFanoutDedupSet(routes)
	if len(dedupKeys) == 0 {
		return CapabilityFanoutResult{Verdicts: []CapabilityVerdict{}, OK: false}, nil
	}

	fanCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	results := runCapabilityFanoutProbes(fanCtx, dedupKeys, dial)
	return CapabilityFanoutResult{Verdicts: results, OK: capabilityFanoutAllOK(results)}, nil
}

// buildCapabilityFanoutDedupSet folds every voter ∪ learner of
// every group into the dedup map keyed by FullNodeID (or address
// for unidentified rows). Pulled out of CapabilityFanout to keep
// the orchestration body under the cyclomatic-complexity budget.
func buildCapabilityFanoutDedupSet(routes RouteSnapshot) map[string]RouteMember {
	dedupKeys := make(map[string]RouteMember)
	for _, group := range routes.Groups {
		for _, m := range group.Voters {
			addCapabilityFanoutDedupTarget(dedupKeys, m)
		}
		for _, m := range group.Learners {
			addCapabilityFanoutDedupTarget(dedupKeys, m)
		}
	}
	return dedupKeys
}

// runCapabilityFanoutProbes dials every dedup target concurrently
// and returns the per-node verdicts in unspecified order. Bounded
// by ctx — missing responses surface as Reachable=false.
func runCapabilityFanoutProbes(ctx context.Context, dedupKeys map[string]RouteMember, dial DialFunc) []CapabilityVerdict {
	results := make([]CapabilityVerdict, 0, len(dedupKeys))
	var mu sync.Mutex
	var wg sync.WaitGroup
	for _, m := range dedupKeys {
		wg.Add(1)
		go func(member RouteMember) {
			defer wg.Done()
			verdict := probeCapability(ctx, member, dial)
			mu.Lock()
			results = append(results, verdict)
			mu.Unlock()
		}(m)
	}
	wg.Wait()
	return results
}

func capabilityFanoutAllOK(verdicts []CapabilityVerdict) bool {
	if len(verdicts) == 0 {
		return false
	}
	for _, v := range verdicts {
		if !v.Reachable || !v.EncryptionCapable {
			return false
		}
	}
	return true
}

// addCapabilityFanoutDedupTarget folds a member into the dedup map.
// Key choice: FullNodeID stringified when non-zero, address-prefixed
// otherwise. A zero FullNodeID is treated as "not yet identified"
// and falls back to address identity so two distinct stub rows
// don't collapse into one probe.
func addCapabilityFanoutDedupTarget(m map[string]RouteMember, member RouteMember) {
	if member.Address == "" {
		return
	}
	key := capabilityFanoutDedupKey(member)
	if _, exists := m[key]; exists {
		return
	}
	m[key] = member
}

func capabilityFanoutDedupKey(member RouteMember) string {
	if member.FullNodeID != 0 {
		return "id:" + uint64ToDecimal(member.FullNodeID)
	}
	return "addr:" + member.Address
}

// uint64ToDecimal avoids pulling fmt for one-call hot-path
// stringification used only to build dedup keys.
func uint64ToDecimal(v uint64) string {
	if v == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for v > 0 {
		i--
		buf[i] = byte('0' + v%10)
		v /= 10
	}
	return string(buf[i:])
}

func probeCapability(ctx context.Context, member RouteMember, dial DialFunc) CapabilityVerdict {
	verdict := CapabilityVerdict{FullNodeID: member.FullNodeID}
	client, closer, err := dial(ctx, member.Address)
	if err != nil {
		verdict.Err = pkgerrors.Wrapf(err, "dial %s", member.Address)
		return verdict
	}
	if closer != nil {
		defer closer()
	}
	report, err := client.GetCapability(ctx, &pb.Empty{})
	if err != nil {
		verdict.Err = pkgerrors.Wrapf(err, "GetCapability %s", member.Address)
		return verdict
	}
	verdict.Reachable = true
	verdict.EncryptionCapable = report.GetEncryptionCapable()
	verdict.BuildSHA = report.GetBuildSha()
	verdict.SidecarPresent = report.GetSidecarPresent()
	if report.GetFullNodeId() != 0 {
		verdict.FullNodeID = report.GetFullNodeId()
	}
	return verdict
}
