package admin

import (
	"context"
	"errors"
	"sort"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/bootjp/elastickv/proto"
	"google.golang.org/grpc"
)

// stubEncryptionAdminClient is a minimal in-test client that
// implements only the methods CapabilityFanout actually invokes
// (GetCapability). Every other method panics so that an accidental
// future use is loud.
type stubEncryptionAdminClient struct {
	pb.EncryptionAdminClient
	report *pb.CapabilityReport
	err    error
}

func (s *stubEncryptionAdminClient) GetCapability(ctx context.Context, _ *pb.Empty, _ ...grpc.CallOption) (*pb.CapabilityReport, error) {
	if s.err != nil {
		return nil, s.err
	}
	return s.report, nil
}

// stubDial maps addresses to canned (client, dialErr) pairs and
// records how many times each address was dialled so the
// dedup-by-full_node_id contract is asserted exactly.
type stubDial struct {
	clients  map[string]*stubEncryptionAdminClient
	dialErrs map[string]error
	dialed   atomic.Int64
	calls    map[string]*atomic.Int64
}

func newStubDial() *stubDial {
	return &stubDial{
		clients:  map[string]*stubEncryptionAdminClient{},
		dialErrs: map[string]error{},
		calls:    map[string]*atomic.Int64{},
	}
}

func (s *stubDial) addOK(addr string, report *pb.CapabilityReport) {
	s.clients[addr] = &stubEncryptionAdminClient{report: report}
	var c atomic.Int64
	s.calls[addr] = &c
}

func (s *stubDial) addDialErr(addr string, err error) {
	s.dialErrs[addr] = err
	var c atomic.Int64
	s.calls[addr] = &c
}

func (s *stubDial) dial(_ context.Context, addr string) (pb.EncryptionAdminClient, func(), error) {
	s.dialed.Add(1)
	if c := s.calls[addr]; c != nil {
		c.Add(1)
	}
	if err, ok := s.dialErrs[addr]; ok {
		return nil, nil, err
	}
	client, ok := s.clients[addr]
	if !ok {
		return nil, nil, errors.New("stubDial: no client registered for " + addr)
	}
	return client, func() {}, nil
}

// TestCapabilityFanout_AllCapable pins the §9 happy-path: every
// node returns encryption_capable=true → Result.OK=true, every
// verdict reachable.
func TestCapabilityFanout_AllCapable(t *testing.T) {
	t.Parallel()
	stub := newStubDial()
	stub.addOK("n1:9000", &pb.CapabilityReport{FullNodeId: 1, EncryptionCapable: true, SidecarPresent: true, BuildSha: "abc"})
	stub.addOK("n2:9000", &pb.CapabilityReport{FullNodeId: 2, EncryptionCapable: true, SidecarPresent: true, BuildSha: "abc"})
	stub.addOK("n3:9000", &pb.CapabilityReport{FullNodeId: 3, EncryptionCapable: true, SidecarPresent: true, BuildSha: "abc"})

	snapshot := RouteSnapshot{Groups: []RouteGroup{{
		GroupID: 1,
		Voters: []RouteMember{
			{FullNodeID: 1, Address: "n1:9000"},
			{FullNodeID: 2, Address: "n2:9000"},
		},
		Learners: []RouteMember{{FullNodeID: 3, Address: "n3:9000"}},
	}}}

	res, err := CapabilityFanout(context.Background(), snapshot, stub.dial, time.Second)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if !res.OK {
		t.Fatalf("expected OK=true with all-capable verdicts, got %+v", res)
	}
	if len(res.Verdicts) != 3 {
		t.Fatalf("expected 3 verdicts, got %d: %+v", len(res.Verdicts), res.Verdicts)
	}
	for _, v := range res.Verdicts {
		if !v.Reachable || !v.EncryptionCapable {
			t.Errorf("verdict %+v should be Reachable && EncryptionCapable", v)
		}
	}
}

// TestCapabilityFanout_OneNotCapable pins the §9 case where every
// member dials successfully but one reports
// encryption_capable=false → Result.OK=false. The verdict list
// still includes every node so the operator-facing error can name
// the refuser.
func TestCapabilityFanout_OneNotCapable(t *testing.T) {
	t.Parallel()
	stub := newStubDial()
	stub.addOK("n1:9000", &pb.CapabilityReport{FullNodeId: 1, EncryptionCapable: true, SidecarPresent: true})
	stub.addOK("n2:9000", &pb.CapabilityReport{FullNodeId: 2, EncryptionCapable: false, SidecarPresent: false})
	stub.addOK("n3:9000", &pb.CapabilityReport{FullNodeId: 3, EncryptionCapable: true, SidecarPresent: true})

	snapshot := RouteSnapshot{Groups: []RouteGroup{{
		GroupID: 1,
		Voters: []RouteMember{
			{FullNodeID: 1, Address: "n1:9000"},
			{FullNodeID: 2, Address: "n2:9000"},
			{FullNodeID: 3, Address: "n3:9000"},
		},
	}}}

	res, err := CapabilityFanout(context.Background(), snapshot, stub.dial, time.Second)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if res.OK {
		t.Fatalf("expected OK=false when one verdict has EncryptionCapable=false, got OK=true; verdicts=%+v", res.Verdicts)
	}
	var refuser *CapabilityVerdict
	for i := range res.Verdicts {
		if res.Verdicts[i].FullNodeID == 2 {
			refuser = &res.Verdicts[i]
		}
	}
	if refuser == nil {
		t.Fatal("expected verdict for node 2 to be present so operator can name the refuser")
	}
	if refuser.Reachable != true || refuser.EncryptionCapable != false {
		t.Errorf("refuser verdict shape wrong: %+v (want Reachable=true, EncryptionCapable=false)", refuser)
	}
}

// TestCapabilityFanout_OneUnreachable pins the §9 case where one
// member's dial fails → Reachable=false on that verdict and
// Result.OK=false. The verdict list still includes the unreachable
// node so the operator-facing error can name it.
func TestCapabilityFanout_OneUnreachable(t *testing.T) {
	t.Parallel()
	stub := newStubDial()
	stub.addOK("n1:9000", &pb.CapabilityReport{FullNodeId: 1, EncryptionCapable: true, SidecarPresent: true})
	stub.addDialErr("n2:9000", errors.New("simulated dial timeout"))
	stub.addOK("n3:9000", &pb.CapabilityReport{FullNodeId: 3, EncryptionCapable: true, SidecarPresent: true})

	snapshot := RouteSnapshot{Groups: []RouteGroup{{
		GroupID: 1,
		Voters: []RouteMember{
			{FullNodeID: 1, Address: "n1:9000"},
			{FullNodeID: 2, Address: "n2:9000"},
			{FullNodeID: 3, Address: "n3:9000"},
		},
	}}}

	res, err := CapabilityFanout(context.Background(), snapshot, stub.dial, time.Second)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if res.OK {
		t.Fatalf("expected OK=false when one node is unreachable, got OK=true; verdicts=%+v", res.Verdicts)
	}
	var unreachable *CapabilityVerdict
	for i := range res.Verdicts {
		if res.Verdicts[i].FullNodeID == 2 {
			unreachable = &res.Verdicts[i]
		}
	}
	if unreachable == nil {
		t.Fatal("expected verdict for node 2 to be present")
	}
	if unreachable.Reachable {
		t.Errorf("expected Reachable=false for dial-failed node, got %+v", unreachable)
	}
	if unreachable.Err == nil {
		t.Errorf("expected Err to be set for dial-failed verdict, got nil")
	}
}

// TestCapabilityFanout_DeduplicatesByFullNodeID pins the §4.1
// contract: a node serving multiple groups is probed exactly once.
// The same FullNodeID appears under group 1 (as voter) and group 2
// (as voter) and group 3 (as learner) — expect exactly one dial
// per unique FullNodeID, exactly one verdict per unique FullNodeID.
func TestCapabilityFanout_DeduplicatesByFullNodeID(t *testing.T) {
	t.Parallel()
	stub := newStubDial()
	stub.addOK("n1:9000", &pb.CapabilityReport{FullNodeId: 1, EncryptionCapable: true, SidecarPresent: true})
	stub.addOK("n2:9000", &pb.CapabilityReport{FullNodeId: 2, EncryptionCapable: true, SidecarPresent: true})

	snapshot := RouteSnapshot{Groups: []RouteGroup{
		{GroupID: 1, Voters: []RouteMember{
			{FullNodeID: 1, Address: "n1:9000"},
			{FullNodeID: 2, Address: "n2:9000"},
		}},
		{GroupID: 2, Voters: []RouteMember{
			{FullNodeID: 1, Address: "n1:9000"},
			{FullNodeID: 2, Address: "n2:9000"},
		}},
		{GroupID: 3, Learners: []RouteMember{
			{FullNodeID: 1, Address: "n1:9000"},
		}},
	}}

	res, err := CapabilityFanout(context.Background(), snapshot, stub.dial, time.Second)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if !res.OK {
		t.Fatalf("expected OK=true, got %+v", res)
	}
	if len(res.Verdicts) != 2 {
		t.Fatalf("expected 2 deduplicated verdicts, got %d: %+v", len(res.Verdicts), res.Verdicts)
	}
	ids := []uint64{res.Verdicts[0].FullNodeID, res.Verdicts[1].FullNodeID}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	if ids[0] != 1 || ids[1] != 2 {
		t.Errorf("expected verdicts for {1, 2}, got %v", ids)
	}
	if got := stub.dialed.Load(); got != 2 {
		t.Errorf("expected exactly 2 dials (one per unique FullNodeID), got %d", got)
	}
	for addr, c := range stub.calls {
		if c.Load() != 1 {
			t.Errorf("address %q dialed %d times, expected 1", addr, c.Load())
		}
	}
}

// TestCapabilityFanout_NilDialRejected pins the input-validation
// contract: a nil DialFunc is a programmer error and returns
// (zero-value, err) so callers fail loudly instead of silently
// completing with zero verdicts.
func TestCapabilityFanout_NilDialRejected(t *testing.T) {
	t.Parallel()
	_, err := CapabilityFanout(context.Background(), RouteSnapshot{}, nil, time.Second)
	if err == nil {
		t.Fatal("expected error for nil dial func")
	}
}

// TestCapabilityFanout_NonPositiveTimeoutRejected pins the input
// validation: a zero or negative timeout would behave like no
// timeout and is a misuse worth refusing.
func TestCapabilityFanout_NonPositiveTimeoutRejected(t *testing.T) {
	t.Parallel()
	_, err := CapabilityFanout(context.Background(), RouteSnapshot{}, func(context.Context, string) (pb.EncryptionAdminClient, func(), error) {
		return nil, nil, errors.New("never called")
	}, 0)
	if err == nil {
		t.Fatal("expected error for zero timeout")
	}
}

// TestCapabilityFanout_EmptyAddressFailsClosed pins the fail-closed
// posture for a malformed route snapshot where a member has no
// Address. The pre-review code silently dropped such rows from the
// dedup set, which would let CapabilityFanout return OK=true even
// though that member was never probed — the cutover would then
// proceed against an un-verified peer. Codex r1 P1 / gemini high /
// coderabbit major all flagged the same defect (PR #793).
//
// Contract pinned here:
//   - The empty-address row is NOT dropped; it appears in
//     Result.Verdicts with Reachable=false.
//   - Result.OK is false because not every verdict is Reachable.
//   - The malformed row's Err field is set so the operator can
//     name the misconfiguration.
func TestCapabilityFanout_EmptyAddressFailsClosed(t *testing.T) {
	t.Parallel()
	stub := newStubDial()
	stub.addOK("n1:9000", &pb.CapabilityReport{FullNodeId: 1, EncryptionCapable: true, SidecarPresent: true})

	snapshot := RouteSnapshot{Groups: []RouteGroup{{
		GroupID: 1,
		Voters: []RouteMember{
			{FullNodeID: 1, Address: "n1:9000"},
			{FullNodeID: 2, Address: ""},
		},
	}}}

	res, err := CapabilityFanout(context.Background(), snapshot, stub.dial, time.Second)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if res.OK {
		t.Fatalf("expected OK=false when a member has empty address, got OK=true (verdicts=%+v)", res.Verdicts)
	}
	if len(res.Verdicts) != 2 {
		t.Fatalf("expected 2 verdicts (incl. empty-address malformed row), got %d: %+v", len(res.Verdicts), res.Verdicts)
	}
	var malformed *CapabilityVerdict
	for i := range res.Verdicts {
		if res.Verdicts[i].FullNodeID == 2 {
			malformed = &res.Verdicts[i]
		}
	}
	if malformed == nil {
		t.Fatal("expected verdict for empty-address member to be present so operator can name it")
	}
	if malformed.Reachable {
		t.Errorf("expected Reachable=false for empty-address verdict, got %+v", malformed)
	}
	if malformed.Err == nil {
		t.Errorf("expected Err to be set on malformed verdict so operator can diagnose, got nil")
	}
}

// TestCapabilityFanout_FullyMalformedRowsNotCollapsed pins the
// sibling fail-closed case: two members with BOTH FullNodeID=0 AND
// Address="" must NOT collapse into one verdict (which would silently
// hide one of the two misconfigured rows). Each gets its own
// Reachable=false verdict via a synthetic dedup key.
func TestCapabilityFanout_FullyMalformedRowsNotCollapsed(t *testing.T) {
	t.Parallel()
	stub := newStubDial()
	snapshot := RouteSnapshot{Groups: []RouteGroup{{
		GroupID: 1,
		Voters: []RouteMember{
			{FullNodeID: 0, Address: ""},
			{FullNodeID: 0, Address: ""},
		},
	}}}

	res, err := CapabilityFanout(context.Background(), snapshot, stub.dial, time.Second)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if res.OK {
		t.Fatalf("expected OK=false for fully-malformed snapshot, got OK=true")
	}
	if len(res.Verdicts) != 2 {
		t.Errorf("expected 2 separate verdicts for two fully-malformed rows, got %d (collapsing would hide one)", len(res.Verdicts))
	}
}

// TestCapabilityFanout_NilClientFromDialFailsClosed pins the
// defensive-programming refusal for a buggy DialFunc that returns
// (nil, nil, nil) — no error but also no client. Codex r2 P1 on
// PR #793 flagged that without this guard the helper would panic on
// client.GetCapability(...) and take down the admin RPC path
// instead of producing a Reachable=false verdict.
//
// Contract pinned here: a nil client is treated as a dial-level
// failure (Reachable=false with Err set) so a misbehaving dialer
// implementation cannot crash the cutover RPC.
func TestCapabilityFanout_NilClientFromDialFailsClosed(t *testing.T) {
	t.Parallel()
	badDial := func(context.Context, string) (pb.EncryptionAdminClient, func(), error) {
		return nil, nil, nil
	}
	snapshot := RouteSnapshot{Groups: []RouteGroup{{
		GroupID: 1,
		Voters:  []RouteMember{{FullNodeID: 1, Address: "n1:9000"}},
	}}}

	res, err := CapabilityFanout(context.Background(), snapshot, badDial, time.Second)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if res.OK {
		t.Fatalf("expected OK=false when dial returns nil client without error, got OK=true")
	}
	if len(res.Verdicts) != 1 {
		t.Fatalf("expected 1 verdict, got %d", len(res.Verdicts))
	}
	v := res.Verdicts[0]
	if v.Reachable {
		t.Errorf("expected Reachable=false for nil-client verdict, got %+v", v)
	}
	if v.Err == nil {
		t.Errorf("expected Err to be set on nil-client verdict, got nil")
	}
}

// TestCapabilityFanout_MismatchedResponderIDFailsClosed pins the
// fail-closed posture for stale-routing / shared-address scenarios:
// when the snapshot says n2:9000 has FullNodeID=2 but the responder
// reports FullNodeID=99, accepting the response would credit
// member 2 as verified despite member 2 never actually answering.
// Codex r3 P1 on PR #793.
//
// Contract pinned here: when both the snapshot and the response
// carry non-zero FullNodeIDs and they disagree, the verdict is
// Reachable=false with an explanatory Err so the cutover RPC
// refuses with OK=false rather than silently passing.
func TestCapabilityFanout_MismatchedResponderIDFailsClosed(t *testing.T) {
	t.Parallel()
	stub := newStubDial()
	stub.addOK("n2:9000", &pb.CapabilityReport{FullNodeId: 99, EncryptionCapable: true, SidecarPresent: true})

	snapshot := RouteSnapshot{Groups: []RouteGroup{{
		GroupID: 1,
		Voters:  []RouteMember{{FullNodeID: 2, Address: "n2:9000"}},
	}}}

	res, err := CapabilityFanout(context.Background(), snapshot, stub.dial, time.Second)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if res.OK {
		t.Fatalf("expected OK=false when responder full_node_id (99) differs from expected (2), got OK=true")
	}
	if len(res.Verdicts) != 1 {
		t.Fatalf("expected 1 verdict, got %d", len(res.Verdicts))
	}
	v := res.Verdicts[0]
	if v.Reachable {
		t.Errorf("expected Reachable=false for mismatched-id verdict, got %+v", v)
	}
	if v.Err == nil {
		t.Errorf("expected Err to be set on mismatched-id verdict, got nil")
	}
}

// hangingEncryptionAdminClient is a test-only client whose
// GetCapability blocks on a channel and deliberately ignores ctx
// cancellation, simulating a buggy gRPC client that fails to honor
// the helper's timeout contract.
type hangingEncryptionAdminClient struct {
	pb.EncryptionAdminClient
	hangCh chan struct{}
}

func (h *hangingEncryptionAdminClient) GetCapability(_ context.Context, _ *pb.Empty, _ ...grpc.CallOption) (*pb.CapabilityReport, error) {
	<-h.hangCh
	return nil, errors.New("hangingEncryptionAdminClient: unblocked")
}

// TestCapabilityFanout_TimeoutBoundsHelperReturn pins the §4.3
// timeout contract: "Returns within `timeout` regardless of how
// many members responded". A DialFunc / client that ignores ctx
// cancellation must NOT be able to hang the admin RPC path
// indefinitely — the helper synthesizes Reachable=false verdicts
// for any probe that hasn't reported by the deadline. Codex r3 P2
// on PR #793.
func TestCapabilityFanout_TimeoutBoundsHelperReturn(t *testing.T) {
	t.Parallel()
	hangCh := make(chan struct{})
	defer close(hangCh)

	hang := &hangingEncryptionAdminClient{hangCh: hangCh}
	dial := func(context.Context, string) (pb.EncryptionAdminClient, func(), error) {
		return hang, func() {}, nil
	}

	snapshot := RouteSnapshot{Groups: []RouteGroup{{
		GroupID: 1,
		Voters:  []RouteMember{{FullNodeID: 1, Address: "n1:9000"}},
	}}}

	start := time.Now()
	res, err := CapabilityFanout(context.Background(), snapshot, dial, 100*time.Millisecond)
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	// Allow a generous margin (5x the timeout) for slow CI; the
	// pre-fix code would have hung indefinitely so any bounded
	// return time is a pass.
	if elapsed > 500*time.Millisecond {
		t.Fatalf("helper hung for %v despite 100ms timeout — contract violated", elapsed)
	}
	if res.OK {
		t.Fatalf("expected OK=false on timeout, got OK=true")
	}
	if len(res.Verdicts) != 1 {
		t.Fatalf("expected 1 verdict (synthesized for hung probe), got %d", len(res.Verdicts))
	}
	if res.Verdicts[0].Reachable {
		t.Errorf("expected Reachable=false for hung probe, got %+v", res.Verdicts[0])
	}
}

// TestCapabilityFanout_EmptySnapshot pins the empty-input case:
// returns OK=false with zero verdicts but no error (the cutover RPC
// can decide its own semantics for "no members" — typically refuse
// because the catalog snapshot is bad).
func TestCapabilityFanout_EmptySnapshot(t *testing.T) {
	t.Parallel()
	res, err := CapabilityFanout(context.Background(), RouteSnapshot{}, func(context.Context, string) (pb.EncryptionAdminClient, func(), error) {
		return nil, nil, errors.New("never called")
	}, time.Second)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if res.OK {
		t.Errorf("expected OK=false on empty snapshot, got OK=true")
	}
	if len(res.Verdicts) != 0 {
		t.Errorf("expected zero verdicts, got %d", len(res.Verdicts))
	}
}
