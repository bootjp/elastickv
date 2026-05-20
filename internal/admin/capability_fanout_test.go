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
