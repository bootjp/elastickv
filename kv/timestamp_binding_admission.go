package kv

import (
	"context"
	"sync"
	"time"

	"github.com/bootjp/elastickv/internal/raftengine"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"google.golang.org/protobuf/proto"
)

const timestampBindingAdmissionBudget = 5 * time.Second

var ErrTimestampBindingPending = errors.New("timestamp binding admission outcome unresolved")

// This gate is intentionally process-local. It prevents a live admission
// process from reissuing an ID whose first attempt may have reached Raft.
// Durable reconciliation is required before enabling this path in production.
var homeBindingAttempts sync.Map // [16]byte -> *homeBindingAttempt

type homeBindingAttempt struct {
	mu        sync.Mutex
	pending   bool
	term      uint64
	timestamp uint64
}

func homeBindingLeaderTerm(leader raftengine.LeaderView) uint64 {
	status, ok := leader.(raftengine.StatusReader)
	if !ok {
		return 0
	}
	current := status.Status()
	if current.State != raftengine.StateLeader {
		return 0
	}
	return current.Term
}

// PrepareHomeRawTimestampBinding prepares one raw write for the opt-in atomic
// data-group apply path. The issuer must be a leader-owned authenticated TSO
// route, not a numeric timestamp supplied by the caller.
func PrepareHomeRawTimestampBinding(
	ctx context.Context,
	leader raftengine.LeaderView,
	metadata store.RaftControlMetadataStore,
	issue func(context.Context) (uint64, error),
	request *pb.Request,
) (*pb.Request, error) {
	operationID, err := validateHomeRawBindingInputs(leader, metadata, issue, request)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, timestampBindingAdmissionBudget)
	defer cancel()
	attemptValue, _ := homeBindingAttempts.LoadOrStore(operationID, &homeBindingAttempt{})
	attempt, ok := attemptValue.(*homeBindingAttempt)
	if !ok {
		return nil, errors.WithStack(ErrTimestampBindingInvalid)
	}
	bound, found, term, err := readHomeBinding(ctx, leader, metadata, operationID)
	if err != nil {
		return nil, err
	}
	owned := proto.CloneOf(request)
	if found {
		return prepareExistingHomeRawBinding(owned, bound, operationID, term)
	}
	attempt.mu.Lock()
	defer attempt.mu.Unlock()
	if err := attempt.reserve(term, leader); err != nil {
		return nil, err
	}
	// An issue error may mean the timestamp was already handed to a proposer.
	// Never release this ID on an unknown outcome, even if the caller cancels.
	timestamp, err := issue(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "issue home write timestamp")
	}
	if timestamp == 0 || homeBindingLeaderTerm(leader) != term {
		return nil, errors.WithStack(ErrTimestampBindingInvalid)
	}
	owned.Ts = timestamp
	owned.TimestampBindingAdmissionTerm = term
	if _, err := timestampBindingForCreationRequest(owned); err != nil {
		return nil, err
	}
	attempt.timestamp = timestamp
	return owned, nil
}

func (a *homeBindingAttempt) reserve(term uint64, leader raftengine.LeaderView) error {
	if homeBindingLeaderTerm(leader) != term {
		return errors.WithStack(ErrLeaderNotFound)
	}
	if a.pending && term <= a.term {
		return errors.WithStack(ErrTimestampBindingPending)
	}
	// The current-term applied read proves the prior-term entry did not
	// commit. A delayed prior-term request is rejected by ProposeIfTerm.
	a.pending = true
	a.term = term
	a.timestamp = 0
	return nil
}

func clearAppliedHomeBindingAttempt(binding TimestampBinding) {
	value, ok := homeBindingAttempts.Load(binding.OperationID)
	if !ok {
		return
	}
	attempt, ok := value.(*homeBindingAttempt)
	if !ok {
		return
	}
	attempt.mu.Lock()
	defer attempt.mu.Unlock()
	if attempt.pending && attempt.timestamp == binding.Timestamp {
		homeBindingAttempts.CompareAndDelete(binding.OperationID, attempt)
	}
}

func prepareExistingHomeRawBinding(owned *pb.Request, bound TimestampBinding, operationID [16]byte, term uint64) (*pb.Request, error) {
	owned.Ts = bound.Timestamp
	owned.TimestampBindingAdmissionTerm = term
	candidate, err := timestampBindingForCreationRequest(owned)
	if err != nil {
		return nil, err
	}
	if candidate != bound {
		return nil, errors.WithStack(ErrTimestampBindingConflict)
	}
	homeBindingAttempts.Delete(operationID)
	return owned, nil
}

func validateHomeRawBindingInputs(leader raftengine.LeaderView, metadata store.RaftControlMetadataStore,
	issue func(context.Context) (uint64, error), request *pb.Request) ([16]byte, error) {
	var operationID [16]byte
	if leader == nil || metadata == nil || issue == nil || !rawBindingRequestUnstamped(request) {
		return operationID, errors.WithStack(ErrTimestampBindingInvalid)
	}
	if _, ok := leader.(raftengine.StatusReader); !ok {
		return operationID, errors.WithStack(ErrTimestampBindingInvalid)
	}
	if len(request.TimestampBindingOperationId) != len(operationID) {
		return operationID, errors.WithStack(ErrTimestampBindingInvalid)
	}
	copy(operationID[:], request.TimestampBindingOperationId)
	if operationID == ([16]byte{}) || leader.State() != raftengine.StateLeader {
		return operationID, errors.WithStack(ErrTimestampBindingInvalid)
	}
	return operationID, nil
}

func rawBindingRequestUnstamped(request *pb.Request) bool {
	return request != nil && !request.IsTxn && request.Phase == pb.Phase_NONE &&
		request.Ts == 0 && request.TimestampAdmissionFloor == 0 && request.TimestampBindingAdmissionTerm == 0
}

func readHomeBinding(ctx context.Context, leader raftengine.LeaderView, metadata store.RaftControlMetadataStore,
	operationID [16]byte) (TimestampBinding, bool, uint64, error) {
	term := homeBindingLeaderTerm(leader)
	if term == 0 {
		return TimestampBinding{}, false, 0, errors.WithStack(ErrLeaderNotFound)
	}
	if _, err := leader.LinearizableRead(ctx); err != nil {
		return TimestampBinding{}, false, 0, errors.Wrap(err, "read home binding barrier")
	}
	if homeBindingLeaderTerm(leader) != term {
		return TimestampBinding{}, false, 0, errors.WithStack(ErrLeaderNotFound)
	}
	bound, found, err := LookupTimestampBinding(ctx, metadata, operationID)
	if err != nil {
		return TimestampBinding{}, false, 0, errors.Wrap(err, "lookup home binding")
	}
	if err := ctx.Err(); err != nil {
		return TimestampBinding{}, false, 0, errors.WithStack(err)
	}
	if homeBindingLeaderTerm(leader) != term {
		return TimestampBinding{}, false, 0, errors.WithStack(ErrLeaderNotFound)
	}
	return bound, found, term, nil
}
