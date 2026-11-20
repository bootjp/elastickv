package kv

import (
	"context"

	"github.com/bootjp/elastickv/internal/raftengine"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/cockroachdb/errors"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

var ErrVersionCASUnavailable = raftengine.ErrVersionCASUnavailable

const versionCASFrame = raftengine.VersionCASFrame

func IsVersionCASRaftPayload(payload []byte) bool {
	return raftengine.IsVersionCASRaftPayload(payload)
}

func DecodeVersionCASRaftPayload(payload []byte) (*pb.Request, error) {
	if !raftengine.IsVersionCASRaftPayload(payload) {
		return nil, errors.WithStack(ErrInvalidRequest)
	}
	var request pb.Request
	if err := proto.Unmarshal(payload[len(versionCASFrame):], &request); err != nil {
		return nil, errors.WithStack(err)
	}
	if !HasCASMutation([]*pb.Request{&request}) {
		return nil, errors.WithStack(ErrInvalidRequest)
	}
	if err := ValidateCASProposalShape([]*pb.Request{&request}); err != nil {
		return nil, err
	}
	return &request, nil
}

func (f *kvFSM) applyVersionCASFrame(ctx context.Context, payload []byte) any {
	req, err := DecodeVersionCASRaftPayload(payload)
	if err != nil {
		return haltErr(errors.Mark(err, ErrRaftEntryApply))
	}
	if !f.commitTSFenceEnabled.Load() {
		return haltErr(errors.Mark(errors.New("version CAS entry precedes commit-ts fence activation"), ErrRaftEntryApply))
	}
	active, err := MigrationVersionIdentityEnabled(ctx, f.store)
	if err != nil || !active {
		return haltErr(errors.Mark(errors.Wrap(errors.Join(ErrVersionCASUnavailable, err),
			"version CAS entry requires migration version identity activation"), ErrRaftEntryApply))
	}
	return f.applyCASEntry(ctx, []*pb.Request{req}, true)
}

// VersionCASAdmission verifies the local leader and every configured replica.
// The returned context carries the proof's term and configuration to Raft.
type VersionCASAdmission func(context.Context) (context.Context, error)

// WithVersionCASAdmission enables the local proposal path only when check is installed.
func WithVersionCASAdmission(check VersionCASAdmission) TransactionOption {
	return func(t *TransactionManager) { t.versionCASAdmission = check }
}

// ConfigureVersionCASAdmission is called during startup before the proxy serves requests.
func (p *LeaderProxy) ConfigureVersionCASAdmission(check VersionCASAdmission) {
	WithVersionCASAdmission(check)(p.tm)
}

const versionCASForwardDomain = "elastickv.internal.version_cas"
const versionCASForwardNotAdmitted = "NOT_ADMITTED"

func VersionCASForwardUnavailable(cause error) error {
	st, err := status.New(codes.FailedPrecondition, "Version CAS admission unavailable").WithDetails(&errdetails.ErrorInfo{
		Domain: versionCASForwardDomain,
		Reason: versionCASForwardNotAdmitted,
	})
	if err != nil {
		return cause
	}
	return errors.WithStack(st.Err())
}

func isVersionCASForwardUnavailable(err error) bool {
	st := status.Convert(err)
	if st.Code() == codes.Unimplemented {
		// A preceding binary has no CAS Forward RPC and cannot have executed it.
		return true
	}
	if st.Code() != codes.FailedPrecondition {
		return false
	}
	details := st.Details()
	if len(details) != 1 {
		return false
	}
	detail, ok := details[0].(*errdetails.ErrorInfo)
	return ok && detail.GetDomain() == versionCASForwardDomain && detail.GetReason() == versionCASForwardNotAdmitted
}
