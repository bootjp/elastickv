package main

import (
	"bytes"
	"context"
	"math"

	"github.com/bootjp/elastickv/internal/peersession"
	"github.com/bootjp/elastickv/internal/raftadmission"
	"github.com/bootjp/elastickv/internal/raftengine"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/cockroachdb/errors"
	"google.golang.org/grpc"
)

type bootstrapRegistrationKey struct{}
type bootstrapRegistrationReceiverKey struct{}

// Install before the startup auth exception on every listener, using the
// independently configured DEFAULT group (not the listener's own group).
// Omission denies typed registration; no metadata can supply this authority.
func bootstrapRegistrationReceiverInterceptor(defaultGroup uint64) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, next grpc.UnaryHandler) (any, error) {
		ctx = context.WithValue(ctx, bootstrapRegistrationReceiverKey{}, defaultGroup)
		reply, err := next(ctx, req)
		return reply, errors.WithStack(err)
	}
}

func bootstrapRegistrationReceiverMatches(ctx context.Context, group uint64) bool {
	expected, configured := ctx.Value(bootstrapRegistrationReceiverKey{}).(uint64)
	return configured && expected == group
}

type bootstrapRegistrationSource struct {
	network       *bootstrapNetwork
	group         uint64
	member        uint64
	validateEpoch func(dek uint32, epoch uint16) error
}

// withBootstrapRegistration is installed before arming registration watchers.
// validateEpoch must check the locally owned durable epoch for the DEK's purpose
// (storage and Raft epochs may differ). It is not an RPC-supplied assertion.
// The network's root owns cancellation/join; watchers retain their errgroup owner.
func withBootstrapRegistration(ctx context.Context, network *bootstrapNetwork, group, member uint64, validateEpoch func(uint32, uint16) error) (context.Context, error) {
	if ctx == nil || network == nil || network.control == nil || validateEpoch == nil {
		return nil, errors.New("protected registration requires network and durable epoch validation")
	}
	local, ok := network.control.groups[group]
	if !ok || member == 0 || local.local.Member != member {
		return nil, errors.WithStack(peersession.ErrDenied)
	}
	return context.WithValue(ctx, bootstrapRegistrationKey{}, &bootstrapRegistrationSource{
		network: network, group: group, member: member, validateEpoch: validateEpoch,
	}), nil
}

func bootstrapRegistrationFromContext(ctx context.Context) *bootstrapRegistrationSource {
	source, _ := ctx.Value(bootstrapRegistrationKey{}).(*bootstrapRegistrationSource)
	return source
}

func (s *bootstrapRegistrationSource) validate(entry []byte, req *pb.RegisterEncryptionWriterRequest) error {
	_, err := bootstrapRegistrationRequest(peersession.RequestBinding{From: peersession.Node{Member: s.member}}, req)
	if err != nil {
		return errors.WithStack(err)
	}
	w := req.GetWriters()[0]
	// The descriptor checked the narrowing bound and all nested unknown fields.
	epoch, err := bootstrapRegistrationEpoch(w.GetLocalEpoch())
	if err != nil {
		return err
	}
	if !bytes.Equal(entry, registrationEntry(req.GetDekId(), s.member, epoch)) {
		return errors.WithStack(peersession.ErrDenied)
	}
	return errors.WithStack(s.validateEpoch(req.GetDekId(), epoch))
}

func bootstrapRegistrationEpoch(epoch uint32) (uint16, error) {
	if epoch > math.MaxUint16 {
		return 0, errors.WithStack(peersession.ErrDenied)
	}
	return uint16(epoch), nil
}

func (s *bootstrapRegistrationSource) propose(ctx context.Context, proposer raftengine.Proposer, entry []byte, req *pb.RegisterEncryptionWriterRequest) error {
	if proposer == nil {
		return errors.New("protected registration requires local proposer")
	}
	if err := s.validate(entry, req); err != nil {
		return err
	}
	call, stop, err := s.network.control.begin(ctx)
	if err != nil {
		return err
	}
	defer stop()
	owned, finish, err := s.network.control.domain.admission.Enter(call, raftadmission.Registration, s.group)
	if err != nil {
		return errors.WithStack(err)
	}
	_, err = runBootstrapStartupControl(owned, req, func(ctx context.Context, _ any) (any, error) {
		if err := s.validate(entry, req); err != nil {
			return nil, err
		}
		result, err := proposer.Propose(ctx, entry)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		if result == nil || result.CommitIndex == 0 {
			return nil, errors.New("protected registration lacks applied proposal result")
		}
		return nil, nil
	}, finish)
	return err
}

func (s *bootstrapRegistrationSource) forward(ctx context.Context, address string, req *pb.RegisterEncryptionWriterRequest) error {
	w := req.GetWriters()
	if len(w) != 1 {
		return errors.WithStack(peersession.ErrDenied)
	}
	epoch, err := bootstrapRegistrationEpoch(w[0].GetLocalEpoch())
	if err != nil {
		return err
	}
	if err := s.validate(registrationEntry(req.GetDekId(), s.member, epoch), req); err != nil {
		return err
	}
	return s.network.control.Invoke(ctx, s.group, address, pb.EncryptionAdmin_RegisterEncryptionWriter_FullMethodName,
		req, &pb.RegisterEncryptionWriterResponse{})
}
