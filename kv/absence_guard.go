package kv

import (
	pb "github.com/bootjp/elastickv/proto"
	"github.com/cockroachdb/errors"
)

var ErrTTLInlineCapabilityRequired = errors.New("TTL-inline replicated capability is not active")

type TTLInlineCapabilityReader interface {
	TTLInlineCapabilityActive() bool
}

var _ TTLInlineCapabilityReader = (*kvFSM)(nil)

// Relocations are single-owner 1PCs. Refuse other phases explicitly instead
// of allowing an intent encoder or raw request path to discard the condition.
func (f *kvFSM) validateAbsenceRequest(request *pb.Request) error {
	for _, mutation := range request.GetMutations() {
		if mutation == nil || !mutation.RequireAbsent {
			continue
		}
		if !f.TTLInlineCapabilityActive() {
			return errors.WithStack(ErrTTLInlineCapabilityRequired)
		}
		if !request.IsTxn || request.Phase != pb.Phase_NONE || mutation.Op != pb.Op_PUT || isTxnInternalKey(mutation.Key) {
			return errors.WithStack(ErrInvalidRequest)
		}
	}
	return nil
}

func validateAbsenceMutationKinds(mutations []*pb.Mutation) error {
	for _, mutation := range mutations {
		if mutation.GetRequireAbsent() && (mutation.Op != pb.Op_PUT || isTxnInternalKey(mutation.Key)) {
			return errors.WithStack(ErrInvalidRequest)
		}
	}
	return nil
}

// A pre-capability binary decodes require_absent as an unknown field and
// applies the PUT, so caller-supplied Forward mutations must be checked before
// proposal as well as by the FSM at apply.
func refuseAbsenceProposal(reqs []*pb.Request, capability TTLInlineCapabilityReader) error {
	for _, req := range reqs {
		for _, mutation := range req.GetMutations() {
			if mutation.GetRequireAbsent() {
				if capability == nil || !capability.TTLInlineCapabilityActive() {
					return errors.WithStack(ErrTTLInlineCapabilityRequired)
				}
				if !req.IsTxn || req.Phase != pb.Phase_NONE || mutation.Op != pb.Op_PUT || isTxnInternalKey(mutation.Key) {
					return errors.WithStack(ErrInvalidRequest)
				}
			}
		}
	}
	return nil
}
