package kv

import (
	pb "github.com/bootjp/elastickv/proto"
	"github.com/cockroachdb/errors"
)

var ErrTTLInlineCapabilityRequired = errors.New("TTL-inline replicated capability is not active")

// Relocations are single-owner 1PCs. Refuse other phases explicitly instead
// of allowing an intent encoder or raw request path to discard the condition.
func (f *kvFSM) validateAbsenceRequest(request *pb.Request) error {
	for _, mutation := range request.GetMutations() {
		if mutation == nil || !mutation.RequireAbsent {
			continue
		}
		if !f.ttlInlineCapability.Load() {
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
