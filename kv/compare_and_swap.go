package kv

import (
	"bytes"
	"context"

	"github.com/bootjp/elastickv/distribution"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
)

func validCASOperation(group *OperationGroup[OP], elem *Elem[OP]) bool {
	return group.IsTxn && len(group.Elems) == 1 && len(group.ReadKeys) == 0 &&
		len(group.PrimaryKey) == 0 && !group.RequireAllParticipants &&
		group.StartTS == 0 && group.CommitTS == 0 && group.PrevCommitTS == 0 &&
		group.ObservedRouteVersion == 0 &&
		validCASElem(elem)
}

func validCASElem(elem *Elem[OP]) bool {
	return IsCASUserKey(elem.Key) && elem.ExpectedCommitTS != 0 &&
		!elem.RequireAbsent && elem.CommitTSValueOffset == 0 &&
		elem.GroupID == 0 && !elem.RequireGroupMatch && elem.ExpectedGroupID == 0
}

// HasCASMutation also detects a predicate attached to another operation, so
// validation cannot silently discard the expected version.
func HasCASMutation(reqs []*pb.Request) bool {
	for _, req := range reqs {
		if requestUsesCAS(req) {
			return true
		}
	}
	return false
}

// IsCASUserKey is the key namespace shared by public and forwarded CAS.
func IsCASUserKey(key []byte) bool {
	return len(key) != 0 && !distribution.IsReservedControlKey(key) && !IsTxnInternalKey(key) &&
		!distribution.IsMigrationKnownInternalKey(key) && !store.IsReplicatedControlKey(key)
}

// ValidateCASProposalShape accepts only a stamped standalone one-phase CAS.
func ValidateCASProposalShape(reqs []*pb.Request) error {
	if !HasCASMutation(reqs) {
		return nil
	}
	return validateCASProposalShape(reqs)
}

func validateCASProposalShape(reqs []*pb.Request) error {
	_, meta, ok := casRequestMutation(reqs)
	if !ok || reqs[0].GetVersionCasGroupId() == 0 || reqs[0].Ts == 0 || meta.CommitTS <= reqs[0].Ts {
		return errors.WithStack(ErrInvalidRequest)
	}
	return nil
}

// ForwardCASMutation leaves timestamp and catalog proof stamping to the
// receiving leader.
func ForwardCASMutation(reqs []*pb.Request) (*pb.Mutation, error) {
	mut, meta, ok := casRequestMutation(reqs)
	if !ok || reqs[0].GetVersionCasGroupId() == 0 || reqs[0].Ts != 0 || meta.CommitTS != 0 || reqs[0].TimestampAdmissionFloor != 0 ||
		reqs[0].ObservedRouteVersion != 0 {
		return nil, errors.WithStack(ErrInvalidRequest)
	}
	return mut, nil
}

func casRequestMutation(reqs []*pb.Request) (*pb.Mutation, TxnMeta, bool) {
	if len(reqs) != 1 {
		return nil, TxnMeta{}, false
	}
	req := reqs[0]
	if !req.GetIsTxn() || req.Phase != pb.Phase_NONE || len(req.Mutations) != 2 ||
		len(req.ReadKeys) != 0 || len(req.TimestampBindingOperationId) != 0 {
		return nil, TxnMeta{}, false
	}
	header, mut := req.Mutations[0], req.Mutations[1]
	if !validCASHeader(header) ||
		!validCASMutation(mut) || !validCASBypass(req.WriteFenceBypassKeys, mut.Key) {
		return nil, TxnMeta{}, false
	}
	return decodeCASRequestMeta(header, mut)
}

func validCASHeader(header *pb.Mutation) bool {
	return header != nil && header.Op == pb.Op_PUT && string(header.Key) == txnMetaPrefix &&
		header.ExpectedCommitTs == 0 && !header.RequireAbsent && header.CommitTsValueOffset == 0
}

func decodeCASRequestMeta(header, mut *pb.Mutation) (*pb.Mutation, TxnMeta, bool) {
	meta, err := DecodeTxnMeta(header.Value)
	return mut, meta, err == nil && bytes.Equal(meta.PrimaryKey, mut.Key) &&
		meta.PrevCommitTS == 0 && meta.PrepareCommitTS == 0 && meta.LockTTLms == 0
}

func validCASMutation(mut *pb.Mutation) bool {
	return mut != nil && mut.Op == pb.Op_COMPARE_AND_SWAP && IsCASUserKey(mut.Key) &&
		mut.ExpectedCommitTs != 0 && !mut.RequireAbsent && mut.CommitTsValueOffset == 0
}

func validCASBypass(bypass [][]byte, key []byte) bool {
	return len(bypass) == 0 || (len(bypass) == 1 && bytes.Equal(bypass[0], key))
}

// IsCertifiedCASMismatch recognizes the indexed local verdict or the same
// verdict carried by a validated reply to this CAS's forwarding call.
func IsCertifiedCASMismatch(err error) bool {
	return !errors.Is(err, ErrTransactionOutcomeUnknown) && errors.Is(err, ErrCASMismatch) &&
		IsCertifiedOnePhaseConflict(err)
}

func (c *ShardedCoordinator) dispatchCAS(ctx context.Context, group *OperationGroup[OP]) (*CoordinateResponse, error) {
	elem := group.Elems[0]
	if !versionCASDirectKey(elem.Key) || c.partitionResolverRecognisesPointKey(elem.Key) {
		return nil, errors.Wrap(ErrVersionCASUnavailable, "key requires alternate routing")
	}
	if c.engine == nil {
		return nil, errors.Wrap(ErrVersionCASUnavailable, "route catalog is unavailable")
	}
	route, ok := c.engine.GetRoute(elem.Key)
	if !ok || !versionCASInitialRoute(route) {
		return nil, errors.Wrap(ErrVersionCASUnavailable, "route has migration history or is unavailable")
	}
	mut := elemToMutation(elem)
	c.observeMutation(route.RouteID, mut, group.KeyVizLabel)
	ctx = context.WithValue(ctx, leaderIssuedCommitTSContextKey{}, true)
	return c.dispatchSingleShardTxn(ctx, 0, 0, 0, elem.Key, route.GroupID, []*pb.Mutation{mut}, nil,
		0, nil)
}
