package kv

import (
	"bytes"

	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/internal/raftbootstrap"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/cockroachdb/errors"
	"google.golang.org/protobuf/proto"
)

var ErrInvalidGuardedBootstrapInner = errors.New("invalid guarded bootstrap inner command")

// EncodeGuardedBootstrapCommand validates and encodes a protected bootstrap
// watermark or catalog command.
func EncodeGuardedBootstrapCommand(command raftbootstrap.GuardedCommand) ([]byte, error) {
	encoded, err := raftbootstrap.EncodeGuardedCommand(command, ValidateGuardedBootstrapInner)
	return encoded, errors.WithStack(err)
}

// ParseGuardedBootstrapCommand recognizes the reserved control prefix and
// validates the kind-specific inner Raft command before returning it.
func ParseGuardedBootstrapCommand(payload []byte) (raftbootstrap.GuardedCommand, bool, error) {
	command, matched, err := raftbootstrap.ParseGuardedCommand(payload, ValidateGuardedBootstrapInner)
	return command, matched, errors.WithStack(err)
}

// ValidateGuardedBootstrapInner binds each protected control kind to its only
// accepted existing Raft command shape.
func ValidateGuardedBootstrapInner(kind raftbootstrap.ControlKind, inner []byte) error {
	switch kind {
	case raftbootstrap.ControlGuardedWatermark:
		return validateGuardedWatermarkInner(inner)
	case raftbootstrap.ControlGuardedCatalog:
		return validateGuardedCatalogInner(inner)
	case raftbootstrap.ControlActivate, raftbootstrap.ControlReserve, raftbootstrap.ControlAbort:
		return errors.WithStack(ErrInvalidGuardedBootstrapInner)
	}
	return errors.WithStack(ErrInvalidGuardedBootstrapInner)
}

func validateGuardedWatermarkInner(inner []byte) error {
	_, matched, err := DecodeWatermarkAdvance(inner)
	if err != nil || !matched {
		return errors.Wrap(ErrInvalidGuardedBootstrapInner, "watermark payload")
	}
	return nil
}

func validateGuardedCatalogInner(inner []byte) error {
	if len(inner) < 2 || inner[0] != raftEncodeSingle {
		return errors.Wrap(ErrInvalidGuardedBootstrapInner, "catalog raft envelope")
	}
	var request pb.Request
	if err := proto.Unmarshal(inner[1:], &request); err != nil {
		return errors.Wrap(ErrInvalidGuardedBootstrapInner, "catalog request protobuf")
	}
	if len(request.ProtoReflect().GetUnknown()) != 0 || !canonicalGuardedCatalogRequest(inner[1:], &request) {
		return errors.Wrap(ErrInvalidGuardedBootstrapInner, "noncanonical catalog request")
	}
	if !validGuardedCatalogRequest(&request) {
		return errors.WithStack(ErrInvalidGuardedBootstrapInner)
	}
	return nil
}

func canonicalGuardedCatalogRequest(encoded []byte, request *pb.Request) bool {
	canonical, err := (proto.MarshalOptions{Deterministic: true}).Marshal(request)
	return err == nil && bytes.Equal(encoded, canonical)
}

func validGuardedCatalogRequest(request *pb.Request) bool {
	if !validGuardedCatalogRequestHeader(request) || !validGuardedCatalogMeta(request) {
		return false
	}
	return guardedCatalogMutationsValid(request.GetMutations()[1:]) && guardedCatalogReadKeysValid(request.GetReadKeys())
}

func validGuardedCatalogRequestHeader(request *pb.Request) bool {
	return request.GetIsTxn() && request.GetPhase() == pb.Phase_NONE && request.GetTs() != 0 &&
		request.GetTs() != ^uint64(0) && request.GetObservedRouteVersion() == 0 &&
		len(request.GetMutations()) >= 2 && len(request.GetReadKeys()) != 0 &&
		len(request.GetWriteFenceBypassKeys()) == 0
}

func validGuardedCatalogMeta(request *pb.Request) bool {
	metaMutation := request.GetMutations()[0]
	if metaMutation == nil || metaMutation.GetOp() != pb.Op_PUT || !bytes.Equal(metaMutation.GetKey(), []byte(TxnMetaPrefix)) {
		return false
	}
	meta, err := DecodeTxnMeta(metaMutation.GetValue())
	if err != nil || len(meta.PrimaryKey) == 0 || !distribution.IsCatalogKey(meta.PrimaryKey) ||
		meta.CommitTS <= request.GetTs() || meta.PrevCommitTS != 0 || meta.LockTTLms != 0 {
		return false
	}
	return true
}

func guardedCatalogMutationsValid(mutations []*pb.Mutation) bool {
	for _, mutation := range mutations {
		if mutation == nil || !distribution.IsCatalogKey(mutation.GetKey()) {
			return false
		}
	}
	return true
}

func guardedCatalogReadKeysValid(readKeys [][]byte) bool {
	for _, key := range readKeys {
		if !distribution.IsCatalogKey(key) {
			return false
		}
	}
	return true
}
