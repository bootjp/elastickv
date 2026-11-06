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

// Commit validation is deliberately stricter than structural frame parsing.
// Reuse the catalog's typed codecs; a !dist| prefix alone never grants the
// right to persist arbitrary bytes or unknown future record families.
func validateGuardedCatalogCommit(request *pb.Request) error {
	if !validGuardedCatalogCommitHeader(request) {
		return errors.WithStack(ErrInvalidGuardedBootstrapInner)
	}
	meta, err := DecodeTxnMeta(request.Mutations[0].Value)
	if err != nil || !bytes.Equal(EncodeTxnMeta(meta), request.Mutations[0].Value) {
		return errors.WithStack(ErrInvalidGuardedBootstrapInner)
	}
	seen := make(map[string]struct{}, len(request.Mutations))
	for _, mutation := range request.Mutations[1:] {
		if _, duplicate := seen[string(mutation.Key)]; duplicate {
			return errors.WithStack(ErrInvalidGuardedBootstrapInner)
		}
		seen[string(mutation.Key)] = struct{}{}
		if err := validateGuardedCatalogMutation(mutation); err != nil {
			return err
		}
	}
	if _, written := seen[string(meta.PrimaryKey)]; !written {
		return errors.WithStack(ErrInvalidGuardedBootstrapInner)
	}
	return nil
}

func validGuardedCatalogCommitHeader(request *pb.Request) bool {
	return validGuardedCatalogRequest(request) && len(request.ProtoReflect().GetUnknown()) == 0 &&
		guardedCatalogMutationOptionsValid(request.Mutations[0])
}

func guardedCatalogMutationOptionsValid(mutation *pb.Mutation) bool {
	return len(mutation.ProtoReflect().GetUnknown()) == 0 && mutation.CommitTsValueOffset == 0 && !mutation.RequireAbsent
}

func validateGuardedCatalogMutation(mutation *pb.Mutation) error {
	if !guardedCatalogMutationOptionsValid(mutation) {
		return errors.WithStack(ErrInvalidGuardedBootstrapInner)
	}
	switch mutation.Op {
	case pb.Op_PUT:
		canonical, err := guardedCatalogCanonicalValue(mutation.Key, mutation.Value)
		if err != nil || !bytes.Equal(canonical, mutation.Value) {
			return errors.Join(ErrInvalidGuardedBootstrapInner, err)
		}
		return nil
	case pb.Op_DEL:
		if !guardedCatalogDeleteValid(mutation) {
			return errors.WithStack(ErrInvalidGuardedBootstrapInner)
		}
		return nil
	case pb.Op_DEL_PREFIX:
		return errors.WithStack(ErrInvalidGuardedBootstrapInner)
	}
	return errors.WithStack(ErrInvalidGuardedBootstrapInner)
}

func guardedCatalogDeleteValid(mutation *pb.Mutation) bool {
	return len(mutation.Value) == 0 && guardedCatalogDeletableKey(mutation.Key)
}

func guardedCatalogCanonicalValue(key, value []byte) ([]byte, error) {
	switch {
	case bytes.Equal(key, distribution.CatalogVersionKey()), bytes.Equal(key, distribution.CatalogDeltaFloorKey()):
		n, err := distribution.DecodeCatalogVersion(value)
		if err != nil || n == 0 {
			return nil, errors.Join(ErrInvalidGuardedBootstrapInner, err)
		}
		return distribution.EncodeCatalogVersion(n), nil
	case bytes.Equal(key, distribution.CatalogNextRouteIDKey()):
		n, err := distribution.DecodeCatalogNextRouteID(value)
		return distribution.EncodeCatalogNextRouteID(n), err
	case bytes.Equal(key, distribution.CatalogNextSplitJobIDKey()):
		n, err := distribution.DecodeCatalogNextSplitJobID(value)
		return distribution.EncodeCatalogNextSplitJobID(n), err
	case distribution.IsCatalogRouteKey(key):
		return guardedCatalogRouteValue(key, value)
	case distribution.IsCatalogDeltaKey(key):
		return guardedCatalogDeltaValue(key, value)
	case distribution.IsCatalogSplitJobKey(key), distribution.IsCatalogSplitJobHistoryKey(key):
		return guardedCatalogJobValue(key, value)
	case distribution.IsCatalogOwnershipPartitionKey(key):
		return guardedCatalogOwnershipValue(key, value)
	}
	return nil, errors.WithStack(ErrInvalidGuardedBootstrapInner)
}

func guardedCatalogRouteValue(key, value []byte) ([]byte, error) {
	route, err := distribution.DecodeRouteDescriptor(value)
	if err != nil || !bytes.Equal(key, distribution.CatalogRouteKey(route.RouteID)) {
		return nil, errors.Join(ErrInvalidGuardedBootstrapInner, err)
	}
	encoded, err := distribution.EncodeRouteDescriptor(route)
	return encoded, errors.WithStack(err)
}

func guardedCatalogDeltaValue(key, value []byte) ([]byte, error) {
	delta, err := distribution.DecodeCatalogDelta(value)
	if err != nil || !bytes.Equal(key, distribution.CatalogDeltaKey(delta.Version)) {
		return nil, errors.Join(ErrInvalidGuardedBootstrapInner, err)
	}
	encoded, err := distribution.EncodeCatalogDelta(delta)
	return encoded, errors.WithStack(err)
}

func guardedCatalogJobValue(key, value []byte) ([]byte, error) {
	job, err := distribution.DecodeSplitJob(value)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if !bytes.Equal(key, distribution.CatalogSplitJobKey(job.JobID)) &&
		!bytes.Equal(key, distribution.CatalogSplitJobHistoryKey(job.TerminalAtMs, job.JobID)) {
		return nil, errors.WithStack(ErrInvalidGuardedBootstrapInner)
	}
	encoded, err := distribution.EncodeSplitJob(job)
	return encoded, errors.WithStack(err)
}

func guardedCatalogOwnershipValue(key, value []byte) ([]byte, error) {
	manifest, err := distribution.DecodeOwnershipPartitionManifest(value)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	hash, err := distribution.OwnershipPartitionDescriptorHash(manifest.Parent)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	expected, err := distribution.CatalogOwnershipPartitionKey(manifest.Parent.RouteID, hash)
	if err != nil || !bytes.Equal(key, expected) {
		return nil, errors.Join(ErrInvalidGuardedBootstrapInner, err)
	}
	encoded, err := distribution.EncodeOwnershipPartitionManifest(manifest)
	return encoded, errors.WithStack(err)
}

func guardedCatalogDeletableKey(key []byte) bool {
	if id, ok := distribution.CatalogRouteIDFromKey(key); ok {
		return id != 0
	}
	if _, ok := distribution.CatalogDeltaVersionFromKey(key); ok {
		return true
	}
	if id, ok := distribution.CatalogSplitJobIDFromKey(key); ok {
		return id != 0
	}
	_, id, ok := distribution.CatalogSplitJobHistoryKeyParts(key)
	return ok && id != 0
}
