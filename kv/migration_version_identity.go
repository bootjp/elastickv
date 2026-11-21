package kv

import (
	"bytes"
	"context"
	"slices"

	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/internal/raftengine"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
)

var migrationVersionIdentityKey = []byte("migration-version-identity")
var migrationVersionIdentityValue = []byte{1}

// EncodeMigrationVersionIdentityActivation returns the Raft frame that makes
// future migration imports, promotions and cleanup preserve CAS version identity.
func EncodeMigrationVersionIdentityActivation() []byte {
	return []byte(raftengine.MigrationVersionIdentityFrame)
}

// IsMigrationVersionIdentityActivation recognizes the replicated marker frame.
func IsMigrationVersionIdentityActivation(data []byte) bool {
	return bytes.HasPrefix(data, []byte(raftengine.MigrationVersionIdentityFrame))
}

// MigrationVersionIdentityEnabled reports whether the store has applied the
// replicated migration version identity marker.
func MigrationVersionIdentityEnabled(ctx context.Context, st store.MVCCStore) (bool, error) {
	metadata, ok := st.(store.RaftControlMetadataStore)
	if !ok {
		return false, nil
	}
	value, err := metadata.ReadRaftControlMetadata(ctx, migrationVersionIdentityKey)
	if errors.Is(err, store.ErrKeyNotFound) {
		return false, nil
	}
	if err != nil {
		return false, errors.WithStack(err)
	}
	if !bytes.Equal(value, migrationVersionIdentityValue) {
		return false, errors.New("invalid migration version identity marker")
	}
	return true, nil
}

func (f *kvFSM) applyMigrationVersionIdentityActivation(ctx context.Context, data []byte) any {
	if !bytes.Equal(data, []byte(raftengine.MigrationVersionIdentityFrame)) {
		return haltErr(errors.Mark(errors.New("invalid migration version identity frame"), ErrRaftEntryApply))
	}
	err := f.persistMigrationVersionIdentity(ctx)
	if err != nil {
		return haltErr(errors.Wrap(errors.Mark(err, ErrRaftEntryApply), "kv/fsm: activate migration version identity"))
	}
	return nil
}

func (f *kvFSM) persistMigrationVersionIdentity(ctx context.Context) error {
	applier, ok := f.store.(store.RaftEntryApplier)
	if !ok || f.pendingApplyIdx == 0 {
		return errors.WithStack(store.ErrRaftControlScopeRequired)
	}
	metadata, ok := f.store.(store.RaftControlMetadataStore)
	if !ok {
		return errors.WithStack(store.ErrNotSupported)
	}
	err := applier.ApplyRaftEntry(ctx, f.pendingApplyIdx, func(ctx context.Context) error {
		replayed, err := store.StageRaftEntryProgress(ctx, f.pendingApplyIdx)
		if err != nil || replayed {
			return errors.WithStack(err)
		}
		enabled, err := MigrationVersionIdentityEnabled(ctx, f.store)
		if err != nil || enabled {
			return err
		}
		return errors.WithStack(metadata.StageRaftControlMetadata(ctx, migrationVersionIdentityKey, migrationVersionIdentityValue))
	})
	return errors.WithStack(err)
}

// Decide before scanning MVCC rows. A replica may have pruned a different set
// of historical rows, and a cursor or page budget can even hide a head behind
// that history. The entire effective direct-key range needs lasting coverage.
func verifyMigrationVersionCleanupCoverage(ctx context.Context, st store.MVCCStore, req *pb.CleanupMigrationRequest) error {
	start, end := req.GetRangeStart(), req.GetRangeEnd()
	if cleanupRangeCannotIssueVersions(start, end) {
		return nil
	}
	// On CAS-eligible keys routeKey is the identity. Special family filters
	// have their own semantics, so only the ordinary route filter narrows this
	// bound. Normal special-family scans are exempt by their physical prefix.
	if req.GetRequiresRouteKeyCheck() && !req.GetRequiresDecodedS3() && req.GetKeyFamily() != distribution.MigrationFamilyLegacyListMetaDelta {
		start, end = prefixDeleteRouteIntersection(distribution.Route{Start: req.GetRouteStart(), End: req.GetRouteEnd()}, start, end)
	}
	if len(end) != 0 && bytes.Compare(start, end) >= 0 {
		return nil
	}
	view, err := readMigrationServingState(ctx, st)
	if err != nil {
		return errors.WithStack(err)
	}
	if !migrationVersionCleanupCovered(view, start, end) {
		return errors.Wrap(store.ErrMigrationOwnership, "version cleanup requires durable CAS range coverage")
	}
	return nil
}

func cleanupRangeCannotIssueVersions(start, end []byte) bool {
	if len(end) == 0 {
		return false
	}
	prefixes := append(distribution.MigrationKnownInternalPrefixes(), distribution.MigrationStagedDataScanPrefix())
	for _, prefix := range prefixes {
		if bytes.HasPrefix(start, prefix) && bytes.Compare(end, store.PrefixScanEnd(prefix)) <= 0 {
			return true
		}
	}
	return false
}

func migrationVersionCleanupCovered(view store.MigrationServingState, start, end []byte) bool {
	// CAS excludes both kinds of ownership evidence. Per-job retirement
	// preserves the ledger, and frontier transitions preserve or expand its
	// interval coverage. Readiness alone can disappear when a job retires.
	proofs := migrationAdmissionProofs(view)
	slices.SortFunc(proofs, func(a, b migrationAdmissionProof) int { return bytes.Compare(a.RouteStart, b.RouteStart) })
	for _, proof := range proofs {
		if bytes.Compare(proof.RouteStart, start) > 0 {
			break
		}
		if !proof.evidenceValid(view) {
			continue
		}
		if len(proof.RouteEnd) == 0 || (len(end) != 0 && bytes.Compare(proof.RouteEnd, end) >= 0) {
			return true
		}
		if bytes.Compare(proof.RouteEnd, start) > 0 {
			start = proof.RouteEnd
		}
	}
	return false
}
