package kv

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"

	"github.com/bootjp/elastickv/distribution"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
)

func onlineMigrationImportOptions(req *pb.ImportRangeVersionsRequest) (store.ImportVersionsOptions, error) {
	source, err := store.DecodeOnlineMigrationManifest(req.GetOnlineManifest())
	if err != nil {
		return store.ImportVersionsOptions{}, errors.WithStack(err)
	}
	if !onlineImportMatchesSource(req, source) {
		return store.ImportVersionsOptions{}, errors.WithStack(store.ErrOnlineMigrationIdentity)
	}
	frame, err := store.DecodeOnlineMigrationFrame(req.GetOnlineFrame(), source)
	if err != nil {
		return store.ImportVersionsOptions{}, errors.WithStack(err)
	}
	if frame.Sequence != req.GetBatchSeq() || len(req.GetCursor()) != onlineSequenceBytes || binary.BigEndian.Uint64(req.GetCursor()) != frame.Sequence {
		return store.ImportVersionsOptions{}, errors.WithStack(store.ErrOnlineMigrationIdentity)
	}
	versions, err := onlineImportVersions(source, frame.Entry.Effects)
	if err != nil {
		return store.ImportVersionsOptions{}, err
	}
	return store.ImportVersionsOptions{JobID: req.GetJobId(), BracketID: req.GetBracketId(), BatchSeq: req.GetBatchSeq(), Cursor: bytes.Clone(req.GetCursor()), Versions: versions, OnlineSequence: frame.Sequence, OnlineDigest: sha256.Sum256(req.GetOnlineFrame()), OnlineSourceIdentity: source.Identity, OnlineSourceIndex: frame.Entry.Identity.Index}, nil
}

func onlineImportMatchesSource(req *pb.ImportRangeVersionsRequest, source store.OnlineMigrationSession) bool {
	return source.Identity.JobID == req.GetJobId() && source.Invalid == "" && !source.Closed && len(req.GetVersions()) == 0 && req.GetBracketId() == store.OnlineMigrationBracketID
}

func onlineImportVersions(source store.OnlineMigrationSession, effects []store.MigrationCaptureEffect) ([]store.MVCCVersion, error) {
	versions := make([]store.MVCCVersion, 0, len(effects))
	for _, effect := range effects {
		if !onlineCapturePointKey(effect.Key) || !onlineCaptureKeyInRange(effect.Key, source) {
			return nil, errors.WithStack(store.ErrOnlineMigrationIdentity)
		}
		versions = append(versions, store.MVCCVersion{
			Key: distribution.MigrationStagedDataKey(source.Identity.JobID, effect.Key), Value: bytes.Clone(effect.Value),
			CommitTS: effect.CommitTS, Tombstone: effect.Tombstone, ExpireAt: effect.ExpireAt, KeyFamily: distribution.MigrationFamilyUser,
		})
	}
	return versions, nil
}

const onlineSequenceBytes = 8

func (f *kvFSM) verifyOnlineImportConfiguration(ctx context.Context, req *pb.ImportRangeVersionsRequest) error {
	target, err := store.ReadOnlineMigrationTarget(ctx, f.store, req.GetJobId())
	if errors.Is(err, store.ErrKeyNotFound) || errors.Is(err, store.ErrNotSupported) {
		return nil
	}
	if err != nil {
		return errors.WithStack(err)
	}
	if target.ConfigurationIndex != f.AppliedConfigurationIndex() {
		// Seal is immutable. A runner that lost the race with ConfChange
		// can bind the target again and resume the same sealed tail.
		manifest, e := store.DecodeOnlineMigrationManifest(req.GetOnlineManifest())
		if e == nil && manifest.Sealed && manifest.Identity == target.Source.Identity {
			return errors.WithStack(ErrMigrationConfigurationChanged)
		}
		return errors.WithStack(store.ErrOnlineMigrationIdentity)
	}
	return nil
}
