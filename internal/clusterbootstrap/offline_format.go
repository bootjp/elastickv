package clusterbootstrap

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"slices"

	"github.com/bootjp/elastickv/internal/raftbootstrap"
	"github.com/bootjp/elastickv/internal/raftengine/etcd"
	"github.com/cockroachdb/errors"
)

const bootstrapFormatStagingMode = 0o700

// Compose the exact data transformation with the downgrade barrier. Temporary
// publication files live in the migration directory, never in the source image
// whose immutable files must remain verifiable after an interrupted prepare.
func BootstrapOfflineFormatPreparation(data *etcd.BaselinePreparationCallbacks) *etcd.BaselinePreparationCallbacks {
	paths := slices.Clone(data.MutablePaths)
	paths = append(paths, raftbootstrap.IntentFile, raftbootstrap.MarkerFile)
	return &etcd.BaselinePreparationCallbacks{MutablePaths: paths,
		StagePreparation: func(ctx context.Context, c etcd.BaselinePreparationContext) error {
			if err := data.StagePreparation(ctx, c); err != nil {
				return errors.WithStack(err)
			}
			return stageBootstrapRequiredFormat(c)
		},
		VerifyPreparedStore: func(ctx context.Context, c etcd.BaselinePreparationContext) (etcd.BaselinePreparedStoreProof, error) {
			proof, err := data.VerifyPreparedStore(ctx, c)
			if err != nil {
				return proof, errors.WithStack(err)
			}
			return proof, verifyBootstrapRequiredFormat(c)
		}}
}

func bootstrapPreparedIntent(c etcd.BaselinePreparationContext) raftbootstrap.Intent {
	return raftbootstrap.Intent{Identity: c.Manifest.Identity, BaselineKind: raftbootstrap.Accepted,
		Baseline: c.Manifest.Configuration, Minimum: c.Manifest.Minimum}
}

func stageBootstrapRequiredFormat(c etcd.BaselinePreparationContext) error {
	dir := filepath.Join(filepath.Dir(c.BackupDir), "format-staging")
	if err := os.Mkdir(dir, bootstrapFormatStagingMode); err != nil && !os.IsExist(err) {
		return errors.WithStack(err)
	}
	files, err := raftbootstrap.NewStoreWithStagingDirectory(c.DataDir, dir)
	if err != nil {
		return errors.WithStack(err)
	}
	intent := bootstrapPreparedIntent(c)
	digest, err := raftbootstrap.ConfigurationDigest(intent.Baseline)
	if err != nil {
		return errors.WithStack(err)
	}
	return errors.WithStack(files.Publish(intent, raftbootstrap.Evidence{Identity: &intent.Identity,
		BaselineDigest: &digest, RaftState: raftbootstrap.Intact}))
}

func verifyBootstrapRequiredFormat(c etcd.BaselinePreparationContext) error {
	want := bootstrapPreparedIntent(c)
	digest, err := raftbootstrap.ConfigurationDigest(want.Baseline)
	if err != nil {
		return errors.WithStack(err)
	}
	inspection, err := raftbootstrap.NewStore(c.DataDir).Inspect(raftbootstrap.Evidence{Identity: &want.Identity,
		BaselineDigest: &digest, RaftState: raftbootstrap.Intact})
	if err != nil {
		return errors.WithStack(err)
	}
	if inspection.Marker != raftbootstrap.MarkerRequired || inspection.Class != raftbootstrap.Required || inspection.Intent == nil {
		return errors.WithStack(raftbootstrap.ErrInconsistent)
	}
	expected, err := raftbootstrap.MarshalIntent(want)
	if err != nil {
		return errors.WithStack(err)
	}
	actual, err := raftbootstrap.MarshalIntent(*inspection.Intent)
	if err != nil {
		return errors.WithStack(err)
	}
	if !bytes.Equal(actual, expected) {
		return errors.WithStack(raftbootstrap.ErrInconsistent)
	}
	return nil
}
