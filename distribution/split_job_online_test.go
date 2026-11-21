package distribution

import (
	"testing"

	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

func TestOnlineSplitJobCodecBindsIdentityAndSealedPosition(t *testing.T) {
	t.Parallel()
	job := sampleSplitJob(9)
	job.SourceGroupID, job.TargetGroupID, job.SplitKey = 1, 2, []byte("m")
	source := store.OnlineMigrationSession{Version: 1, Identity: store.MigrationCaptureIdentity{SourceGroupID: 1, JobID: 9, Generation: 1, Index: 10, Lineage: store.OnlineMigrationLineage("owner")}, StartIndex: 10, LastIndex: 12, ConfigurationIndex: 3, RouteStart: []byte("m"), RouteEnd: []byte("z"), Sequence: 2, Sealed: true}
	manifest, err := store.EncodeOnlineMigrationManifest(source)
	require.NoError(t, err)
	job.OnlineCaptureVersion, job.OnlineSequence, job.OnlineSealSequence, job.OnlineManifest = 1, 2, 2, manifest
	require.NoError(t, validateOnlineSplitJob(job))
	for _, change := range []func(*SplitJob){
		func(j *SplitJob) { j.OnlineCaptureVersion = 2 },
		func(j *SplitJob) { j.OnlineCaptureVersion = 0 },
		func(j *SplitJob) { j.SourceGroupID = 3 },
		func(j *SplitJob) { j.JobID = 10 },
		func(j *SplitJob) { j.SplitKey = []byte("n") },
		func(j *SplitJob) { j.OnlineSequence = 3 },
		func(j *SplitJob) { j.OnlineSealSequence = 1 },
		func(j *SplitJob) { j.OnlineManifest = []byte("bad") },
	} {
		changed := CloneSplitJob(job)
		change(&changed)
		require.ErrorIs(t, validateOnlineSplitJob(changed), ErrCatalogInvalidSplitJobRecord)
	}
}
