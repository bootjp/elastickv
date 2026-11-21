package distribution

import (
	"bytes"

	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
)

func validateOnlineSplitJob(job SplitJob) error {
	if job.OnlineCaptureVersion == 0 {
		return validateLegacySplitJobOnlineFields(job)
	}
	if job.OnlineCaptureVersion != store.OnlineMigrationVersion {
		return errors.WithStack(ErrCatalogInvalidSplitJobRecord)
	}
	source, err := store.DecodeOnlineMigrationManifest(job.OnlineManifest)
	if err != nil {
		return errors.Wrap(ErrCatalogInvalidSplitJobRecord, "invalid online source manifest")
	}
	if !onlineSplitJobSourceMatches(job, source) {
		return errors.WithStack(ErrCatalogInvalidSplitJobRecord)
	}
	return validateOnlineSplitJobPosition(job, source)
}

func validateLegacySplitJobOnlineFields(job SplitJob) error {
	if job.OnlineSequence != 0 || job.OnlineSealSequence != 0 || len(job.OnlineManifest) != 0 {
		return errors.WithStack(ErrCatalogInvalidSplitJobRecord)
	}
	return nil
}

func validateOnlineSplitJobPosition(job SplitJob, source store.OnlineMigrationSession) error {
	if source.Sealed {
		if job.OnlineSealSequence != source.Sequence || job.OnlineSequence > source.Sequence {
			return errors.WithStack(ErrCatalogInvalidSplitJobRecord)
		}
	} else if job.OnlineSealSequence != 0 {
		return errors.WithStack(ErrCatalogInvalidSplitJobRecord)
	}
	return nil
}

func onlineSplitJobSourceMatches(job SplitJob, source store.OnlineMigrationSession) bool {
	return source.Identity.JobID == job.JobID && source.Identity.SourceGroupID == job.SourceGroupID &&
		source.Identity.SourceGroupID != job.TargetGroupID && bytes.Equal(source.RouteStart, job.SplitKey) && !source.Closed && source.Invalid == ""
}
