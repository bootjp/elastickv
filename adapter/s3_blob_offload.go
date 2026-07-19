package adapter

import (
	"context"
	"log/slog"
	"os"
	"strconv"
	"strings"
)

const (
	// S3BlobOffloadCapabilityName is the Admin GetClusterOverview capability
	// key used by mixed-version PUT admission before writing chunkref metadata.
	S3BlobOffloadCapabilityName = "feature_s3_blob_offload"

	s3BlobOffloadEnvVar = "ELASTICKV_S3_BLOB_OFFLOAD"

	s3BlobOffloadModeLegacy  = "legacy"
	s3BlobOffloadModeOffload = "offload"

	s3BlobOffloadReasonFlagDisabled      = "flag_disabled"
	s3BlobOffloadReasonCapabilityMissing = "capability_missing"
	s3BlobOffloadReasonDataPathDisabled  = "data_path_disabled"
	s3BlobOffloadReasonEnabled           = "enabled"
)

// S3BlobOffloadCapabilityChecker is the cluster-wide mixed-version guard for
// admitting an S3 PUT into the offloaded chunkref/chunkblob keyspace.
type S3BlobOffloadCapabilityChecker interface {
	AllPeersSupportS3BlobOffload(ctx context.Context) bool
}

// S3BlobOffloadObserver records offload rollout and chunkblob durability
// outcomes. The data path is still fail-closed, so only decision counters are
// emitted today; the remaining counters are wired for the durable replication
// PR that will turn the offload path on.
type S3BlobOffloadObserver interface {
	ObserveS3BlobOffloadDecision(mode, reason string)
	ObserveS3ChunkBlobReplicationDegraded()
	ObserveS3ChunkBlobSHAMismatch()
	ObserveS3ChunkBlobUnrecoverable()
}

type s3BlobOffloadDecision struct {
	mode   string
	reason string
}

// S3BlobOffloadLocalCapability reports whether this binary can safely serve the
// full offloaded S3 chunkref/chunkblob data path. The scaffolding PR keeps this
// false so future leaders do not mistake these nodes for offload readers.
func S3BlobOffloadLocalCapability() bool {
	return false
}

func WithS3BlobOffloadEnabled(enabled bool) S3ServerOption {
	return func(server *S3Server) {
		if server == nil {
			return
		}
		server.blobOffloadEnabled = enabled
	}
}

func WithS3BlobOffloadCapabilityChecker(checker S3BlobOffloadCapabilityChecker) S3ServerOption {
	return func(server *S3Server) {
		if server == nil {
			return
		}
		server.blobOffloadChecker = checker
	}
}

func WithS3BlobOffloadObserver(observer S3BlobOffloadObserver) S3ServerOption {
	return func(server *S3Server) {
		if server == nil {
			return
		}
		server.blobOffloadObserver = observer
	}
}

func newS3BlobOffloadEnabledFromEnv() bool {
	raw, ok := os.LookupEnv(s3BlobOffloadEnvVar)
	if !ok {
		return false
	}
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return false
	}
	enabled, err := strconv.ParseBool(raw)
	if err != nil {
		slog.Warn("invalid S3 blob offload boolean env; using default", "name", s3BlobOffloadEnvVar, "value", raw, "default", false)
		return false
	}
	return enabled
}

func (s *S3Server) s3BlobOffloadDecision(ctx context.Context) s3BlobOffloadDecision {
	if s == nil || !s.blobOffloadEnabled {
		return s3BlobOffloadDecision{mode: s3BlobOffloadModeLegacy, reason: s3BlobOffloadReasonFlagDisabled}
	}
	if s.blobOffloadChecker == nil || !s.blobOffloadChecker.AllPeersSupportS3BlobOffload(ctx) {
		return s3BlobOffloadDecision{mode: s3BlobOffloadModeLegacy, reason: s3BlobOffloadReasonCapabilityMissing}
	}
	if !S3BlobOffloadLocalCapability() {
		return s3BlobOffloadDecision{mode: s3BlobOffloadModeLegacy, reason: s3BlobOffloadReasonDataPathDisabled}
	}
	return s3BlobOffloadDecision{mode: s3BlobOffloadModeOffload, reason: s3BlobOffloadReasonEnabled}
}

func (s *S3Server) observeS3BlobOffloadDecision(ctx context.Context) {
	decision := s.s3BlobOffloadDecision(ctx)
	if s != nil && s.blobOffloadObserver != nil {
		s.blobOffloadObserver.ObserveS3BlobOffloadDecision(decision.mode, decision.reason)
	}
}
