package adapter

import (
	"context"
	"log/slog"
	"os"
	"strconv"
	"strings"

	"github.com/cockroachdb/errors"
)

const (
	// S3BlobOffloadCapabilityName is the Admin GetClusterOverview capability
	// key used by mixed-version PUT admission before writing chunkref metadata.
	S3BlobOffloadCapabilityName = "feature_s3_blob_offload"

	s3BlobOffloadEnvVar     = "ELASTICKV_S3_BLOB_OFFLOAD"
	s3BlobMinReplicasEnvVar = "ELASTICKV_S3_CHUNKBLOB_MIN_REPLICAS"

	s3BlobOffloadModeLegacy  = "legacy"
	s3BlobOffloadModeOffload = "offload"

	s3BlobMinimumDurableCopies = 2

	s3BlobOffloadReasonFlagDisabled      = "flag_disabled"
	s3BlobOffloadReasonCapabilityMissing = "capability_missing"
	s3BlobOffloadReasonDataPathDisabled  = "data_path_disabled"
	s3BlobOffloadReasonGCNotReady        = "gc_not_ready"
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

// S3BlobOffloadLocalCapability reports whether this binary can serve the M1
// offloaded PUT/GET path, including durable peer push and proxy-on-miss reads.
func S3BlobOffloadLocalCapability() bool {
	return true
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

func WithS3BlobCluster(cluster S3BlobCluster) S3ServerOption {
	return func(server *S3Server) {
		if server == nil {
			return
		}
		server.blobCluster = cluster
		server.blobOffloadChecker = cluster
	}
}

func WithS3BlobLocalStoreResolver(resolver S3BlobLocalStoreResolver) S3ServerOption {
	return func(server *S3Server) {
		if server == nil {
			return
		}
		server.blobLocalStores = resolver
	}
}

func WithS3BlobMinReplicas(minReplicas int) S3ServerOption {
	return func(server *S3Server) {
		if server == nil {
			return
		}
		server.blobMinReplicas = minReplicas
	}
}

// S3BlobMinReplicasFromEnv returns zero when the dynamic Raft-quorum default
// should be used. Explicit values below two are rejected because a leader-only
// chunkblob is weaker than the legacy Raft data path.
func S3BlobMinReplicasFromEnv() (int, error) {
	raw := strings.TrimSpace(os.Getenv(s3BlobMinReplicasEnvVar))
	if raw == "" {
		return 0, nil
	}
	minReplicas, err := strconv.Atoi(raw)
	if err != nil {
		return 0, errors.Wrap(err, "parse S3 chunkblob minimum replicas")
	}
	if minReplicas < s3BlobMinimumDurableCopies {
		return 0, errors.New("S3 chunkblob minimum replicas must be at least 2")
	}
	return minReplicas, nil
}

// S3BlobOffloadEnabledFromEnv returns the local rollout flag used by both the
// S3 write path and the Admin capability advertisement.
func S3BlobOffloadEnabledFromEnv() bool {
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
	// M1 has a complete PUT/GET path, but content-addressed chunkblobs cannot
	// be enabled operationally until M3 installs reference counting, the grace
	// queue, and the orphan scanner. M3 sets this readiness only after those
	// workers are wired into the server lifecycle.
	if !s.blobOffloadGCReady {
		return s3BlobOffloadDecision{mode: s3BlobOffloadModeLegacy, reason: s3BlobOffloadReasonGCNotReady}
	}
	return s3BlobOffloadDecision{mode: s3BlobOffloadModeOffload, reason: s3BlobOffloadReasonEnabled}
}

func (s *S3Server) observeS3BlobOffloadDecision(ctx context.Context) s3BlobOffloadDecision {
	decision := s.s3BlobOffloadDecision(ctx)
	if s != nil && s.blobOffloadObserver != nil {
		s.blobOffloadObserver.ObserveS3BlobOffloadDecision(decision.mode, decision.reason)
	}
	return decision
}
