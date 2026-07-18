package adapter

import (
	"context"
	"log/slog"

	"github.com/bootjp/elastickv/internal/s3keys"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const s3BlobVoterSuffrage = "voter"

type s3BlobReplicationResult struct {
	replica S3BlobReplica
	err     error
	local   bool
}

type s3BlobReplicationPlan struct {
	localStore    store.MVCCStore
	replicas      []S3BlobReplica
	selfNodeID    string
	durableTarget int
	voterTarget   int
	degraded      bool
}

type s3BlobReplicationSummary struct {
	durable      int
	voterDurable int
	localDurable bool
	replicas     []S3BlobReplica
}

func (s *S3Server) persistS3ChunkBlob(
	ctx context.Context,
	digest [s3ChunkBlobSHA256Bytes]byte,
	payload []byte,
	commitTS uint64,
) (s3keys.ChunkRefValue, error) {
	plan, err := s.prepareS3BlobReplication(ctx, digest)
	if err != nil {
		return s3keys.ChunkRefValue{}, err
	}
	replicationCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	results := s.startS3BlobReplication(replicationCtx, plan, digest, payload, commitTS)
	summary := s.collectS3BlobReplication(
		ctx, results, len(plan.replicas), plan.durableTarget, plan.voterTarget,
	)
	if !summary.localDurable || summary.durable < plan.durableTarget || summary.voterDurable < plan.voterTarget {
		return s3keys.ChunkRefValue{}, s3BlobUnavailablef(
			"s3 chunkblob reached %d durable replicas (%d voters); require %d including local and %d voters",
			summary.durable,
			summary.voterDurable,
			plan.durableTarget,
			plan.voterTarget,
		)
	}
	if plan.degraded && s.blobOffloadObserver != nil {
		s.blobOffloadObserver.ObserveS3ChunkBlobReplicationDegraded()
	}
	return s3keys.ChunkRefValue{
		ContentSHA256: digest,
		Size:          uint64(len(payload)), //nolint:gosec // Payload is bounded by s3ChunkSize.
		SourcePeer:    plan.selfNodeID,
		ReplicaPeers:  s3ChunkRefPeers(summary.replicas),
	}, nil
}

func s3ChunkRefPeers(replicas []S3BlobReplica) []s3keys.ChunkRefPeer {
	peers := make([]s3keys.ChunkRefPeer, 0, len(replicas))
	for _, replica := range replicas {
		peers = append(peers, s3keys.ChunkRefPeer{NodeID: replica.NodeID, Address: replica.Address})
	}
	return peers
}

func (s *S3Server) prepareS3BlobReplication(
	ctx context.Context,
	digest [s3ChunkBlobSHA256Bytes]byte,
) (s3BlobReplicationPlan, error) {
	if s == nil || s.blobCluster == nil || s.blobLocalStores == nil {
		return s3BlobReplicationPlan{}, s3BlobUnavailable("s3 blob offload data path is not configured")
	}
	chunkKey := s3keys.ChunkBlobKey(digest)
	localStore, ok := s.blobLocalStores.LocalStoreForKey(chunkKey)
	if !ok || localStore == nil {
		return s3BlobReplicationPlan{}, s3BlobUnavailable("s3 chunkblob local store is unavailable")
	}
	replicas, err := s.blobCluster.ReplicasForChunk(ctx, chunkKey)
	if err != nil {
		return s3BlobReplicationPlan{}, s3BlobUnavailablef("resolve s3 chunkblob replicas: %v", err)
	}
	selfNodeID := s.blobCluster.SelfNodeID()
	durableTarget, voterTarget, degraded, err := s.s3BlobDurableTarget(replicas, selfNodeID)
	if err != nil {
		return s3BlobReplicationPlan{}, err
	}
	return s3BlobReplicationPlan{
		localStore:    localStore,
		replicas:      replicas,
		selfNodeID:    selfNodeID,
		durableTarget: durableTarget,
		voterTarget:   voterTarget,
		degraded:      degraded,
	}, nil
}

func (s *S3Server) startS3BlobReplication(
	ctx context.Context,
	plan s3BlobReplicationPlan,
	digest [s3ChunkBlobSHA256Bytes]byte,
	payload []byte,
	commitTS uint64,
) <-chan s3BlobReplicationResult {
	results := make(chan s3BlobReplicationResult, len(plan.replicas))
	for _, replica := range plan.replicas {
		if replica.NodeID == plan.selfNodeID {
			go func() {
				fetchServer := NewS3BlobFetchServer(plan.localStore, s.blobOffloadObserver, WithS3BlobFetchClock(s.clock()))
				writeErr := fetchServer.storeChunkBlob(ctx, digest, payload, commitTS)
				results <- s3BlobReplicationResult{replica: replica, err: writeErr, local: true}
			}()
			continue
		}
		go func() {
			pushErr := s.blobCluster.PushChunkBlob(ctx, replica, digest, payload, commitTS)
			results <- s3BlobReplicationResult{replica: replica, err: pushErr}
		}()
	}
	return results
}

func (s *S3Server) collectS3BlobReplication(
	ctx context.Context,
	results <-chan s3BlobReplicationResult,
	resultCount int,
	durableTarget int,
	voterTarget int,
) s3BlobReplicationSummary {
	summary := s3BlobReplicationSummary{replicas: make([]S3BlobReplica, 0, durableTarget)}
	for range resultCount {
		result := <-results
		if result.err != nil {
			slog.WarnContext(ctx, "s3 chunkblob replica write failed",
				"node_id", result.replica.NodeID,
				"address", result.replica.Address,
				"local", result.local,
				"err", result.err,
			)
			continue
		}
		summary.durable++
		if result.replica.Suffrage == s3BlobVoterSuffrage {
			summary.voterDurable++
		}
		summary.localDurable = summary.localDurable || result.local
		summary.replicas = append(summary.replicas, result.replica)
		if summary.localDurable && summary.durable >= durableTarget && summary.voterDurable >= voterTarget {
			return summary
		}
	}
	return summary
}

func (s *S3Server) s3BlobDurableTarget(replicas []S3BlobReplica, selfNodeID string) (target int, voterTarget int, degraded bool, err error) {
	voters, err := validateS3BlobMembership(replicas, selfNodeID)
	if err != nil {
		return 0, 0, false, err
	}
	quorum := voters/s3BlobQuorumDivisor + 1
	voterTarget = quorum
	target = quorum
	if s != nil && s.blobMinReplicas > target {
		target = s.blobMinReplicas
	}
	if target < s3BlobMinimumDurableCopies {
		return 0, 0, false, errors.New("s3 chunkblob durable target is below two")
	}
	if target <= len(replicas) {
		return target, voterTarget, false, nil
	}
	// Membership has already shrunk below a previously configured target.
	// Use the current Raft quorum and emit the degradation signal. A mere
	// unreachable peer does not reduce len(replicas), so explicit full
	// replication still fails closed during transient outages.
	return quorum, voterTarget, true, nil
}

func validateS3BlobMembership(replicas []S3BlobReplica, selfNodeID string) (int, error) {
	if len(replicas) < s3BlobMinimumDurableCopies {
		return 0, s3BlobUnavailable("s3 chunkblob replication requires at least two members")
	}
	selfFound := false
	selfVoter := false
	voters := 0
	for _, replica := range replicas {
		if replica.NodeID == selfNodeID {
			selfFound = true
			selfVoter = replica.Suffrage == s3BlobVoterSuffrage
		}
		switch replica.Suffrage {
		case s3BlobVoterSuffrage:
			voters++
		case "learner":
		default:
			return 0, s3BlobUnavailablef("s3 chunkblob member %q has unknown suffrage %q", replica.NodeID, replica.Suffrage)
		}
	}
	if !selfFound {
		return 0, s3BlobUnavailable("s3 chunkblob membership does not include this node")
	}
	if !selfVoter {
		return 0, s3BlobUnavailable("s3 chunkblob local member is not a voter")
	}
	if voters < s3BlobMinimumDurableCopies {
		return 0, s3BlobUnavailable("s3 chunkblob replication requires at least two voters")
	}
	return voters, nil
}

func s3BlobUnavailable(message string) error {
	return errors.WithStack(status.Error(codes.Unavailable, message))
}

func s3BlobUnavailablef(format string, args ...any) error {
	return errors.WithStack(status.Errorf(codes.Unavailable, format, args...))
}
