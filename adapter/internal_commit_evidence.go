package adapter

import (
	"bytes"
	"context"
	"time"

	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/cockroachdb/errors"
)

const legacyCommitEvidenceTimeout = 5 * time.Second

func (i *Internal) verifyLegacyCommitEvidence(ctx context.Context, reqs []*pb.Request, startTS, commitTS uint64) error {
	return i.verifyCommitEvidence(ctx, reqs, startTS, commitTS, false)
}

func (i *Internal) verifyCommitEvidence(ctx context.Context, reqs []*pb.Request, startTS, commitTS uint64, pending bool) error {
	ctx, cancel := context.WithTimeout(ctx, legacyCommitEvidenceTimeout)
	defer cancel()
	keys, err := collectLegacyPrimaryKeys(ctx, reqs)
	if err != nil {
		return err
	}
	if len(keys) == 0 {
		return nil
	}
	if i.primaryCommitEvidence == nil {
		return errors.Wrap(kv.ErrTSOProtocolUnsupported, "primary commit evidence reader is required")
	}
	results, err := i.readLegacyPrimaryEvidence(ctx, keys, startTS)
	if err != nil {
		return err
	}
	evidence := make(map[string]kv.PrimaryCommitEvidence, len(keys))
	for index, key := range keys {
		evidence[string(key)] = results[index]
	}
	return classifyLegacyRequests(ctx, reqs, evidence, commitTS, pending)
}

func legacyCommitMeta(req *pb.Request) (kv.TxnMeta, bool, error) {
	if req == nil || req.Phase != pb.Phase_COMMIT {
		return kv.TxnMeta{}, false, nil
	}
	mutation, ok := forwardedTxnMetaMutation(req, []byte(kv.TxnMetaPrefix))
	if !ok {
		return kv.TxnMeta{}, false, nil
	}
	meta, err := kv.DecodeTxnMeta(mutation.Value)
	return meta, true, errors.WithStack(err)
}

func collectLegacyPrimaryKeys(ctx context.Context, reqs []*pb.Request) ([][]byte, error) {
	seen := make(map[string]struct{})
	var keys [][]byte
	for _, req := range reqs {
		if err := ctx.Err(); err != nil {
			return nil, errors.WithStack(err)
		}
		meta, ok, err := legacyCommitMeta(req)
		if err != nil {
			return nil, err
		}
		if !ok {
			continue
		}
		key := string(meta.PrimaryKey)
		if _, exists := seen[key]; exists {
			continue
		}
		if len(keys) == kv.MaxPrimaryCommitEvidenceKeys {
			return nil, errors.Wrap(kv.ErrInvalidRequest, "too many legacy primary evidence keys")
		}
		seen[key] = struct{}{}
		keys = append(keys, meta.PrimaryKey)
	}
	return keys, nil
}

func (i *Internal) readLegacyPrimaryEvidence(ctx context.Context, keys [][]byte, startTS uint64) ([]kv.PrimaryCommitEvidence, error) {
	if reader, ok := i.primaryCommitEvidence.(kv.PrimaryCommitEvidenceBatchReader); ok {
		results, err := reader.PrimaryCommitEvidenceBatch(ctx, keys, startTS)
		if err != nil {
			return nil, errors.Wrap(err, "validate legacy primary commit evidence")
		}
		if len(results) != len(keys) {
			return nil, errors.Wrap(kv.ErrInvalidRequest, "invalid primary evidence batch result")
		}
		return results, nil
	}
	results := make([]kv.PrimaryCommitEvidence, len(keys))
	for index, key := range keys {
		if err := ctx.Err(); err != nil {
			return nil, errors.WithStack(err)
		}
		evidence, err := i.primaryCommitEvidence.PrimaryCommitEvidence(ctx, key, startTS)
		if err != nil {
			return nil, errors.Wrap(err, "validate legacy primary commit evidence")
		}
		results[index] = evidence
	}
	return results, nil
}

func classifyLegacyRequests(ctx context.Context, reqs []*pb.Request, evidence map[string]kv.PrimaryCommitEvidence, commitTS uint64, pending bool) error {
	for _, req := range reqs {
		if err := ctx.Err(); err != nil {
			return errors.WithStack(err)
		}
		meta, ok, err := legacyCommitMeta(req)
		if err != nil {
			return err
		}
		if !ok {
			continue
		}
		proof := evidence[string(meta.PrimaryKey)]
		if forwardedTxnHasPreparedPrimary(req, meta.PrimaryKey, commitTS, proof, pending) {
			continue
		}
		if err := kv.ClassifyPrePhaseDCommit(proof, commitTS, false); err != nil {
			return errors.Wrap(err, "validate legacy primary commit decision")
		}
	}
	return nil
}

func forwardedTxnHasPreparedPrimary(req *pb.Request, primaryKey []byte, commitTS uint64, evidence kv.PrimaryCommitEvidence, pending bool) bool {
	if !evidence.Readable || evidence.Found || !evidence.PrimaryPrepared {
		return false
	}
	if !preparedPrimaryTimestampMatches(evidence, commitTS, pending) {
		return false
	}
	for _, mutation := range req.Mutations[1:] {
		if mutation != nil && bytes.Equal(mutation.Key, primaryKey) {
			return true
		}
	}
	return false
}

func preparedPrimaryTimestampMatches(evidence kv.PrimaryCommitEvidence, commitTS uint64, pending bool) bool {
	if evidence.PreparedCommitTS == commitTS {
		return true
	}
	return evidence.PreparedCommitTS == 0 && (pending || evidence.LegacyPreparedAllowed)
}
