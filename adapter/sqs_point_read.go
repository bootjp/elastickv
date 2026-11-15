package adapter

import (
	"context"
	"net/http"

	"github.com/cockroachdb/errors"
)

// All partitioned record families, including locks and dedup entries, follow
// the same owner as sends and visibility scans. Catalog keys remain global.
func (s *SQSServer) getSQSAt(ctx context.Context, key []byte, readTS uint64) ([]byte, error) {
	if s.partitionResolver == nil || !s.partitionResolver.RecognisesPartitionedKey(key) {
		value, err := requestGetAt(ctx, s.store, key, readTS)
		return value, errors.WithStack(err)
	}
	groupID, ok := s.partitionResolver.ResolveGroup(key)
	if !ok || groupID == 0 {
		return nil, newSQSAPIError(http.StatusServiceUnavailable, sqsErrServiceUnavailable, "SQS partition owner is unavailable")
	}
	getter, ok := s.store.(sqsGroupGetter)
	if !ok {
		return nil, errors.New("SQS partition point read requires a group-aware store")
	}
	value, err := requestGetGroupAt(ctx, getter, groupID, key, readTS)
	return value, errors.WithStack(err)
}
