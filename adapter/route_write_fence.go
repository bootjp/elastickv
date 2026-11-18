package adapter

import (
	"github.com/bootjp/elastickv/kv"
	"github.com/cockroachdb/errors"
)

// Only a local sentinel or a validated Forward rejection can prove a fence.
func isRouteWriteFencedError(err error) bool {
	if errors.Is(err, kv.ErrTransactionOutcomeUnknown) || isIndeterminateMutationDispatchFailure(err) {
		return false
	}
	return errors.Is(err, kv.ErrRouteWriteFenced)
}
