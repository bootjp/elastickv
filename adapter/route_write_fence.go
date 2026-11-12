package adapter

import (
	"github.com/bootjp/elastickv/kv"
	"github.com/cockroachdb/errors"
)

func isRouteWriteFencedError(err error) bool {
	if errors.Is(err, kv.ErrTransactionOutcomeUnknown) {
		return false
	}
	return errors.Is(err, kv.ErrRouteWriteFenced)
}
