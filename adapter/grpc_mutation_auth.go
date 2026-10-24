package adapter

import (
	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/kv"
	"github.com/cockroachdb/errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func validatePublicMutationKey(key []byte) error {
	if distribution.IsReservedControlKey(key) || kv.IsTxnInternalKey(key) {
		return errors.WithStack(status.Error(codes.PermissionDenied, "public mutations cannot write internal control keys"))
	}
	return nil
}
