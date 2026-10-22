package adapter

import (
	"strings"

	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Forwarded OCC conflicts lose their Go error chain. Restore only the known
// wire envelope so item retries retain the existing exact-timestamp dedup path.
func normalizeDynamoWriteDispatchError(err error) error {
	if err == nil || errors.Is(err, store.ErrWriteConflict) {
		return err
	}
	type statusCarrier interface{ GRPCStatus() *status.Status }
	var carrier statusCarrier
	if !errors.As(err, &carrier) {
		return err
	}
	st := carrier.GRPCStatus()
	if st != nil && (st.Code() == codes.Unknown || st.Code() == codes.Aborted) &&
		strings.HasPrefix(st.Message(), "key: ") && strings.HasSuffix(st.Message(), ": "+store.ErrWriteConflict.Error()) {
		return errors.WithStack(errors.Mark(err, store.ErrWriteConflict))
	}
	return err
}
