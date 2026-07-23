package adapter

import (
	"strings"

	"github.com/bootjp/elastickv/kv"
	"github.com/cockroachdb/errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func isRouteWriteFencedError(err error) bool {
	if errors.Is(err, kv.ErrRouteWriteFenced) {
		return true
	}
	st, ok := grpcStatusFromError(err)
	if !ok {
		return false
	}
	code := st.Code()
	if code != codes.Unknown && code != codes.Aborted && code != codes.FailedPrecondition {
		return false
	}
	return strings.HasSuffix(st.Message(), kv.ErrRouteWriteFenced.Error())
}

func grpcStatusFromError(err error) (*status.Status, bool) {
	type grpcStatusCarrier interface {
		GRPCStatus() *status.Status
	}
	var carrier grpcStatusCarrier
	if !errors.As(err, &carrier) {
		return nil, false
	}
	st := carrier.GRPCStatus()
	return st, st != nil
}
