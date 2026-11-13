package nodeauth

import (
	"github.com/bootjp/elastickv/adapter"
	internalutil "github.com/bootjp/elastickv/internal"
	"github.com/cockroachdb/errors"
	"google.golang.org/grpc"
)

const AdminTokenMaxBytes = 4 << 10

// AdminGRPCInterceptors bundles the unary+stream interceptors that enforce the
// Admin bearer token. Returning the raw interceptor functions (rather than
// pre-wrapped grpc.ServerOption values via grpc.ChainUnaryInterceptor) lets
// the registration site combine them with any other interceptors in a single
// ChainUnaryInterceptor call, so using grpc.UnaryInterceptor alongside risks
// silent overwrites (gRPC-Go: last option of the same type wins).
type AdminGRPCInterceptors struct {
	Unary              []grpc.UnaryServerInterceptor
	Stream             []grpc.StreamServerInterceptor
	Token              string
	MigrationToken     string
	S3BlobFetchEnabled bool
	// MigrationReady is the split-migration readiness gate the raft gRPC
	// servers apply to migration RPCs and to ownership readiness.
	MigrationReady adapter.MigrationReadinessGate
}

func (a AdminGRPCInterceptors) Empty() bool {
	return len(a.Unary) == 0 && len(a.Stream) == 0
}

// LoadAdminTokenFile materialises --adminTokenFile with a strict upper bound
// so a misconfigured path (for example a log file) cannot force an arbitrary
// allocation before the bearer-token check. Delegates to the shared helper in
// internal/ so the admin binary and the node process read tokens identically.
func LoadAdminTokenFile(path string) (string, error) {
	tok, err := internalutil.LoadBearerTokenFile(path, AdminTokenMaxBytes, "admin token")
	if err != nil {
		return "", errors.Wrap(err, "load admin token")
	}
	return tok, nil
}

func LoadS3BlobPeerTokenFile(path string) (string, error) {
	tok, err := internalutil.LoadBearerTokenFile(path, AdminTokenMaxBytes, "S3 blob peer token")
	if err != nil {
		return "", errors.Wrap(err, "load S3 blob peer token")
	}
	return tok, nil
}
