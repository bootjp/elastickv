package adapter

import (
	"crypto/sha256"
	"fmt"
	"strconv"

	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const migrationTransferErrorDomain = "elastickv.migration.transfer"
const migrationOversizedVersionReason = "OVERSIZED_MVCC_VERSION"

var errMigrationOversizedSourceVersion = errors.New("authenticated source version exceeds migration transfer budget")

func migrationOversizedVersionError(v *pb.MVCCVersion) error {
	digest := sha256.Sum256(v.GetKey())
	// Preserve the legacy runner's explicit-failure classification. Only the
	// new structured detail authorizes the new runner's compensation path.
	s := status.New(codes.FailedPrecondition, fmt.Sprintf("migration version exceeds transfer budget; key %.128q commit_ts=%d value_bytes=%d key_sha256=%x", v.GetKey(), v.GetCommitTs(), len(v.GetValue()), digest))
	withDetail, err := s.WithDetails(&errdetails.ErrorInfo{
		Domain: migrationTransferErrorDomain, Reason: migrationOversizedVersionReason,
		Metadata: map[string]string{
			"key_sha256": fmt.Sprintf("%x", digest), "commit_ts": strconv.FormatUint(v.GetCommitTs(), 10),
			"value_bytes": strconv.Itoa(len(v.GetValue())), "budget_bytes": strconv.Itoa(store.MigrationPagePayloadBudget),
		},
	})
	if err != nil {
		return errors.WithStack(s.Err())
	}
	return errors.WithStack(withDetail.Err())
}

// Only call at the authenticated source ExportRangeVersions boundary. Errors
// from target import, local I/O, transport limits or generic backpressure must
// never acquire the authority to cancel a migration.
func migrationSourceExportError(err error) error {
	if status.Code(err) != codes.FailedPrecondition {
		return err
	}
	for _, detail := range status.Convert(err).Details() {
		if info, ok := detail.(*errdetails.ErrorInfo); ok && info.GetDomain() == migrationTransferErrorDomain && info.GetReason() == migrationOversizedVersionReason {
			return errors.WithStack(errors.Mark(err, errMigrationOversizedSourceVersion))
		}
	}
	return err
}
