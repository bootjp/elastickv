package adapter

import "github.com/cockroachdb/errors"

// ErrAdminS3Stopping is returned by the admin object operations that use
// the blob peer cluster (AdminPutObject, AdminGetObject) once Stop has
// begun: the cluster is about to close, so the operation is refused rather
// than started against it. The admin handler maps it to the not-leader
// response so the client retries against another node.
var ErrAdminS3Stopping = errors.New("s3 admin: server is stopping")

// enterAdminBlobUse registers an admin object operation that may replicate
// or fetch chunk blobs through the blob peer cluster. Stop waits, bounded,
// for every registered operation before it closes the cluster: the admin
// listener's object bridge (and the gRPC admin forwarder) call into this
// server outside its HTTP listener, so draining that listener does not
// cover them. Each successful call must be paired with exitAdminBlobUse.
func (s *S3Server) enterAdminBlobUse() error {
	if s == nil || s.adminBlobUsers.enter() {
		return nil
	}
	return ErrAdminS3Stopping
}

func (s *S3Server) exitAdminBlobUse() {
	if s != nil {
		s.adminBlobUsers.exit()
	}
}
