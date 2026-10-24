package kv

import (
	"github.com/bootjp/elastickv/distribution"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/cockroachdb/errors"
)

// Validate supplied user mutations before evidence, dedup or resolution can
// short-circuit a request. Typed control opcodes never enter this path.
func verifyRequestMigrationControlKeys(request *pb.Request) error {
	mutations := request.GetMutations()
	if request.GetIsTxn() {
		_, userMutations, err := extractTxnMeta(mutations)
		if err != nil {
			return err
		}
		mutations = userMutations
	}
	for _, mutation := range mutations {
		if isTxnInternalKey(mutation.GetKey()) || distribution.IsMigrationOnlyControlKey(mutation.GetKey()) ||
			(mutation.GetOp() == pb.Op_DEL_PREFIX && distribution.ReservedControlPrefixIntersects(mutation.GetKey())) {
			return errors.WithStack(ErrInvalidRequest)
		}
	}
	return nil
}
