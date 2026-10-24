package distribution

import (
	"bytes"
	"context"
	"encoding/binary"

	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
)

const ownershipPartitionNegativeScanLimit = 2

// OwnershipPartitionNegativeAt discovers geometry for historical negative
// evidence only. It is NOT an ownership attestation: the caller must validate
// every prior as NONOWNER and preserve its owner, floor, role and lineage.
// Production route IDs are never reused. Two retained records under one ID
// are therefore ambiguous, even if only one appears to match the bounds.
func (s *CatalogStore) OwnershipPartitionNegativeAt(ctx context.Context, routeID, parentRouteID uint64, start, end []byte, readTS uint64) (OwnershipPartitionManifest, bool, error) {
	var zero OwnershipPartitionManifest
	if s == nil || s.store == nil {
		return zero, false, errors.WithStack(ErrCatalogStoreRequired)
	}
	if routeID == 0 || readTS == 0 {
		return zero, false, errors.WithStack(ErrOwnershipPartitionManifest)
	}
	prefix := binary.BigEndian.AppendUint64([]byte(ownershipPartitionCatalogPrefix), routeID)
	rows, err := s.store.ScanAt(ctx, prefix, store.PrefixScanEnd(prefix), ownershipPartitionNegativeScanLimit, readTS)
	if err != nil {
		return zero, false, errors.WithStack(err)
	}
	if len(rows) == 0 {
		return zero, false, nil
	}
	if len(rows) != 1 {
		return zero, false, errors.WithStack(ErrOwnershipPartitionManifest)
	}
	manifest, err := s.validateNegativeOwnershipPartitionRecord(ctx, routeID, rows[0].Key, rows[0].Value, readTS)
	if err != nil {
		return zero, false, err
	}
	if !ownershipPartitionNegativeBoundsMatch(manifest.Parent, parentRouteID, start, end) {
		return zero, false, nil
	}
	return manifest, true, nil
}

func (s *CatalogStore) validateNegativeOwnershipPartitionRecord(ctx context.Context, routeID uint64, key, raw []byte, readTS uint64) (OwnershipPartitionManifest, error) {
	var zero OwnershipPartitionManifest
	manifest, err := DecodeOwnershipPartitionManifest(raw)
	if err != nil {
		return zero, err
	}
	hash, err := OwnershipPartitionDescriptorHash(manifest.Parent)
	if err != nil {
		return zero, err
	}
	expected, err := CatalogOwnershipPartitionKey(routeID, hash)
	if err != nil {
		return zero, err
	}
	canonical, err := EncodeOwnershipPartitionManifest(manifest)
	if err != nil {
		return zero, err
	}
	if !bytes.Equal(key, expected) || !bytes.Equal(raw, canonical) {
		return zero, errors.WithStack(ErrOwnershipPartitionManifest)
	}
	return s.validateOwnershipPartitionRecordAt(ctx, readTS, routeID, hash, raw)
}

func ownershipPartitionNegativeBoundsMatch(parent RouteDescriptor, parentRouteID uint64, start, end []byte) bool {
	return parent.ParentRouteID == parentRouteID && bytes.Equal(parent.Start, start) && bytes.Equal(parent.End, end)
}
