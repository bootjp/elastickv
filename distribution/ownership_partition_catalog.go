package distribution

import (
	"bytes"
	"context"

	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
)

// BuildOwnershipPartitionMutationAt prepares, but does not publish, immutable
// root lineage. The caller MUST include this key and CatalogVersionKey in its
// OCC read set and commit this mutation with the route/version/job mutations.
func (s *CatalogStore) BuildOwnershipPartitionMutationAt(ctx context.Context, readTS uint64, manifest OwnershipPartitionManifest) (*store.KVPairMutation, error) {
	if s == nil || s.store == nil {
		return nil, errors.WithStack(ErrCatalogStoreRequired)
	}
	version, err := s.versionAt(ctx, readTS)
	if err != nil {
		return nil, err
	}
	if version != manifest.PreviousVersion {
		return nil, errors.WithStack(ErrCatalogVersionMismatch)
	}
	encoded, err := EncodeOwnershipPartitionManifest(manifest)
	if err != nil {
		return nil, err
	}
	digest, err := OwnershipPartitionDescriptorHash(manifest.Parent)
	if err != nil {
		return nil, err
	}
	key, err := CatalogOwnershipPartitionKey(manifest.Parent.RouteID, digest)
	if err != nil {
		return nil, err
	}
	if err := s.validateOwnershipPartitionExistingAt(ctx, readTS, key, encoded); err != nil {
		return nil, err
	}
	return &store.KVPairMutation{Op: store.OpTypePut, Key: key, Value: encoded}, nil
}

func (s *CatalogStore) validateOwnershipPartitionExistingAt(ctx context.Context, readTS uint64, key, encoded []byte) error {
	prior, err := s.store.GetAt(ctx, key, readTS)
	if err != nil && !errors.Is(err, store.ErrKeyNotFound) {
		return errors.WithStack(err)
	}
	if err == nil && !bytes.Equal(prior, encoded) {
		return errors.WithStack(ErrOwnershipPartitionManifest)
	}
	return nil
}

// OwnershipPartitionAt loads historical lineage at a caller's leader-issued
// root read timestamp. Root RPC callers additionally require a linearizable
// applied-read barrier; a cache-backed route listing is not sufficient.
func (s *CatalogStore) OwnershipPartitionAt(ctx context.Context, routeID uint64, descriptorHash []byte, readTS uint64) (OwnershipPartitionManifest, bool, error) {
	if s == nil || s.store == nil {
		return OwnershipPartitionManifest{}, false, errors.WithStack(ErrCatalogStoreRequired)
	}
	key, err := CatalogOwnershipPartitionKey(routeID, descriptorHash)
	if err != nil {
		return OwnershipPartitionManifest{}, false, err
	}
	raw, err := s.store.GetAt(ctx, key, readTS)
	if errors.Is(err, store.ErrKeyNotFound) {
		return OwnershipPartitionManifest{}, false, nil
	}
	if err != nil {
		return OwnershipPartitionManifest{}, false, errors.WithStack(err)
	}
	manifest, err := s.validateOwnershipPartitionRecordAt(ctx, readTS, routeID, descriptorHash, raw)
	if err != nil {
		return OwnershipPartitionManifest{}, false, err
	}
	return manifest, true, nil
}

func (s *CatalogStore) validateOwnershipPartitionRecordAt(ctx context.Context, readTS, routeID uint64, descriptorHash, raw []byte) (OwnershipPartitionManifest, error) {
	manifest, err := DecodeOwnershipPartitionManifest(raw)
	if err != nil {
		return OwnershipPartitionManifest{}, err
	}
	digest, err := OwnershipPartitionDescriptorHash(manifest.Parent)
	if err != nil {
		return OwnershipPartitionManifest{}, err
	}
	version, err := s.versionAt(ctx, readTS)
	if err != nil {
		return OwnershipPartitionManifest{}, err
	}
	if manifest.Parent.RouteID != routeID || !bytes.Equal(descriptorHash, digest) || manifest.Version > version {
		return OwnershipPartitionManifest{}, errors.WithStack(ErrOwnershipPartitionManifest)
	}
	return manifest, nil
}
