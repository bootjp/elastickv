package distribution

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"math"
	"sort"

	"github.com/cockroachdb/errors"
)

const (
	ownershipPartitionCatalogPrefix      = catalogMetaPrefix + "ownership_partition/"
	ownershipPartitionManifestVersion    = byte(1)
	ownershipPartitionManifestChildCount = 2
	// Three bounded route descriptors, including JSON byte-slice expansion.
	// This bounds only the control record, not ordinary client values.
	maxOwnershipPartitionManifestBytes = 16 << 20
)

var ErrOwnershipPartitionManifest = errors.New("invalid committed ownership partition manifest")

// OwnershipPartitionManifest records a same-owner geometry operation in the
// same root transaction as its route/version mutations. It is independent of
// compactable catalog deltas and TTL-collected split-job history.
type OwnershipPartitionManifest struct {
	PreviousVersion uint64
	Version         uint64
	JobID           uint64
	Parent          RouteDescriptor
	Children        []RouteDescriptor
}

// CatalogOwnershipPartitionKey indexes immutable manifests by the full prior
// descriptor identity. A reused route ID or changed bounds cannot alias it.
func CatalogOwnershipPartitionKey(routeID uint64, descriptorHash []byte) ([]byte, error) {
	if routeID == 0 || len(descriptorHash) != sha256.Size {
		return nil, errors.WithStack(ErrOwnershipPartitionManifest)
	}
	key := make([]byte, len(ownershipPartitionCatalogPrefix)+catalogUint64Bytes+sha256.Size)
	copy(key, ownershipPartitionCatalogPrefix)
	binary.BigEndian.PutUint64(key[len(ownershipPartitionCatalogPrefix):], routeID)
	copy(key[len(ownershipPartitionCatalogPrefix)+catalogUint64Bytes:], descriptorHash)
	return key, nil
}

// OwnershipPartitionDescriptorHash uses the same descriptor encoding as the
// root ownership producer, including bounds, staged lineage and known floor.
func OwnershipPartitionDescriptorHash(route RouteDescriptor) ([]byte, error) {
	encoded, err := EncodeRouteDescriptorForCatalogWrite(route, true)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	digest := sha256.Sum256(encoded)
	return digest[:], nil
}

// EncodeOwnershipPartitionManifest gives the operation a deterministic identity
// without mutating or aliasing caller-owned descriptors.
func EncodeOwnershipPartitionManifest(manifest OwnershipPartitionManifest) ([]byte, error) {
	manifest.Parent = cloneOwnershipPartitionRoute(manifest.Parent)
	manifest.Children = cloneOwnershipPartitionChildren(manifest.Children)
	if err := validateOwnershipPartitionManifest(manifest); err != nil {
		return nil, err
	}
	encoded, err := json.Marshal(manifest)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if len(encoded) >= maxOwnershipPartitionManifestBytes {
		return nil, errors.WithStack(ErrOwnershipPartitionManifest)
	}
	return append([]byte{ownershipPartitionManifestVersion}, encoded...), nil
}

func DecodeOwnershipPartitionManifest(encoded []byte) (OwnershipPartitionManifest, error) {
	if len(encoded) == 0 || len(encoded) > maxOwnershipPartitionManifestBytes || encoded[0] != ownershipPartitionManifestVersion {
		return OwnershipPartitionManifest{}, errors.WithStack(ErrOwnershipPartitionManifest)
	}
	var manifest OwnershipPartitionManifest
	if err := json.Unmarshal(encoded[1:], &manifest); err != nil {
		return OwnershipPartitionManifest{}, errors.Wrap(ErrOwnershipPartitionManifest, err.Error())
	}
	manifest.Children = cloneOwnershipPartitionChildren(manifest.Children)
	if err := validateOwnershipPartitionManifest(manifest); err != nil {
		return OwnershipPartitionManifest{}, err
	}
	return manifest, nil
}

// OwnershipPartitionManifestHash identifies the committed operation, not an
// ownership grant or a data-group configuration epoch.
func OwnershipPartitionManifestHash(manifest OwnershipPartitionManifest) ([]byte, error) {
	encoded, err := EncodeOwnershipPartitionManifest(manifest)
	if err != nil {
		return nil, err
	}
	digest := sha256.Sum256(encoded)
	return digest[:], nil
}

func cloneOwnershipPartitionRoute(route RouteDescriptor) RouteDescriptor {
	route.Start = bytes.Clone(route.Start)
	route.End = bytes.Clone(route.End)
	return route
}

func cloneOwnershipPartitionChildren(children []RouteDescriptor) []RouteDescriptor {
	out := make([]RouteDescriptor, len(children))
	for i, child := range children {
		out[i] = cloneOwnershipPartitionRoute(child)
	}
	sort.Slice(out, func(i, j int) bool { return bytes.Compare(out[i].Start, out[j].Start) < 0 })
	return out
}

func validateOwnershipPartitionManifest(manifest OwnershipPartitionManifest) error {
	if !ownershipPartitionManifestHeaderValid(manifest) {
		return errors.WithStack(ErrOwnershipPartitionManifest)
	}
	if _, err := OwnershipPartitionDescriptorHash(manifest.Parent); err != nil {
		return errors.Wrap(ErrOwnershipPartitionManifest, err.Error())
	}
	for _, child := range manifest.Children {
		if !ownershipPartitionChildIdentity(manifest.Parent, child) || !ownershipPartitionChildLineage(manifest.Parent, child) {
			return errors.WithStack(ErrOwnershipPartitionManifest)
		}
		if _, err := OwnershipPartitionDescriptorHash(child); err != nil {
			return errors.Wrap(ErrOwnershipPartitionManifest, err.Error())
		}
	}
	return validateOwnershipPartitionTiling(manifest)
}

func ownershipPartitionManifestHeaderValid(manifest OwnershipPartitionManifest) bool {
	return manifest.PreviousVersion != 0 && manifest.PreviousVersion != math.MaxUint64 &&
		manifest.Version == manifest.PreviousVersion+1 && manifest.Parent.State == RouteStateActive &&
		len(manifest.Children) == ownershipPartitionManifestChildCount
}

func ownershipPartitionChildIdentity(parent, child RouteDescriptor) bool {
	return child.RouteID != parent.RouteID && child.ParentRouteID == parent.RouteID &&
		child.GroupID == parent.GroupID && child.State == parent.State
}

func ownershipPartitionChildLineage(parent, child RouteDescriptor) bool {
	return child.MinWriteTSExclusive == parent.MinWriteTSExclusive && child.StagedVisibilityActive == parent.StagedVisibilityActive &&
		child.MigrationJobID == parent.MigrationJobID && child.SplitAtHLC != 0 && child.SplitAtHLC > parent.SplitAtHLC
}

func validateOwnershipPartitionTiling(manifest OwnershipPartitionManifest) error {
	left, right := manifest.Children[0], manifest.Children[1]
	if left.RouteID == right.RouteID || !bytes.Equal(left.Start, manifest.Parent.Start) ||
		!bytes.Equal(right.End, manifest.Parent.End) || len(left.End) == 0 ||
		!bytes.Equal(left.End, right.Start) || left.SplitAtHLC != right.SplitAtHLC {
		return errors.WithStack(ErrOwnershipPartitionManifest)
	}
	return nil
}
