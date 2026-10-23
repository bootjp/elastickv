package kv

import (
	"bytes"
	"encoding/json"
	"slices"
	"testing"

	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"
)

func codecPropertyControlSeeds(t *testing.T) []MigrationPartitionControl {
	t.Helper()
	var seeds []MigrationPartitionControl
	for _, group := range []uint64{1, 2} {
		st := store.NewMVCCStore()
		t.Cleanup(func() { require.NoError(t, st.Close()) })
		_, seed := partitionControlFixture(t, st, group)
		seeds = append(seeds, seed)
	}
	return seeds
}

func codecPropertyControl(t *rapid.T, seeds []MigrationPartitionControl) MigrationPartitionControl {
	t.Helper()
	seed := seeds[rapid.IntRange(0, len(seeds)-1).Draw(t, "owner role")]
	manifest, err := distribution.DecodeOwnershipPartitionManifest(seed.Manifest)
	require.NoError(t, err)
	split := append([]byte("m"), rapid.SliceOfN(rapid.Byte(), 0, 32).Draw(t, "split suffix")...)
	route := rapid.Uint64Range(10, 1000000).Draw(t, "child route id")
	stamp := rapid.Uint64Range(1, ^uint64(0)).Draw(t, "split timestamp")
	manifest.Children[0].RouteID, manifest.Children[1].RouteID = route, route+1
	manifest.Children[0].End, manifest.Children[1].Start = split, split
	manifest.Children[0].SplitAtHLC, manifest.Children[1].SplitAtHLC = stamp, stamp
	raw, err := distribution.EncodeOwnershipPartitionManifest(manifest)
	require.NoError(t, err)
	slices.Reverse(manifest.Children)
	permuted, err := distribution.EncodeOwnershipPartitionManifest(manifest)
	require.NoError(t, err)
	require.Equal(t, raw, permuted, "manifest encoding canonicalizes child order")
	control, err := BuildMigrationPartitionControl(nil, seed.PriorOwnership, raw, nil,
		rapid.Uint64Range(2, 1000000).Draw(t, "catalog version"),
		rapid.Uint64Range(9, 1000000).Draw(t, "configuration index"))
	require.NoError(t, err)
	return control
}

func TestMigrationCodecPropertyControlRoundTrip(t *testing.T) {
	seeds := codecPropertyControlSeeds(t)
	rapid.Check(t, func(t *rapid.T) {
		control := codecPropertyControl(t, seeds)
		raw, err := EncodeMigrationPartitionControl(control)
		require.NoError(t, err)
		decoded, err := DecodeMigrationPartitionControl(raw)
		require.NoError(t, err)
		require.Equal(t, control, decoded)
		reencoded, err := EncodeMigrationPartitionControl(decoded)
		require.NoError(t, err)
		require.Equal(t, raw, reencoded)
		transition, err := EncodeMigrationPartitionJSON(control.Transition)
		require.NoError(t, err)
		var actual store.MigrationOwnershipPartitionTransition
		require.NoError(t, DecodeMigrationPartitionJSON(transition, &actual))
		require.Equal(t, control.Transition, actual)
		id, err := store.MigrationOwnershipPartitionReceiptIDFor(control.Transition)
		require.NoError(t, err)
		slices.Reverse(actual.After)
		permutedID, err := store.MigrationOwnershipPartitionReceiptIDFor(actual)
		require.NoError(t, err)
		require.Equal(t, id, permutedID)
	})
}

func TestMigrationCodecPropertyControlRejectsMalformed(t *testing.T) {
	seeds := codecPropertyControlSeeds(t)
	rapid.Check(t, func(t *rapid.T) {
		control := codecPropertyControl(t, seeds)
		raw, err := EncodeMigrationPartitionControl(control)
		require.NoError(t, err)
		malformed := [][]byte{
			append([]byte(" "), raw...), append(bytes.Clone(raw), ' '),
			append(bytes.Clone(raw), []byte(`{}`)...), raw[:len(raw)-1],
			append([]byte(`{"Unknown":true,`), raw[1:]...),
			append([]byte(`{"Transition":{},`), raw[1:]...),
			bytes.Replace(raw, []byte(`"Transition":`), []byte(`"transition":`), 1),
		}
		for _, bad := range malformed {
			_, err = DecodeMigrationPartitionControl(bad)
			require.Error(t, err)
		}
		mutations := []struct {
			name   string
			mutate func(*MigrationPartitionControl)
		}{
			{"child descriptor", func(c *MigrationPartitionControl) { c.Transition.After[0].DescriptorHash[0] ^= 1 }},
			{"prior hash", func(c *MigrationPartitionControl) { c.Transition.Before[0].AttestationHash[0] ^= 1 }},
			{"configuration", func(c *MigrationPartitionControl) { c.Transition.ConfigurationIndex++ }},
			{"operation identity", func(c *MigrationPartitionControl) { c.Transition.OperationID += "/changed" }},
			{"child ordering", func(c *MigrationPartitionControl) { slices.Reverse(c.Transition.After) }},
		}
		for _, mutation := range mutations {
			bad, decodeErr := DecodeMigrationPartitionControl(raw)
			require.NoError(t, decodeErr)
			mutation.mutate(&bad)
			encoded, encodeErr := json.Marshal(bad)
			require.NoError(t, encodeErr)
			_, decodeErr = DecodeMigrationPartitionControl(encoded)
			require.Error(t, decodeErr, mutation.name)
		}
	})
}
