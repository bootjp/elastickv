package kv

import (
	"testing"

	"github.com/bootjp/elastickv/internal/raftbootstrap"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func testBootstrapGuardedCommand(t *testing.T, kind raftbootstrap.ControlKind, inner []byte) raftbootstrap.GuardedCommand {
	t.Helper()
	var cluster raftbootstrap.UUID
	var digest raftbootstrap.Digest
	var operationID raftbootstrap.OperationID
	cluster[0], digest[0], operationID[0] = 1, 2, 3
	return raftbootstrap.GuardedCommand{
		Kind: kind,
		Witness: raftbootstrap.Witness{
			Cluster: cluster, Group: 1, ActivationIndex: 2, Generation: 3,
			ConfigurationIndex: 4, ConfigurationDigest: digest,
		},
		OperationID: operationID,
		Inner:       inner,
	}
}

func testGuardedCatalogInner(t *testing.T) []byte {
	t.Helper()
	request := onePhaseTxnRequestWithPrevCommit(
		10, 20, 0, []byte("!dist|meta|version"),
		[]*Elem[OP]{{Op: Put, Key: []byte("!dist|meta|version"), Value: []byte{1}}},
		[][]byte{[]byte("!dist|meta|version")}, 0, nil,
	)
	encoded, err := proto.Marshal(request)
	require.NoError(t, err)
	return prependByte(raftEncodeSingle, encoded)
}

func TestGuardedBootstrapInnerBindsKindToPayload(t *testing.T) {
	t.Parallel()
	watermark := EncodeWatermarkAdvance(42)
	catalog := testGuardedCatalogInner(t)

	for _, tc := range []struct {
		kind  raftbootstrap.ControlKind
		inner []byte
	}{
		{kind: raftbootstrap.ControlGuardedWatermark, inner: watermark},
		{kind: raftbootstrap.ControlGuardedCatalog, inner: catalog},
	} {
		command := testBootstrapGuardedCommand(t, tc.kind, tc.inner)
		encoded, err := EncodeGuardedBootstrapCommand(command)
		require.NoError(t, err)
		decoded, matched, err := ParseGuardedBootstrapCommand(encoded)
		require.NoError(t, err)
		require.True(t, matched)
		require.Equal(t, command, decoded)
	}

	_, err := EncodeGuardedBootstrapCommand(testBootstrapGuardedCommand(t, raftbootstrap.ControlGuardedWatermark, catalog))
	require.ErrorIs(t, err, ErrInvalidGuardedBootstrapInner)
	_, err = EncodeGuardedBootstrapCommand(testBootstrapGuardedCommand(t, raftbootstrap.ControlGuardedCatalog, watermark))
	require.ErrorIs(t, err, ErrInvalidGuardedBootstrapInner)
}

func TestGuardedCatalogInnerRejectsNonCatalogAndNoncanonicalRequests(t *testing.T) {
	t.Parallel()
	valid := testGuardedCatalogInner(t)
	request := onePhaseTxnRequestWithPrevCommit(
		10, 20, 0, []byte("user-key"),
		[]*Elem[OP]{{Op: Put, Key: []byte("user-key"), Value: []byte("value")}},
		[][]byte{[]byte("user-key")}, 0, nil,
	)
	nonCatalog, err := proto.Marshal(request)
	require.NoError(t, err)

	for _, inner := range [][]byte{
		nil,
		[]byte("arbitrary"),
		EncodeWatermarkAdvance(0),
		prependByte(raftEncodeSingle, nonCatalog),
		append(valid, 0x78, 0x00),
	} {
		err := ValidateGuardedBootstrapInner(raftbootstrap.ControlGuardedCatalog, inner)
		require.ErrorIs(t, err, ErrInvalidGuardedBootstrapInner)
	}
}
