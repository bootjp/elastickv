package raftbootstrap

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/require"
)

func acceptTestGuardedInner(ControlKind, []byte) error { return nil }

func testGuardedCommand(kind ControlKind) GuardedCommand {
	var cluster UUID
	var digest Digest
	var operationID OperationID
	for i := range cluster {
		cluster[i] = byte(i + 1)
		operationID[i] = byte(32 + i)
	}
	for i := range digest {
		digest[i] = byte(64 + i)
	}
	return GuardedCommand{
		Kind: kind,
		Witness: Witness{
			Cluster: cluster, Group: 7, ActivationIndex: 11, Generation: 12,
			ConfigurationIndex: 13, ConfigurationDigest: digest,
		},
		OperationID: operationID,
		Inner:       []byte("inner-command"),
	}
}

func TestGuardedCommandRoundTrip(t *testing.T) {
	t.Parallel()
	for _, kind := range []ControlKind{ControlGuardedWatermark, ControlGuardedCatalog} {
		want := testGuardedCommand(kind)
		encoded, err := EncodeGuardedCommand(want, acceptTestGuardedInner)
		require.NoError(t, err)
		require.Equal(t, byte(0x1f), encoded[0])
		require.Equal(t, "BC", string(encoded[1:3]))
		require.Equal(t, uint32(len(encoded)-controlHeaderSize), binary.BigEndian.Uint32(encoded[5:9]))

		got, matched, err := ParseGuardedCommand(encoded, acceptTestGuardedInner)
		require.NoError(t, err)
		require.True(t, matched)
		require.Equal(t, want, got)

		encoded[len(encoded)-1] ^= 0xff
		require.Equal(t, []byte("inner-command"), got.Inner, "decoded inner bytes must not alias the input")
	}
}

func TestGuardedCommandGoldenLayout(t *testing.T) {
	t.Parallel()
	command := testGuardedCommand(ControlGuardedCatalog)
	encoded, err := EncodeGuardedCommand(command, acceptTestGuardedInner)
	require.NoError(t, err)
	want, err := hex.DecodeString(
		"1f4243010500000071" +
			"0102030405060708090a0b0c0d0e0f10" +
			"0000000000000007" +
			"000000000000000b" +
			"000000000000000c" +
			"000000000000000d" +
			"404142434445464748494a4b4c4d4e4f505152535455565758595a5b5c5d5e5f" +
			"202122232425262728292a2b2c2d2e2f" +
			"0000000d" +
			"696e6e65722d636f6d6d616e64",
	)
	require.NoError(t, err)
	require.Equal(t, want, encoded)
}

func TestGuardedCommandParserNeverFallsThroughReservedPrefix(t *testing.T) {
	t.Parallel()
	for _, payload := range [][]byte{
		{controlPrefix},
		{controlPrefix, 'X', 'X', controlVersion, byte(ControlGuardedCatalog), 0, 0, 0, 0},
		{controlPrefix, 'B', 'C', controlVersion + 1, byte(ControlGuardedCatalog), 0, 0, 0, 0},
		{controlPrefix, 'B', 'C', controlVersion, byte(ControlActivate), 0, 0, 0, 0},
	} {
		_, matched, err := ParseGuardedCommand(payload, acceptTestGuardedInner)
		require.True(t, matched)
		require.Error(t, err)
	}
	_, matched, err := ParseGuardedCommand([]byte("legacy"), acceptTestGuardedInner)
	require.NoError(t, err)
	require.False(t, matched)
}

func TestGuardedCommandRejectsInvalidAndNoncanonicalRecords(t *testing.T) {
	t.Parallel()
	valid := testGuardedCommand(ControlGuardedCatalog)
	for _, mutate := range []func(*GuardedCommand){
		func(c *GuardedCommand) { c.Witness.Cluster = UUID{} },
		func(c *GuardedCommand) { c.Witness.ActivationIndex = 0 },
		func(c *GuardedCommand) { c.Witness.Generation = 0 },
		func(c *GuardedCommand) { c.Witness.ConfigurationIndex = 0 },
		func(c *GuardedCommand) { c.Witness.ConfigurationDigest = Digest{} },
		func(c *GuardedCommand) { c.OperationID = OperationID{} },
		func(c *GuardedCommand) { c.Inner = nil },
	} {
		candidate := valid
		mutate(&candidate)
		_, err := EncodeGuardedCommand(candidate, acceptTestGuardedInner)
		require.ErrorIs(t, err, ErrInvalid)
	}

	encoded, err := EncodeGuardedCommand(valid, acceptTestGuardedInner)
	require.NoError(t, err)
	for _, corrupt := range [][]byte{
		append(bytes.Clone(encoded), 0),
		func() []byte { b := bytes.Clone(encoded); binary.BigEndian.PutUint32(b[5:9], 1); return b }(),
		func() []byte {
			b := bytes.Clone(encoded)
			binary.BigEndian.PutUint32(b[controlHeaderSize+WitnessSize+operationIDSize:], 1)
			return b
		}(),
	} {
		_, matched, err := ParseGuardedCommand(corrupt, acceptTestGuardedInner)
		require.True(t, matched)
		require.ErrorIs(t, err, ErrInvalid)
	}
}

func TestGuardedCommandParserRejectsEveryTruncationAndExternalInvalidField(t *testing.T) {
	t.Parallel()
	valid, err := EncodeGuardedCommand(testGuardedCommand(ControlGuardedCatalog), acceptTestGuardedInner)
	require.NoError(t, err)
	for length := 1; length < len(valid); length++ {
		_, matched, parseErr := ParseGuardedCommand(valid[:length], acceptTestGuardedInner)
		require.True(t, matched, "length %d", length)
		require.Error(t, parseErr, "length %d", length)
	}

	for _, offset := range []int{controlHeaderSize, controlHeaderSize + WitnessSize, controlHeaderSize + WitnessSize + operationIDSize} {
		corrupt := bytes.Clone(valid)
		end := min(offset+operationIDSize, len(corrupt))
		clear(corrupt[offset:end])
		_, matched, parseErr := ParseGuardedCommand(corrupt, acceptTestGuardedInner)
		require.True(t, matched)
		require.ErrorIs(t, parseErr, ErrInvalid)
	}

	maxLength := bytes.Clone(valid)
	binary.BigEndian.PutUint32(maxLength[5:9], ^uint32(0))
	_, matched, err := ParseGuardedCommand(maxLength, acceptTestGuardedInner)
	require.True(t, matched)
	require.ErrorIs(t, err, ErrInvalid)

	maxInner := bytes.Clone(valid)
	binary.BigEndian.PutUint32(maxInner[controlHeaderSize+WitnessSize+operationIDSize:], ^uint32(0))
	_, matched, err = ParseGuardedCommand(maxInner, acceptTestGuardedInner)
	require.True(t, matched)
	require.ErrorIs(t, err, ErrInvalid)

	oversized := make([]byte, MaxFileSize+1)
	copy(oversized, valid)
	binary.BigEndian.PutUint32(oversized[5:9], uint32(MaxFileSize+1-controlHeaderSize))
	_, matched, err = ParseGuardedCommand(oversized, acceptTestGuardedInner)
	require.True(t, matched)
	require.ErrorIs(t, err, ErrInvalid)
}

func TestGuardedCommandHonorsMaximumFrameSize(t *testing.T) {
	t.Parallel()
	command := testGuardedCommand(ControlGuardedWatermark)
	command.Inner = make([]byte, MaxFileSize-controlHeaderSize-guardedBodyHeaderSize)
	encoded, err := EncodeGuardedCommand(command, acceptTestGuardedInner)
	require.NoError(t, err)
	require.Len(t, encoded, MaxFileSize)
	_, matched, err := ParseGuardedCommand(encoded, acceptTestGuardedInner)
	require.NoError(t, err)
	require.True(t, matched)

	command.Inner = append(command.Inner, 0)
	_, err = EncodeGuardedCommand(command, acceptTestGuardedInner)
	require.ErrorIs(t, err, ErrInvalid)
}

func TestGuardedCommandRequiresInnerValidation(t *testing.T) {
	t.Parallel()
	command := testGuardedCommand(ControlGuardedCatalog)
	_, err := EncodeGuardedCommand(command, nil)
	require.ErrorIs(t, err, ErrInvalid)

	encoded, err := EncodeGuardedCommand(command, acceptTestGuardedInner)
	require.NoError(t, err)
	reject := func(ControlKind, []byte) error { return ErrInvalid }
	_, matched, err := ParseGuardedCommand(encoded, reject)
	require.True(t, matched)
	require.ErrorIs(t, err, ErrInvalid)
}
