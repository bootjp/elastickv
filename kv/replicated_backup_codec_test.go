package kv

import (
	"encoding/binary"
	"testing"

	pb "github.com/bootjp/elastickv/proto"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func replicatedBackupWireFixtures() []ReplicatedBackupCommand {
	id := BackupIdentity{Epoch: 1, Generation: 7}
	pinID := backupTrackerTestPinID(1)
	return []ReplicatedBackupCommand{
		{Operation: ReplicatedBackupPin, Identity: id, PinID: pinID, ReadTS: 100, DeadlineMillis: 200},
		{Operation: ReplicatedBackupExtend, Identity: id, PinID: pinID, DeadlineMillis: 300},
		{Operation: ReplicatedBackupRelease, Identity: id, PinID: pinID},
		{Operation: ReplicatedBackupExpire, Identity: id, PinID: pinID, ExpectedDeadlineMillis: 200},
		{Operation: ReplicatedBackupAllocate, Identity: BackupIdentity{Epoch: 1}},
	}
}

func TestReplicatedBackupWireRoundTrip(t *testing.T) {
	t.Parallel()
	require.Equal(t, replicatedBackupPayloadSize, binary.Size(replicatedBackupWire{}))
	require.Equal(t, byte(0x0f), raftEncodeReplicatedBackup)
	require.Equal(t, byte(replicatedPinAdmit), byte(ReplicatedBackupPin))
	require.Equal(t, byte(replicatedPinExtend), byte(ReplicatedBackupExtend))
	require.Equal(t, byte(replicatedPinRelease), byte(ReplicatedBackupRelease))
	require.Equal(t, byte(replicatedPinExpire), byte(ReplicatedBackupExpire))
	for _, command := range replicatedBackupWireFixtures() {
		encoded, err := EncodeReplicatedBackupCommand(command)
		require.NoError(t, err)
		require.Len(t, encoded, 1+replicatedBackupPayloadSize)
		require.Equal(t, raftEncodeReplicatedBackup, encoded[0])
		decoded, err := decodeReplicatedBackupCommand(encoded[1:])
		require.NoError(t, err)
		require.Equal(t, command, decoded)
		require.Error(t, proto.Unmarshal(encoded, new(pb.Request)), "must not overlap a legacy protobuf message")
	}
}

func TestReplicatedBackupWireRejectsMalformedOrIgnoredFields(t *testing.T) {
	t.Parallel()
	valid := replicatedBackupWireFixtures()[0]
	encoded, err := EncodeReplicatedBackupCommand(valid)
	require.NoError(t, err)
	for length := range replicatedBackupPayloadSize {
		_, err := decodeReplicatedBackupCommand(encoded[1 : 1+length])
		require.Error(t, err)
	}
	_, err = decodeReplicatedBackupCommand(append(encoded[1:], 0))
	require.Error(t, err)
	for _, field := range []int{1, 2} {
		bad := append([]byte(nil), encoded...)
		bad[field] = 0xff
		_, err := decodeReplicatedBackupCommand(bad[1:])
		require.Error(t, err)
	}
	for _, command := range []ReplicatedBackupCommand{
		{Operation: ReplicatedBackupAllocate},
		{Operation: ReplicatedBackupAllocate, Identity: valid.Identity},
		{Operation: ReplicatedBackupPin, Identity: valid.Identity, ReadTS: 100, DeadlineMillis: 200},
		{Operation: ReplicatedBackupPin, Identity: valid.Identity, PinID: valid.PinID, ReadTS: 100, DeadlineMillis: -1},
		{Operation: ReplicatedBackupExtend, Identity: valid.Identity, PinID: valid.PinID, ReadTS: 100, DeadlineMillis: 300},
		{Operation: ReplicatedBackupRelease, Identity: valid.Identity, PinID: valid.PinID, DeadlineMillis: 200},
		{Operation: ReplicatedBackupExpire, Identity: valid.Identity, PinID: valid.PinID, DeadlineMillis: 200, ExpectedDeadlineMillis: 200},
	} {
		_, err := EncodeReplicatedBackupCommand(command)
		require.Error(t, err)
	}
}

func FuzzReplicatedBackupWire(f *testing.F) {
	for _, command := range replicatedBackupWireFixtures() {
		encoded, err := EncodeReplicatedBackupCommand(command)
		require.NoError(f, err)
		f.Add(encoded[1:])
	}
	f.Fuzz(func(t *testing.T, payload []byte) {
		command, err := decodeReplicatedBackupCommand(payload)
		if err != nil {
			return
		}
		encoded, err := EncodeReplicatedBackupCommand(command)
		require.NoError(t, err)
		require.Equal(t, payload, encoded[1:])
	})
}
