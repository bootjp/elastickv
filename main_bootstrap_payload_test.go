package main

import (
	"bytes"
	"encoding/binary"
	"slices"
	"testing"
	"time"

	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/internal/encryption"
	"github.com/bootjp/elastickv/internal/encryption/fsmwire"
	"github.com/bootjp/elastickv/internal/raftadmission"
	"github.com/bootjp/elastickv/internal/raftbootstrap"
	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

type bootstrapPayloadCase struct {
	name    string
	payload []byte
	kinds   []raftadmission.Kind
}

func bootstrapAdmissionKinds() []raftadmission.Kind {
	return []raftadmission.Kind{raftadmission.Data, raftadmission.Control, raftadmission.Catalog,
		raftadmission.Allocator, raftadmission.HLC, raftadmission.Registration, raftadmission.Recovery,
		raftadmission.Rotation, raftadmission.Maintenance, 0, 255}
}

func bootstrapPayloadProto(t testing.TB, tag byte, message proto.Message) []byte {
	t.Helper()
	encoded, err := (proto.MarshalOptions{Deterministic: true}).Marshal(message)
	require.NoError(t, err)
	return append([]byte{tag}, encoded...)
}

func bootstrapPayloadRequest() *pb.Request {
	return &pb.Request{Ts: 10, Mutations: []*pb.Mutation{{Op: pb.Op_PUT, Key: []byte("key"), Value: []byte("value")}}}
}

func bootstrapPayloadActivation(t testing.TB, group uint64) []byte {
	t.Helper()
	encoded, err := raftbootstrap.EncodeActivationCommand(bootstrapActivationFixture(t, group))
	require.NoError(t, err)
	return encoded
}

func bootstrapPayloadGuarded(t testing.TB, kind raftbootstrap.ControlKind) []byte {
	t.Helper()
	inner := kv.EncodeWatermarkAdvance(42)
	if kind == raftbootstrap.ControlGuardedCatalog {
		key := distribution.CatalogVersionKey()
		inner = bootstrapPayloadProto(t, bootstrapPayloadSingle, &pb.Request{
			IsTxn: true, Ts: 10, ReadKeys: [][]byte{key}, Mutations: []*pb.Mutation{
				{Op: pb.Op_PUT, Key: []byte(kv.TxnMetaPrefix), Value: kv.EncodeTxnMeta(kv.TxnMeta{PrimaryKey: key, CommitTS: 20})},
				{Op: pb.Op_PUT, Key: key, Value: distribution.EncodeCatalogVersion(1)},
			},
		})
	}
	command := raftbootstrap.GuardedCommand{Kind: kind,
		Witness: raftbootstrap.Witness{Cluster: raftbootstrap.UUID{1}, Group: 17, ActivationIndex: 2,
			Generation: 3, ConfigurationIndex: 4, ConfigurationDigest: raftbootstrap.Digest{5}},
		OperationID: raftbootstrap.OperationID{6}, Inner: inner}
	encoded, err := kv.EncodeGuardedBootstrapCommand(command)
	require.NoError(t, err)
	return encoded
}

func bootstrapPayloadFixtures(t testing.TB) []bootstrapPayloadCase {
	t.Helper()
	data := []raftadmission.Kind{raftadmission.Data, raftadmission.Maintenance}
	registration := append([]byte{fsmwire.OpRegistration}, fsmwire.EncodeRegistration(fsmwire.RegistrationPayload{DEKID: 1, FullNodeID: 2, LocalEpoch: 3})...)
	pin := kv.BackupPinEntry{PinID: kv.BackupPinID{1}, ReadTS: 10, Deadline: time.Unix(1, 0)}
	replicated, err := kv.EncodeReplicatedBackupCommand(kv.ReplicatedBackupCommand{
		Operation: kv.ReplicatedBackupPin, Identity: kv.BackupIdentity{Epoch: 1, Generation: 2},
		PinID: kv.BackupPinID{3}, ReadTS: 10, DeadlineMillis: 1000})
	require.NoError(t, err)
	retire, err := kv.MarshalMigrationRetireCommand(1)
	require.NoError(t, err)
	return []bootstrapPayloadCase{
		{"initial activation", bootstrapPayloadActivation(t, 17), []raftadmission.Kind{raftadmission.Recovery}},
		{"single", bootstrapPayloadProto(t, bootstrapPayloadSingle, bootstrapPayloadRequest()), data},
		{"batch", bootstrapPayloadProto(t, bootstrapPayloadBatch, &pb.RaftCommand{Requests: []*pb.Request{bootstrapPayloadRequest(), bootstrapPayloadRequest()}}), data},
		{"watermark", kv.EncodeWatermarkAdvance(42), data},
		{"pin", kv.EncodeBackupPinEntry(pin), data},
		{"reserve", kv.EncodeBackupReserveEntry(pin), data},
		{"extend", kv.EncodeBackupExtendEntry(kv.BackupExtendEntry{PinID: pin.PinID, Deadline: pin.Deadline}), data},
		{"release", kv.EncodeBackupReleaseEntry(kv.BackupReleaseEntry{PinID: pin.PinID}), data},
		{"unreserve", kv.EncodeBackupUnreserveEntry(kv.BackupUnreserveEntry{PinID: pin.PinID}), data},
		{"replicated backup", replicated, data},
		{"import", bootstrapPayloadProto(t, bootstrapPayloadImport, &pb.ImportRangeVersionsRequest{JobId: 1}), data},
		{"promote", bootstrapPayloadProto(t, bootstrapPayloadPromote, &pb.PromoteStagedVersionsRequest{JobId: 1}), data},
		{"cleanup", bootstrapPayloadProto(t, bootstrapPayloadCleanup, &pb.CleanupMigrationRequest{JobId: 1, Mode: pb.MigrationCleanupMode_MIGRATION_CLEANUP_MODE_METADATA}), data},
		{"readiness", bootstrapPayloadProto(t, bootstrapPayloadReadiness, &pb.TargetStagedReadinessRequest{Armed: true}), data},
		{"retire", retire, data},
		{"hlc", binary.BigEndian.AppendUint64([]byte{bootstrapPayloadHLC}, 1000), []raftadmission.Kind{raftadmission.HLC}},
		{"tso allocation", binary.BigEndian.AppendUint64([]byte("\x07TSOF\x01"), 42), []raftadmission.Kind{raftadmission.Allocator}},
		{"tso cutover", []byte("\x07TSOC\x01"), []raftadmission.Kind{raftadmission.Allocator}},
		{"tso pending", binary.BigEndian.AppendUint64([]byte("\x07TSOP\x01"), 42), []raftadmission.Kind{raftadmission.Allocator}},
		{"tso phase d", binary.BigEndian.AppendUint64([]byte("\x07TSOD\x01"), 42), []raftadmission.Kind{raftadmission.Allocator}},
		{"registration", registration, []raftadmission.Kind{raftadmission.Registration}},
		{"prepare activation", kv.EncodePrepareEvidenceFence(0), []raftadmission.Kind{raftadmission.Control}},
		{"prepare seal", kv.EncodePrepareEvidenceSeal(), []raftadmission.Kind{raftadmission.Control}},
		{"prepare fence", kv.EncodePrepareEvidenceFence(42), []raftadmission.Kind{raftadmission.Control}},
		{"prepare release", kv.EncodePrepareEvidenceRelease(42), []raftadmission.Kind{raftadmission.Control}},
		{"guarded watermark", bootstrapPayloadGuarded(t, raftbootstrap.ControlGuardedWatermark), []raftadmission.Kind{raftadmission.Catalog}},
		{"guarded catalog", bootstrapPayloadGuarded(t, raftbootstrap.ControlGuardedCatalog), []raftadmission.Kind{raftadmission.Catalog}},
	}
}

func TestBootstrapAdmissionPayloadLaneMatrix(t *testing.T) {
	for _, tc := range bootstrapPayloadFixtures(t) {
		t.Run(tc.name, func(t *testing.T) {
			for _, kind := range bootstrapAdmissionKinds() {
				err := validateBootstrapAdmissionPayload(kind, tc.payload)
				if slices.Contains(tc.kinds, kind) {
					require.NoError(t, err, "kind %d", kind)
				} else {
					require.ErrorIs(t, err, errBootstrapAdmissionPayload, "kind %d", kind)
				}
			}
		})
	}
}

func TestBootstrapAdmissionPayloadRejectsTrailingAndControlTruncation(t *testing.T) {
	for _, tc := range bootstrapPayloadFixtures(t) {
		t.Run(tc.name, func(t *testing.T) {
			for _, kind := range tc.kinds {
				require.ErrorIs(t, validateBootstrapAdmissionPayload(kind, append(bytes.Clone(tc.payload), 0)), errBootstrapAdmissionPayload)
				if kind == raftadmission.Data || kind == raftadmission.Maintenance {
					continue // Protobuf truncation at a field boundary can be a different complete data request.
				}
				for n := 0; n < len(tc.payload); n++ {
					require.ErrorIs(t, validateBootstrapAdmissionPayload(kind, tc.payload[:n]), errBootstrapAdmissionPayload, "length %d", n)
				}
			}
		})
	}
}

func TestBootstrapAdmissionPayloadRejectsUnknownAndUnsupportedControls(t *testing.T) {
	guarded := bootstrapPayloadGuarded(t, raftbootstrap.ControlGuardedWatermark)
	for _, controlKind := range []raftbootstrap.ControlKind{raftbootstrap.ControlActivate, raftbootstrap.ControlReserve, raftbootstrap.ControlAbort, 255} {
		candidate := bytes.Clone(guarded)
		candidate[4] = byte(controlKind)
		for _, lane := range bootstrapAdmissionKinds() {
			require.ErrorIs(t, validateBootstrapAdmissionPayload(lane, candidate), errBootstrapAdmissionPayload)
		}
	}
	for _, candidate := range [][]byte{nil, {}, {0xff}, {0x06}, {0x07}, {0x1f},
		[]byte("\x07TSOF\x02"), []byte("\x07TSOX\x01"), {fsmwire.OpBootstrap, 1}, {fsmwire.OpRotation, 1},
		bootstrapPayloadProto(t, bootstrapPayloadBatch, &pb.RaftCommand{}),
		bootstrapPayloadProto(t, bootstrapPayloadSingle, &pb.Request{}),
		bootstrapPayloadProto(t, bootstrapPayloadSingle, &pb.Request{Ts: 1}),
	} {
		for _, lane := range bootstrapAdmissionKinds() {
			require.ErrorIs(t, validateBootstrapAdmissionPayload(lane, candidate), errBootstrapAdmissionPayload)
		}
	}
}

func TestBootstrapAdmissionPayloadRejectsUnknownNestedFieldsAndEnums(t *testing.T) {
	unknown := []byte{0xf8, 0x07, 0x01}
	request := bootstrapPayloadRequest()
	request.Mutations[0].ProtoReflect().SetUnknown(unknown)
	badPhase := bootstrapPayloadRequest()
	badPhase.IsTxn, badPhase.Phase = true, pb.Phase(99)
	badOp := bootstrapPayloadRequest()
	badOp.Mutations[0].Op = pb.Op(99)
	rawPhase := bootstrapPayloadRequest()
	rawPhase.Phase = pb.Phase_PREPARE
	for name, payload := range map[string][]byte{
		"nested unknown":       bootstrapPayloadProto(t, bootstrapPayloadSingle, request),
		"batch nested unknown": bootstrapPayloadProto(t, bootstrapPayloadBatch, &pb.RaftCommand{Requests: []*pb.Request{bootstrapPayloadRequest(), request}}),
		"phase":                bootstrapPayloadProto(t, bootstrapPayloadSingle, badPhase),
		"operation":            bootstrapPayloadProto(t, bootstrapPayloadSingle, badOp),
		"raw phase":            bootstrapPayloadProto(t, bootstrapPayloadSingle, rawPhase),
		"mode":                 bootstrapPayloadProto(t, bootstrapPayloadCleanup, &pb.CleanupMigrationRequest{Mode: pb.MigrationCleanupMode(99)}),
		"role":                 bootstrapPayloadProto(t, bootstrapPayloadCleanup, &pb.CleanupMigrationRequest{Ownership: &pb.MigrationOwnershipProof{Role: pb.MigrationOwnershipRole(99)}}),
	} {
		t.Run(name, func(t *testing.T) {
			for _, lane := range bootstrapAdmissionKinds() {
				require.ErrorIs(t, validateBootstrapAdmissionPayload(lane, payload), errBootstrapAdmissionPayload)
			}
		})
	}
}

func TestBootstrapAdmissionPayloadRejectsAmbiguousProtobuf(t *testing.T) {
	single := bootstrapPayloadProto(t, bootstrapPayloadSingle, bootstrapPayloadRequest())
	batch := bootstrapPayloadProto(t, bootstrapPayloadBatch, &pb.RaftCommand{Requests: []*pb.Request{bootstrapPayloadRequest()}})
	for _, payload := range [][]byte{
		single[1:], batch[1:], // Legacy bare forms are not new-proposal codecs.
		append(bytes.Clone(single), 0xf8, 0x07, 0x01), // Unknown top-level field.
		append(bytes.Clone(single), 0x18, 0x0a),       // A trailing duplicate timestamp.
		append(bytes.Clone(single), single[1:]...),    // Concatenated requests merge in lenient protobuf.
	} {
		require.ErrorIs(t, validateBootstrapAdmissionPayload(raftadmission.Data, payload), errBootstrapAdmissionPayload)
	}
}

func TestBootstrapAdmissionPayloadRejectsInvalidScalarsAndBackup(t *testing.T) {
	for _, ceiling := range []uint64{0, uint64(1) << 48, ^uint64(0)} {
		require.ErrorIs(t, validateBootstrapAdmissionPayload(raftadmission.HLC,
			binary.BigEndian.AppendUint64([]byte{bootstrapPayloadHLC}, ceiling)), errBootstrapAdmissionPayload)
	}
	for _, header := range []string{"\x07TSOF\x01", "\x07TSOD\x01", "\x07TSOP\x01"} {
		for _, floor := range []uint64{0, ^uint64(0)} {
			require.ErrorIs(t, validateBootstrapAdmissionPayload(raftadmission.Allocator,
				binary.BigEndian.AppendUint64([]byte(header), floor)), errBootstrapAdmissionPayload)
		}
	}
	for _, subtype := range []byte{0, 9, 255} {
		require.ErrorIs(t, validateBootstrapAdmissionPayload(raftadmission.Data, []byte{bootstrapPayloadBackup, subtype}), errBootstrapAdmissionPayload)
	}
	command, err := kv.EncodeReplicatedBackupCommand(kv.ReplicatedBackupCommand{Operation: kv.ReplicatedBackupAllocate, Identity: kv.BackupIdentity{Epoch: 1}})
	require.NoError(t, err)
	for _, offset := range []int{1, 2} {
		invalid := bytes.Clone(command)
		invalid[offset] = 255
		require.ErrorIs(t, validateBootstrapAdmissionPayload(raftadmission.Data, invalid), errBootstrapAdmissionPayload)
	}
}

func TestBootstrapAdmissionPayloadRejectsOpaqueEncryptedNativeFrame(t *testing.T) {
	// The same version byte as a KV batch must not grant permission to an
	// envelope whose contents this keyless API cannot authenticate/decrypt.
	envelope := encryption.Envelope{Version: encryption.EnvelopeVersionV1, KeyID: 1, Body: make([]byte, encryption.TagSize)}
	encoded, err := envelope.Encode()
	require.NoError(t, err)
	for _, lane := range bootstrapAdmissionKinds() {
		require.ErrorIs(t, validateBootstrapAdmissionPayload(lane, encoded), errBootstrapAdmissionPayload)
	}
}

func FuzzBootstrapAdmissionPayload(f *testing.F) {
	for _, tc := range bootstrapPayloadFixtures(f) {
		for _, lane := range tc.kinds {
			f.Add(byte(lane), tc.payload)
		}
	}
	f.Fuzz(func(t *testing.T, kind byte, payload []byte) {
		original := bytes.Clone(payload)
		err := validateBootstrapAdmissionPayload(raftadmission.Kind(kind), payload)
		require.Equal(t, original, payload)
		if err == nil {
			require.NotContains(t, []byte{0, byte(raftadmission.Rotation), 255}, kind, "unexpected open lane %d", kind)
			if raftadmission.Kind(kind) == raftadmission.Recovery {
				_, matched, parseErr := raftbootstrap.ParseActivationCommand(payload)
				require.True(t, matched)
				require.NoError(t, parseErr)
			}
		}
	})
}
