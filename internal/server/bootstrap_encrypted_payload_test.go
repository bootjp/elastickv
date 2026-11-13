package server

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"slices"
	"sync/atomic"
	"testing"

	"github.com/bootjp/elastickv/internal/clusterbootstrap"
	"github.com/bootjp/elastickv/internal/encryption"
	"github.com/bootjp/elastickv/internal/encryption/encryptiontest"
	"github.com/bootjp/elastickv/internal/encryption/fsmwire"
	"github.com/bootjp/elastickv/internal/encryptionwiring"
	"github.com/bootjp/elastickv/internal/raftadmission"
	"github.com/bootjp/elastickv/internal/raftbootstrap"
	"github.com/bootjp/elastickv/kv"
	"github.com/stretchr/testify/require"
)

const bootstrapEncryptedTestKey uint32 = 4

func bootstrapEncryptedPayloadFixture(t *testing.T, group uint64, grants ...clusterbootstrap.BootstrapRotationPayloadGrant) (clusterbootstrap.BootstrapPayloadProjection, kv.RaftPayloadWrapper) {
	t.Helper()
	cipher := encryptiontest.NewCipher(t, bootstrapEncryptedTestKey)
	var cutover atomic.Uint64
	runtime, err := encryptionwiring.NewRaftEnvelopeRuntime(cipher, encryptiontest.NewNonceFactory(t), 1, bootstrapEncryptedTestKey, &cutover, 1, 42, nil)
	require.NoError(t, err)
	shard := &kv.ShardGroup{}
	runtime.AttachGroup(group, shard)
	projection, err := clusterbootstrap.NewBootstrapPayloadProjection(group, 1, cipher, bootstrapEncryptedTestKey, true, grants...)
	require.NoError(t, err)
	require.NotNil(t, shard.RaftPayloadWrap())
	return projection, shard.RaftPayloadWrap()
}

func bootstrapFixturePayloadCluster(t *testing.T, payload []byte) raftbootstrap.UUID {
	t.Helper()
	activation, matched, err := raftbootstrap.ParseActivationCommand(payload)
	require.NoError(t, err)
	if matched {
		return activation.Witness.Cluster
	}
	return raftbootstrap.UUID{1}
}

func TestBootstrapEncryptedPayloadUsesRealWrapperAndStrictLane(t *testing.T) {
	t.Parallel()
	for _, fixture := range bootstrapPayloadFixtures(t) {
		t.Run(fixture.name, func(t *testing.T) {
			projection, wrap := bootstrapEncryptedPayloadFixture(t, 17)
			final, err := wrap(fixture.payload)
			require.NoError(t, err)
			original := bytes.Clone(final)
			cluster := bootstrapFixturePayloadCluster(t, fixture.payload)
			for _, kind := range bootstrapAdmissionKinds() {
				validated, err := clusterbootstrap.ValidateBootstrapFinalPayload(cluster, kind, 17, final, projection)
				if slices.Contains(fixture.kinds, kind) {
					require.NoError(t, err, "kind %d", kind)
					require.Equal(t, sha256.Sum256(final), validated.FinalDigest)
					require.NotEqual(t, sha256.Sum256(fixture.payload), validated.FinalDigest)
					require.Equal(t, sha256.Sum256(fixture.payload), validated.PlaintextDigest)
					require.Equal(t, projection.Generation, validated.ProjectionGeneration)
				} else {
					require.ErrorIs(t, err, clusterbootstrap.ErrBootstrapAdmissionPayload, "kind %d", kind)
					require.Zero(t, validated)
				}
			}
			require.Equal(t, original, final)
		})
	}
}

func TestBootstrapEncryptedPayloadRejectsTamperedOrIncompleteEnvelope(t *testing.T) {
	t.Parallel()
	projection, wrap := bootstrapEncryptedPayloadFixture(t, 17)
	final, err := wrap(bootstrapPayloadProto(t, clusterbootstrap.BootstrapPayloadSingle, bootstrapPayloadRequest()))
	require.NoError(t, err)
	for _, offset := range []int{0, 1, 2, encryption.HeaderAADSize, encryption.HeaderSize, len(final) - 1} {
		bad := bytes.Clone(final)
		bad[offset] ^= 1
		validated, err := clusterbootstrap.ValidateBootstrapFinalPayload(raftbootstrap.UUID{1}, raftadmission.Data, 17, bad, projection)
		require.ErrorIs(t, err, clusterbootstrap.ErrBootstrapAdmissionPayload, "offset %d", offset)
		require.Zero(t, validated)
	}
	for length := range len(final) {
		_, err := clusterbootstrap.ValidateBootstrapFinalPayload(raftbootstrap.UUID{1}, raftadmission.Data, 17, final[:length], projection)
		require.ErrorIs(t, err, clusterbootstrap.ErrBootstrapAdmissionPayload, "length %d", length)
	}
	_, err = clusterbootstrap.ValidateBootstrapFinalPayload(raftbootstrap.UUID{1}, raftadmission.Data, 17, append(bytes.Clone(final), 0), projection)
	require.ErrorIs(t, err, encryption.ErrIntegrity)
}

func TestBootstrapEncryptedPayloadAuthenticatesThenParsesCompleteInner(t *testing.T) {
	t.Parallel()
	projection, wrap := bootstrapEncryptedPayloadFixture(t, 17)
	plain := bootstrapPayloadProto(t, clusterbootstrap.BootstrapPayloadSingle, bootstrapPayloadRequest())
	for _, payload := range [][]byte{{0xff}, append(bytes.Clone(plain), 0), {fsmwire.OpRotation, 1}, {0x06}, {0x07}} {
		final, err := wrap(payload)
		require.NoError(t, err)
		_, err = clusterbootstrap.ValidateBootstrapFinalPayload(raftbootstrap.UUID{1}, raftadmission.Data, 17, final, projection)
		require.ErrorIs(t, err, clusterbootstrap.ErrBootstrapAdmissionPayload)
	}
	_, err := clusterbootstrap.ValidateBootstrapFinalPayload(raftbootstrap.UUID{1}, raftadmission.Data, 17, plain, projection)
	require.ErrorIs(t, err, clusterbootstrap.ErrBootstrapAdmissionPayload, "active envelope mode must never fall back to plaintext")
	inactive, err := clusterbootstrap.NewBootstrapPayloadProjection(17, 1, nil, 0, false)
	require.NoError(t, err)
	_, err = clusterbootstrap.ValidateBootstrapFinalPayload(raftbootstrap.UUID{1}, raftadmission.Data, 17, plain, inactive)
	require.NoError(t, err)
	final, err := wrap(plain)
	require.NoError(t, err)
	_, err = clusterbootstrap.ValidateBootstrapFinalPayload(raftbootstrap.UUID{1}, raftadmission.Data, 17, final, inactive)
	require.ErrorIs(t, err, clusterbootstrap.ErrBootstrapAdmissionPayload)
}

func TestBootstrapEncryptedPayloadRejectsWrongProjectionKeysAndScope(t *testing.T) {
	t.Parallel()
	projection, wrap := bootstrapEncryptedPayloadFixture(t, 17)
	final, err := wrap(bootstrapPayloadGuarded(t, raftbootstrap.ControlGuardedWatermark))
	require.NoError(t, err)
	_, err = clusterbootstrap.ValidateBootstrapFinalPayload(raftbootstrap.UUID{1}, raftadmission.Catalog, 18, final, projection)
	require.ErrorIs(t, err, clusterbootstrap.ErrBootstrapPayloadProjection)
	otherGroup := projection
	otherGroup.Group = 18
	_, err = clusterbootstrap.ValidateBootstrapFinalPayload(raftbootstrap.UUID{1}, raftadmission.Catalog, 18, final, otherGroup)
	require.ErrorIs(t, err, raftadmission.ErrClosed, "authenticated ciphertext does not override its inner group witness")
	_, err = clusterbootstrap.ValidateBootstrapFinalPayload(raftbootstrap.UUID{2}, raftadmission.Catalog, 17, final, projection)
	require.ErrorIs(t, err, raftadmission.ErrClosed)
	wrongCipher := projection
	wrongCipher.RaftCipher = encryptiontest.NewCipher(t, bootstrapEncryptedTestKey)
	_, err = clusterbootstrap.ValidateBootstrapFinalPayload(raftbootstrap.UUID{1}, raftadmission.Catalog, 17, final, wrongCipher)
	require.ErrorIs(t, err, encryption.ErrIntegrity)
	unknown := bytes.Clone(final)
	const unknownKeyID uint32 = 99
	binary.BigEndian.PutUint32(unknown[2:encryption.HeaderAADSize], unknownKeyID)
	unknownProjection := projection
	unknownProjection.RaftKeyID = unknownKeyID
	_, err = clusterbootstrap.ValidateBootstrapFinalPayload(raftbootstrap.UUID{1}, raftadmission.Catalog, 17, unknown, unknownProjection)
	require.ErrorIs(t, err, encryption.ErrUnknownKeyID)
}

func TestBootstrapEncryptedPayloadRejectsLoadedButInactiveKey(t *testing.T) {
	t.Parallel()
	projection, wrap := bootstrapEncryptedPayloadFixture(t, 17)
	final, err := wrap(bootstrapPayloadProto(t, clusterbootstrap.BootstrapPayloadSingle, bootstrapPayloadRequest()))
	require.NoError(t, err)
	projection.RaftKeyID++
	_, err = clusterbootstrap.ValidateBootstrapFinalPayload(raftbootstrap.UUID{1}, raftadmission.Data, 17, final, projection)
	require.ErrorIs(t, err, clusterbootstrap.ErrBootstrapPayloadProjection, "loaded historical DEKs are not current write permission")
	_, err = clusterbootstrap.NewBootstrapPayloadProjection(17, 0, nil, 0, false)
	require.ErrorIs(t, err, clusterbootstrap.ErrBootstrapPayloadProjection)
	_, err = clusterbootstrap.NewBootstrapPayloadProjection(17, 1, nil, bootstrapEncryptedTestKey, true)
	require.ErrorIs(t, err, clusterbootstrap.ErrBootstrapPayloadProjection)
	_, err = clusterbootstrap.NewBootstrapPayloadProjection(17, 1, projection.RaftCipher, 0, true)
	require.ErrorIs(t, err, clusterbootstrap.ErrBootstrapPayloadProjection)
}

func TestBootstrapEncryptedPayloadSealBindsActualCiphertext(t *testing.T) {
	t.Parallel()
	projection, wrap := bootstrapEncryptedPayloadFixture(t, 17)
	plain := binary.BigEndian.AppendUint64([]byte{clusterbootstrap.BootstrapPayloadHLC}, 1000)
	first, err := wrap(plain)
	require.NoError(t, err)
	second, err := wrap(plain)
	require.NoError(t, err)
	a, err := clusterbootstrap.ValidateBootstrapFinalPayload(raftbootstrap.UUID{1}, raftadmission.HLC, 17, first, projection)
	require.NoError(t, err)
	b, err := clusterbootstrap.ValidateBootstrapFinalPayload(raftbootstrap.UUID{1}, raftadmission.HLC, 17, second, projection)
	require.NoError(t, err)
	require.NotEqual(t, a.FinalDigest, b.FinalDigest, "different actual nonces/ciphertexts must not share a plaintext-only seal")
	projection.Generation++
	c, err := clusterbootstrap.ValidateBootstrapFinalPayload(raftbootstrap.UUID{1}, raftadmission.HLC, 17, first, projection)
	require.NoError(t, err)
	require.Equal(t, a.FinalDigest, c.FinalDigest)
	require.NotEqual(t, a.ProjectionGeneration, c.ProjectionGeneration, "A must separately bind the installed generation")
}

func TestBootstrapEncryptedRegistrationRejectsInvalidIdentity(t *testing.T) {
	t.Parallel()
	projection, wrap := bootstrapEncryptedPayloadFixture(t, 17)
	for _, registration := range []fsmwire.RegistrationPayload{{FullNodeID: 42}, {DEKID: 4}} {
		plain := append([]byte{fsmwire.OpRegistration}, fsmwire.EncodeRegistration(registration)...)
		final, err := wrap(plain)
		require.NoError(t, err)
		_, err = clusterbootstrap.ValidateBootstrapFinalPayload(raftbootstrap.UUID{1}, raftadmission.Registration, 17, final, projection)
		require.ErrorIs(t, err, clusterbootstrap.ErrBootstrapAdmissionPayload)
	}
}

func TestBootstrapEncryptedPayloadRejectsWrongAADPurpose(t *testing.T) {
	t.Parallel()
	projection, _ := bootstrapEncryptedPayloadFixture(t, 17)
	nonce, err := encryptiontest.NewNonceFactory(t).Next()
	require.NoError(t, err)
	aad := encryption.BuildRaftAAD(encryption.EnvelopeVersionV1, bootstrapEncryptedTestKey)
	aad[0] = 'S'
	body, err := projection.RaftCipher.Encrypt(bootstrapPayloadProto(t, clusterbootstrap.BootstrapPayloadSingle, bootstrapPayloadRequest()), aad, bootstrapEncryptedTestKey, nonce[:])
	require.NoError(t, err)
	envelope := encryption.Envelope{Version: encryption.EnvelopeVersionV1, KeyID: bootstrapEncryptedTestKey, Nonce: nonce, Body: body}
	final, err := envelope.Encode()
	require.NoError(t, err)
	_, err = clusterbootstrap.ValidateBootstrapFinalPayload(raftbootstrap.UUID{1}, raftadmission.Data, 17, final, projection)
	require.ErrorIs(t, err, encryption.ErrIntegrity)
}

func FuzzBootstrapEncryptedPayload(f *testing.F) {
	// Stable test material keeps the same fuzz input meaningful in every
	// worker process. This key is never used outside this isolated fixture.
	keys := encryption.NewKeystore()
	require.NoError(f, keys.Set(bootstrapEncryptedTestKey, bytes.Repeat([]byte{4}, encryption.KeySize)))
	cipher, err := encryption.NewCipher(keys)
	require.NoError(f, err)
	projection, err := clusterbootstrap.NewBootstrapPayloadProjection(17, 1, cipher, bootstrapEncryptedTestKey, true)
	require.NoError(f, err)
	nonce, err := encryptiontest.NewNonceFactory(f).Next()
	require.NoError(f, err)
	valid, err := encryption.WrapRaftPayload(cipher, bootstrapEncryptedTestKey, nonce[:], bootstrapPayloadProto(f, clusterbootstrap.BootstrapPayloadSingle, bootstrapPayloadRequest()))
	require.NoError(f, err)
	f.Add(byte(raftadmission.Data), valid)
	f.Add(byte(raftadmission.Rotation), []byte{0xff})
	f.Fuzz(func(t *testing.T, lane byte, payload []byte) {
		original := bytes.Clone(payload)
		validated, err := clusterbootstrap.ValidateBootstrapFinalPayload(raftbootstrap.UUID{1}, raftadmission.Kind(lane), 17, payload, projection)
		require.Equal(t, original, payload)
		if err != nil {
			require.Zero(t, validated)
			return
		}
		plain, err := encryption.UnwrapRaftPayload(cipher, payload)
		require.NoError(t, err)
		require.NoError(t, clusterbootstrap.ValidateBootstrapAdmissionPayload(raftadmission.Kind(lane), plain))
		require.Equal(t, sha256.Sum256(payload), validated.FinalDigest)
		require.Equal(t, sha256.Sum256(plain), validated.PlaintextDigest)
		require.NotEqual(t, byte(raftadmission.Rotation), lane, "fixture has no owned rotation grant")
	})
}
