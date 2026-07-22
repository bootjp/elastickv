package store

import (
	"bytes"
	"context"
	"crypto/rand"
	"testing"

	"github.com/bootjp/elastickv/internal/encryption"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/v2"
	"github.com/golang/snappy"
	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"
)

func TestEncryption_CompressesOnlyWhenSmaller(t *testing.T) {
	t.Parallel()
	incompressible := make([]byte, 4096)
	if _, err := rand.Read(incompressible); err != nil {
		t.Fatalf("rand.Read incompressible value: %v", err)
	}
	cases := []struct {
		name       string
		value      []byte
		compressed bool
	}{
		{name: "compressible", value: bytes.Repeat([]byte("json-field:"), 512), compressed: true},
		{name: "below threshold", value: bytes.Repeat([]byte("a"), minEncryptionCompressionSize-1), compressed: false},
		{name: "at threshold", value: bytes.Repeat([]byte("a"), minEncryptionCompressionSize), compressed: true},
		{name: "empty", value: []byte{}, compressed: false},
		{name: "small", value: []byte("value"), compressed: false},
		{name: "high entropy", value: incompressible, compressed: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newEncryptedStoreFixture(t, 101)
			key := []byte("compression/" + tc.name)
			if err := f.mvcc.PutAt(context.Background(), key, tc.value, 100, 0); err != nil {
				t.Fatalf("PutAt: %v", err)
			}

			var version, flag byte
			f.tamperPebbleValue(t, key, 100, func(raw []byte) []byte {
				sv, err := decodeValue(raw)
				if err != nil {
					t.Fatalf("decodeValue: %v", err)
				}
				env, err := encryption.DecodeEnvelope(sv.Value)
				if err != nil {
					t.Fatalf("DecodeEnvelope: %v", err)
				}
				version = env.Version
				flag = env.Flag
				return raw
			})
			require.Equal(t, tc.compressed, flag&encryption.FlagCompressed != 0)
			if tc.compressed {
				require.Equal(t, encryption.EnvelopeVersionV2, version)
			} else {
				require.Equal(t, encryption.EnvelopeVersionV1, version)
			}

			got, err := f.mvcc.GetAt(context.Background(), key, 100)
			require.NoError(t, err)
			require.Equal(t, tc.value, got)
		})
	}
}

func TestEncryption_StorageEnvelopeV2CapabilityGate(t *testing.T) {
	t.Parallel()
	ks := encryption.NewKeystore()
	dek := make([]byte, encryption.KeySize)
	if _, err := rand.Read(dek); err != nil {
		t.Fatalf("rand.Read DEK: %v", err)
	}
	const keyID uint32 = 107
	if err := ks.Set(keyID, dek); err != nil {
		t.Fatalf("Keystore.Set: %v", err)
	}
	cipher, err := encryption.NewCipher(ks)
	if err != nil {
		t.Fatalf("NewCipher: %v", err)
	}
	v2Active := false
	s := &pebbleStore{
		cipher:                  cipher,
		nonceFactory:            NewCounterNonceFactory(0x0102, 0x0304),
		activeStorageKeyID:      func() (uint32, bool) { return keyID, true },
		storageEnvelopeV2Active: func() bool { return v2Active },
	}
	plaintext := bytes.Repeat([]byte("compressible"), 512)
	pebbleKey := encodeKey([]byte("capability-gate"), 100)

	body, _, err := s.encryptForKey(pebbleKey, plaintext, 0, false)
	require.NoError(t, err)
	env, err := encryption.DecodeEnvelope(body)
	require.NoError(t, err)
	require.Equal(t, encryption.EnvelopeVersionV1, env.Version)
	require.Zero(t, env.Flag)

	v2Active = true
	body, _, err = s.encryptForKey(pebbleKey, plaintext, 0, false)
	require.NoError(t, err)
	env, err = encryption.DecodeEnvelope(body)
	require.NoError(t, err)
	require.Equal(t, encryption.EnvelopeVersionV2, env.Version)
	require.Equal(t, encryption.FlagCompressed, env.Flag)
}

func TestEncryption_CompressionFlagTamperRejected(t *testing.T) {
	t.Parallel()
	f := newEncryptedStoreFixture(t, 102)
	key := []byte("compressed-tamper")
	value := bytes.Repeat([]byte("compress-me"), 512)
	if err := f.mvcc.PutAt(context.Background(), key, value, 100, 0); err != nil {
		t.Fatalf("PutAt: %v", err)
	}
	f.tamperPebbleValue(t, key, 100, func(raw []byte) []byte {
		// value header (9) + envelope version (1) precede the flag.
		raw[valueHeaderSize+1] &^= encryption.FlagCompressed
		return raw
	})
	if _, err := f.mvcc.GetAt(context.Background(), key, 100); !errors.Is(err, ErrEncryptedReadIntegrity) {
		t.Fatalf("compression flag tamper: expected ErrEncryptedReadIntegrity, got %v", err)
	}
}

func TestEncryption_CompressedEnvelopeRebadgeRejected(t *testing.T) {
	t.Parallel()
	f := newEncryptedStoreFixture(t, 104)
	key := []byte("compressed-rebadge")
	value := bytes.Repeat([]byte("sensitive-json-field"), 512)
	if err := f.mvcc.PutAt(context.Background(), key, value, 100, 0); err != nil {
		t.Fatalf("PutAt: %v", err)
	}
	f.tamperPebbleValue(t, key, 100, func(raw []byte) []byte {
		raw[0] &^= encStateMask
		return raw
	})
	if _, err := f.mvcc.GetAt(context.Background(), key, 100); !errors.Is(err, ErrEncryptedReadIntegrity) {
		t.Fatalf("compressed rebadge: expected ErrEncryptedReadIntegrity, got %v", err)
	}
}

func TestEncryption_RejectsAuthenticatedMalformedSnappy(t *testing.T) {
	t.Parallel()
	f := newEncryptedStoreFixture(t, 103)
	key := []byte("malformed-snappy")
	const commitTS uint64 = 100
	pebbleKey := encodeKey(key, commitTS)
	var header [valueHeaderSize]byte
	writeValueHeaderBytes(header[:], false, 0, encStateEncrypted)
	aad := buildStorageAAD(encryption.EnvelopeVersionV2, encryption.FlagCompressed, f.keyID, header[:], pebbleKey)
	var nonce [encryption.NonceSize]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		t.Fatalf("rand.Read nonce: %v", err)
	}
	body, err := f.cipher.Encrypt([]byte{0xff}, aad, f.keyID, nonce[:])
	if err != nil {
		t.Fatalf("Encrypt malformed payload: %v", err)
	}
	envelopeBytes, err := (&encryption.Envelope{
		Version: encryption.EnvelopeVersionV2,
		Flag:    encryption.FlagCompressed,
		KeyID:   f.keyID,
		Nonce:   nonce,
		Body:    body,
	}).Encode()
	if err != nil {
		t.Fatalf("Envelope.Encode: %v", err)
	}

	f.closeIfOpen(t)
	pdb, err := pebble.Open(f.dir, &pebble.Options{})
	if err != nil {
		t.Fatalf("pebble.Open: %v", err)
	}
	if err := pdb.Set(pebbleKey, encodeValue(envelopeBytes, false, 0, encStateEncrypted), pebble.Sync); err != nil {
		_ = pdb.Close()
		t.Fatalf("pdb.Set: %v", err)
	}
	if err := pdb.Close(); err != nil {
		t.Fatalf("pdb.Close: %v", err)
	}
	f.reopen(t)

	if _, err := f.mvcc.GetAt(context.Background(), key, commitTS); !errors.Is(err, ErrEncryptedReadCompression) {
		t.Fatalf("malformed authenticated Snappy: expected ErrEncryptedReadCompression, got %v", err)
	}
	if err := f.mvcc.DeletePrefixAt(context.Background(), []byte("malformed-"), nil, commitTS+1); err != nil {
		t.Fatalf("visibility-only prefix delete expanded malformed Snappy: %v", err)
	}
}

func TestEncryption_RejectsAuthenticatedOversizedSnappyBeforeDecode(t *testing.T) {
	previousMax := maxSnapshotValueSize
	maxSnapshotValueSize = 64
	t.Cleanup(func() { maxSnapshotValueSize = previousMax })

	f := newEncryptedStoreFixture(t, 106)
	key := []byte("oversized-snappy")
	const commitTS uint64 = 100
	pebbleKey := encodeKey(key, commitTS)
	var header [valueHeaderSize]byte
	writeValueHeaderBytes(header[:], false, 0, encStateEncrypted)
	aad := buildStorageAAD(encryption.EnvelopeVersionV2, encryption.FlagCompressed, f.keyID, header[:], pebbleKey)
	compressed := snappy.Encode(nil, bytes.Repeat([]byte("a"), maxSnapshotValueSize+1))
	var nonce [encryption.NonceSize]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		t.Fatalf("rand.Read nonce: %v", err)
	}
	body, err := f.cipher.Encrypt(compressed, aad, f.keyID, nonce[:])
	if err != nil {
		t.Fatalf("Encrypt oversized payload: %v", err)
	}
	envelopeBytes, err := (&encryption.Envelope{
		Version: encryption.EnvelopeVersionV2,
		Flag:    encryption.FlagCompressed,
		KeyID:   f.keyID,
		Nonce:   nonce,
		Body:    body,
	}).Encode()
	if err != nil {
		t.Fatalf("Envelope.Encode: %v", err)
	}

	f.closeIfOpen(t)
	pdb, err := pebble.Open(f.dir, &pebble.Options{})
	if err != nil {
		t.Fatalf("pebble.Open: %v", err)
	}
	if err := pdb.Set(pebbleKey, encodeValue(envelopeBytes, false, 0, encStateEncrypted), pebble.Sync); err != nil {
		_ = pdb.Close()
		t.Fatalf("pdb.Set: %v", err)
	}
	if err := pdb.Close(); err != nil {
		t.Fatalf("pdb.Close: %v", err)
	}
	f.reopen(t)

	if _, err := f.mvcc.GetAt(context.Background(), key, commitTS); !errors.Is(err, ErrEncryptedReadCompression) {
		t.Fatalf("oversized authenticated Snappy: expected ErrEncryptedReadCompression, got %v", err)
	}
}

func TestCompressionRoundTripProperty(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		plaintext := rapid.SliceOfN(rapid.Byte(), 0, 16<<10).Draw(t, "plaintext")
		payload, flag := compressForEncryption(plaintext)
		if flag == 0 {
			if !bytes.Equal(payload, plaintext) {
				t.Fatalf("uncompressed payload mismatch")
			}
			return
		}
		if flag != encryption.FlagCompressed {
			t.Fatalf("unexpected compression flag %#x", flag)
		}
		if len(payload) >= len(plaintext) {
			t.Fatalf("compressed payload grew: got=%d original=%d", len(payload), len(plaintext))
		}
		got, err := snappy.Decode(nil, payload)
		if err != nil {
			t.Fatalf("snappy.Decode: %v", err)
		}
		if !bytes.Equal(got, plaintext) {
			t.Fatalf("round-trip mismatch")
		}
	})
}

func TestEncryptedStoreCompressionRoundTripProperty(t *testing.T) {
	t.Parallel()
	ks := encryption.NewKeystore()
	dek := make([]byte, encryption.KeySize)
	if _, err := rand.Read(dek); err != nil {
		t.Fatalf("rand.Read DEK: %v", err)
	}
	const keyID uint32 = 105
	if err := ks.Set(keyID, dek); err != nil {
		t.Fatalf("Keystore.Set: %v", err)
	}
	cipher, err := encryption.NewCipher(ks)
	if err != nil {
		t.Fatalf("NewCipher: %v", err)
	}
	s := &pebbleStore{
		cipher:             cipher,
		nonceFactory:       NewCounterNonceFactory(0x0102, 0x0304),
		activeStorageKeyID: func() (uint32, bool) { return keyID, true },
	}
	rapid.Check(t, func(t *rapid.T) {
		key := rapid.SliceOfN(rapid.Byte(), 0, 256).Draw(t, "key")
		plaintext := rapid.SliceOfN(rapid.Byte(), 0, 16<<10).Draw(t, "plaintext")
		expireAt := rapid.Uint64().Draw(t, "expireAt")
		pebbleKey := encodeKey(key, 100)
		body, encState, err := s.encryptForKey(pebbleKey, plaintext, expireAt, false)
		if err != nil {
			t.Fatalf("encryptForKey: %v", err)
		}
		if encState != encStateEncrypted {
			t.Fatalf("encryption state=%#x, want encrypted", encState)
		}
		got, err := s.decryptForKey(pebbleKey, storedValue{
			Value:    body,
			EncState: encState,
			ExpireAt: expireAt,
		}, body)
		if err != nil {
			t.Fatalf("decryptForKey: %v", err)
		}
		if !bytes.Equal(got, plaintext) {
			t.Fatalf("round-trip mismatch")
		}
	})
}

func TestPebbleCompressionPolicy(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name     string
		disable  bool
		wantName string
	}{
		{name: "legacy defaults", disable: false, wantName: "Snappy"},
		{name: "encrypted store", disable: true, wantName: "NoCompression"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			opts, cache := defaultPebbleOptionsWithCache(tc.disable)
			t.Cleanup(cache.Unref)
			for level, levelOpts := range opts.Levels {
				profile := levelOpts.Compression()
				if tc.disable {
					require.Equalf(t, tc.wantName, profile.Name, "level %d", level)
					continue
				}
				require.NotEqualf(t, "NoCompression", profile.Name, "level %d", level)
			}
		})
	}
}
