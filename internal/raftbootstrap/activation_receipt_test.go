package raftbootstrap

import (
	"bytes"
	"encoding/hex"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func testActivationReceipt() ActivationReceipt {
	return ActivationReceipt{
		Entry:       EntryIdentity{Cluster: UUID{1}, Index: 12, Term: 3, Digest: Digest{2}},
		Witness:     Witness{Cluster: UUID{1}, ConfigurationIndex: 8, ConfigurationDigest: Digest{3}},
		OperationID: OperationID{4}, ManifestDigest: Digest{5}, PolicyDigest: Digest{6}, Verdict: ActivationReceiptApplied,
	}
}

func uncheckedActivationReceipt(r ActivationReceipt) []byte {
	w := &encoder{limit: activationReceiptSize}
	w.appendString(activationReceiptMagic)
	w.fixed(r.Entry)
	w.fixed(r.Witness)
	w.fixed(r.OperationID)
	w.fixed(r.ManifestDigest)
	w.fixed(r.PolicyDigest)
	w.fixed(r.Verdict)
	return w.Bytes()
}

func TestActivationReceiptGoldenAndRoundTrip(t *testing.T) {
	t.Parallel()
	r := testActivationReceipt()
	encoded, err := EncodeActivationReceipt(r)
	require.NoError(t, err)
	require.Len(t, encoded, 237)
	require.Equal(t,
		"42474131"+
			"01"+strings.Repeat("00", 15)+ // Entry cluster.
			"0000000000000000"+"000000000000000c"+"0000000000000003"+
			"02"+strings.Repeat("00", 31)+ // Entry digest.
			"01"+strings.Repeat("00", 15)+ // W0 cluster.
			"0000000000000000"+"0000000000000000"+"0000000000000000"+"0000000000000008"+
			"03"+strings.Repeat("00", 31)+
			"04"+strings.Repeat("00", 15)+
			"05"+strings.Repeat("00", 31)+
			"06"+strings.Repeat("00", 31)+"01",
		hex.EncodeToString(encoded))
	parsed, err := ParseActivationReceipt(encoded)
	require.NoError(t, err)
	require.Equal(t, r, parsed)
	encoded[len(activationReceiptMagic)]++
	require.Equal(t, r, parsed, "parsed fields must not alias input")
}

func TestActivationReceiptVerdicts(t *testing.T) {
	t.Parallel()
	for _, verdict := range []ActivationReceiptVerdict{ActivationReceiptApplied, ActivationReceiptRejectedStale, ActivationReceiptAlreadyActive} {
		r := testActivationReceipt()
		r.Verdict = verdict
		if verdict == ActivationReceiptRejectedStale {
			r.PolicyDigest = Digest{}
		}
		b, err := EncodeActivationReceipt(r)
		require.NoError(t, err)
		parsed, err := ParseActivationReceipt(b)
		require.NoError(t, err)
		require.Equal(t, r, parsed)
		r.PolicyDigest = Digest{7}
		if verdict != ActivationReceiptRejectedStale {
			r.PolicyDigest = Digest{}
		}
		_, err = EncodeActivationReceipt(r)
		require.ErrorIs(t, err, ErrInvalid)
		_, err = ParseActivationReceipt(uncheckedActivationReceipt(r))
		require.ErrorIs(t, err, ErrInvalid)
	}
}

func TestActivationReceiptRejectsInvalidFields(t *testing.T) {
	t.Parallel()
	cases := map[string]func(*ActivationReceipt){
		"entry cluster":          func(r *ActivationReceipt) { r.Entry.Cluster = UUID{} },
		"foreign cluster":        func(r *ActivationReceipt) { r.Entry.Cluster[0]++ },
		"foreign group":          func(r *ActivationReceipt) { r.Entry.Group++ },
		"entry index":            func(r *ActivationReceipt) { r.Entry.Index = 0 },
		"entry term":             func(r *ActivationReceipt) { r.Entry.Term = 0 },
		"entry digest":           func(r *ActivationReceipt) { r.Entry.Digest = Digest{} },
		"active index":           func(r *ActivationReceipt) { r.Witness.ActivationIndex = 1 },
		"active generation":      func(r *ActivationReceipt) { r.Witness.Generation = 1 },
		"configuration index":    func(r *ActivationReceipt) { r.Witness.ConfigurationIndex = 0 },
		"configuration digest":   func(r *ActivationReceipt) { r.Witness.ConfigurationDigest = Digest{} },
		"operation":              func(r *ActivationReceipt) { r.OperationID = OperationID{} },
		"manifest":               func(r *ActivationReceipt) { r.ManifestDigest = Digest{} },
		"policy":                 func(r *ActivationReceipt) { r.PolicyDigest = Digest{} },
		"zero verdict":           func(r *ActivationReceipt) { r.Verdict = 0 },
		"unknown verdict":        func(r *ActivationReceipt) { r.Verdict = 255 },
		"configuration at entry": func(r *ActivationReceipt) { r.Witness.ConfigurationIndex = r.Entry.Index },
		"future configuration":   func(r *ActivationReceipt) { r.Witness.ConfigurationIndex = r.Entry.Index + 1 },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			r := testActivationReceipt()
			mutate(&r)
			_, err := EncodeActivationReceipt(r)
			require.ErrorIs(t, err, ErrInvalid)
			_, err = ParseActivationReceipt(uncheckedActivationReceipt(r))
			require.ErrorIs(t, err, ErrInvalid)
		})
	}
}

func TestActivationReceiptRejectionsCanRecordFutureCut(t *testing.T) {
	t.Parallel()
	for _, verdict := range []ActivationReceiptVerdict{ActivationReceiptRejectedStale, ActivationReceiptAlreadyActive} {
		r := testActivationReceipt()
		r.Verdict = verdict
		r.Witness.ConfigurationIndex = r.Entry.Index + 1
		if verdict == ActivationReceiptRejectedStale {
			r.PolicyDigest = Digest{}
		}
		b, err := EncodeActivationReceipt(r)
		require.NoError(t, err, "rejected command must still occupy its committed entry slot")
		parsed, err := ParseActivationReceipt(b)
		require.NoError(t, err)
		require.Equal(t, r, parsed)
	}
}

func TestActivationReceiptStrictFrames(t *testing.T) {
	t.Parallel()
	r := testActivationReceipt()
	b, err := EncodeActivationReceipt(r)
	require.NoError(t, err)
	for i := range b {
		_, err := ParseActivationReceipt(b[:i])
		require.ErrorIs(t, err, ErrInvalid)
	}
	for _, bad := range [][]byte{append(bytes.Clone(b), 0), make([]byte, MaxFileSize+1)} {
		_, err := ParseActivationReceipt(bad)
		require.ErrorIs(t, err, ErrInvalid)
	}
	for i := range activationReceiptMagic {
		bad := bytes.Clone(b)
		bad[i]++
		_, err := ParseActivationReceipt(bad)
		require.ErrorIs(t, err, ErrInvalid)
	}
	bad := bytes.Clone(b)
	bad[len(bad)-1] = 0
	_, err = ParseActivationReceipt(bad)
	require.ErrorIs(t, err, ErrInvalid)
	bad = bytes.Clone(b)
	clear(bad[len(activationReceiptMagic)+entryIdentitySize+WitnessSize : len(activationReceiptMagic)+entryIdentitySize+WitnessSize+operationIDSize])
	_, err = ParseActivationReceipt(bad)
	require.ErrorIs(t, err, ErrInvalid)
	_, err = ParseGuardedReceipt(b)
	require.ErrorIs(t, err, ErrInvalid)
	guarded := GuardedReceipt{Entry: r.Entry, Witness: r.Witness, Kind: ControlGuardedCatalog, Verdict: GuardedReceiptApplied, OperationID: r.OperationID}
	guarded.Witness.ActivationIndex, guarded.Witness.Generation = 9, 1
	g, err := EncodeGuardedReceipt(guarded)
	require.NoError(t, err)
	_, err = ParseActivationReceipt(g)
	require.ErrorIs(t, err, ErrInvalid)
}

func TestActivationReceiptUsesSameSlotAsGuardedReceipt(t *testing.T) {
	t.Parallel()
	r := testActivationReceipt()
	key, err := ReceiptKey(r.Entry)
	require.NoError(t, err)
	conflict := r
	conflict.Entry.Term++
	conflict.Entry.Digest[0]++
	otherKey, err := ReceiptKey(conflict.Entry)
	require.NoError(t, err)
	require.Equal(t, key, otherKey)
	a, err := EncodeActivationReceipt(r)
	require.NoError(t, err)
	b, err := EncodeActivationReceipt(conflict)
	require.NoError(t, err)
	require.NotEqual(t, a, b, "same slot must require full identity comparison before replay")
	guarded := GuardedReceipt{Entry: r.Entry, Witness: r.Witness, Kind: ControlGuardedCatalog, Verdict: GuardedReceiptApplied, OperationID: r.OperationID}
	guarded.Witness.ActivationIndex, guarded.Witness.Generation = 9, 1
	g, err := EncodeGuardedReceipt(guarded)
	require.NoError(t, err)
	parsed, err := ParseGuardedReceipt(g)
	require.NoError(t, err)
	guardedKey, err := ReceiptKey(parsed.Entry)
	require.NoError(t, err)
	require.Equal(t, key, guardedKey)
}

func FuzzActivationReceipt(f *testing.F) {
	b, err := EncodeActivationReceipt(testActivationReceipt())
	require.NoError(f, err)
	f.Add(b)
	f.Add([]byte(activationReceiptMagic))
	f.Fuzz(func(t *testing.T, data []byte) {
		r, err := ParseActivationReceipt(data)
		if err == nil {
			canonical, err := EncodeActivationReceipt(r)
			require.NoError(t, err)
			require.Equal(t, data, canonical)
		}
	})
}
