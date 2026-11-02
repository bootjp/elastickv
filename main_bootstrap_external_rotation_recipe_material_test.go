package main

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/bootjp/elastickv/internal/encryption"
	"github.com/bootjp/elastickv/internal/encryption/kek"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/stretchr/testify/require"
)

func newExternalRotationMaterialFixture(t *testing.T) *rotationRecipeFixture {
	t.Helper()
	f := newRotationRecipeFixture(t)
	path := filepath.Join(t.TempDir(), "kek")
	require.NoError(t, os.WriteFile(path, bytes.Repeat([]byte{33}, encryption.KeySize), 0o600))
	wrapper, err := kek.NewFileWrapper(path)
	require.NoError(t, err)
	f.cfg.Wrapper = wrapper
	sc, err := encryption.ReadSidecar(f.path)
	require.NoError(t, err)
	for _, id := range []string{"1", "2"} {
		key := sc.Keys[id]
		material := bytes.Clone(key.Wrapped[:encryption.KeySize])
		key.Wrapped, err = wrapper.Wrap(material)
		zeroBytes(material)
		require.NoError(t, err)
		sc.Keys[id] = key
	}
	f.loaded[1], f.loaded[2] = bytes.Clone(sc.Keys["1"].Wrapped), bytes.Clone(sc.Keys["2"].Wrapped)
	require.NoError(t, encryption.WriteSidecar(f.path, sc))
	return f
}

type externalRotationObservedUnwrap struct {
	kek.Wrapper
	plaintext [][]byte
	failAt    int
}

func (w *externalRotationObservedUnwrap) Unwrap(wrapped []byte) ([]byte, error) {
	material, err := w.Wrapper.Unwrap(wrapped)
	w.plaintext = append(w.plaintext, material)
	if w.failAt == len(w.plaintext) {
		return material, errors.New("partial unwrap failure")
	}
	return material, err
}

func TestBootstrapExternalRotationRecipeMaterialPlaintextCleared(t *testing.T) {
	for _, tc := range []struct {
		name   string
		id     uint32
		failAt int
		valid  bool
	}{
		{"exact_active_retry", 1, 0, true},
		{"reused_material", 20, 0, false},
		{"request_partial_failure", 20, 1, false},
		{"historical_partial_failure", 20, 2, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newExternalRotationMaterialFixture(t)
			sc, err := encryption.ReadSidecar(f.path)
			require.NoError(t, err)
			req := externalRotationTestRequest(f)
			req.NewDekId, req.WrappedNewDek = tc.id, bytes.Clone(sc.Keys["1"].Wrapped)
			observed := &externalRotationObservedUnwrap{Wrapper: f.cfg.Wrapper, failAt: tc.failAt}
			f.cfg.Wrapper = observed
			r, err := newBootstrapExternalRotationRecipe(t.Context(), f.cfg, req)
			if tc.valid {
				require.NoError(t, err)
				receipt, runErr := r.Run(t.Context())
				require.NoError(t, runErr)
				require.True(t, receipt.Complete)
			} else {
				require.ErrorIs(t, err, errBootstrapExternalRotationRequest)
			}
			require.NotEmpty(t, observed.plaintext)
			for _, material := range observed.plaintext {
				require.Equal(t, make([]byte, len(material)), material, "temporary requested AND historical plaintext must be cleared on every return")
			}
			require.Empty(t, f.calls)
			require.Equal(t, sc.Keys["1"].Wrapped, req.WrappedNewDek, "validation cannot clear the persisted or caller-owned wrapped bytes")
		})
	}
}

func TestBootstrapExternalRotationRecipeRejectsSameIDRewrappedMaterial(t *testing.T) {
	f := newExternalRotationMaterialFixture(t)
	req := externalRotationMaterialReplay(t, f, true, false, true)
	req.NewDekId = 1
	_, err := newBootstrapExternalRotationRecipe(t.Context(), f.cfg, req)
	require.ErrorIs(t, err, errBootstrapExternalRotationRequest, "same plaintext is not an exact original request retry")
	require.Empty(t, f.calls)
}

func externalRotationMaterialReplay(t *testing.T, f *rotationRecipeFixture, samePurpose, retired, rewrapped bool) *pb.RotateDEKRequest {
	t.Helper()
	sc, err := encryption.ReadSidecar(f.path)
	require.NoError(t, err)
	wrapped := bytes.Clone(sc.Keys["1"].Wrapped)
	if rewrapped {
		material, err := f.cfg.Wrapper.Unwrap(wrapped)
		require.NoError(t, err)
		wrapped, err = f.cfg.Wrapper.Wrap(material)
		zeroBytes(material)
		require.NoError(t, err)
		require.NotEqual(t, sc.Keys["1"].Wrapped, wrapped, "randomized KEK envelopes must differ for the SAME AES key")
	}
	if retired {
		fresh := externalRotationTestRequest(f)
		fresh.NewDekId = 30
		fresh.WrappedNewDek, err = f.cfg.Wrapper.Wrap(fresh.WrappedNewDek)
		require.NoError(t, err)
		f.apply(fresh)
	}
	req := externalRotationTestRequest(f)
	req.WrappedNewDek = wrapped
	if !samePurpose {
		req.Purpose, req.ProposerLocalEpoch = pb.RotateDEKRequest_PURPOSE_RAFT, uint32(f.cfg.RaftEpoch)
	}
	return req
}

func TestBootstrapExternalRotationRecipeRejectsReusedMaterial(t *testing.T) {
	for _, tc := range []struct {
		name                            string
		samePurpose, retired, rewrapped bool
	}{
		{"cross_purpose", false, false, false},
		{"same_purpose_new_id", true, false, false},
		{"equivalent_kek_envelope", false, false, true},
		{"historical_key", false, true, false},
		{"historical_rewrapped_same_purpose", true, true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newExternalRotationMaterialFixture(t)
			req := externalRotationMaterialReplay(t, f, tc.samePurpose, tc.retired, tc.rewrapped)
			before, err := encryption.ReadSidecar(f.path)
			require.NoError(t, err)
			_, err = newBootstrapExternalRotationRecipe(t.Context(), f.cfg, req)
			require.ErrorIs(t, err, errBootstrapExternalRotationRequest)
			require.Empty(t, f.calls, "reused material must be refused before granting a proposal")
			after, err := encryption.ReadSidecar(f.path)
			require.NoError(t, err)
			require.Equal(t, before, after)
		})
	}
}
