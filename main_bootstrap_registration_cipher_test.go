package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"testing"

	"github.com/bootjp/elastickv/internal/clusterbootstrap"
	"github.com/bootjp/elastickv/internal/encryptionwiring"
	"github.com/bootjp/elastickv/internal/peersession"
	"github.com/bootjp/elastickv/internal/raftadmission"
	"github.com/bootjp/elastickv/internal/raftbootstrap"
	"github.com/bootjp/elastickv/internal/raftengine"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/stretchr/testify/require"
)

func TestBootstrapRegistrationTLSCipherBoundDeclaration(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name                                                       string
		dek                                                        uint32
		member                                                     uint64
		epoch                                                      uint16
		tamper, trailing, stale, invalidate, substitute, plainSeal bool
		accepted                                                   bool
	}{
		{name: "matching plaintext and ciphertext seal", dek: 7, member: 2, epoch: 3, accepted: true},
		{name: "different DEK", dek: 8, member: 2, epoch: 3},
		{name: "different full member", dek: 7, member: 1<<32 | 2, epoch: 3},
		{name: "different epoch", dek: 7, member: 2, epoch: 4},
		{name: "tampered ciphertext", dek: 7, member: 2, epoch: 3, tamper: true},
		{name: "trailing ciphertext", dek: 7, member: 2, epoch: 3, trailing: true},
		{name: "stale projection", dek: 7, member: 2, epoch: 3, stale: true},
		{name: "publication during inspection", dek: 7, member: 2, epoch: 3, invalidate: true},
		{name: "same plaintext different ciphertext", dek: 7, member: 2, epoch: 3, substitute: true},
		{name: "plaintext cannot replace final seal", dek: 7, member: 2, epoch: 3, plainSeal: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			projection, wrap := bootstrapEncryptedPayloadFixture(t, 17)
			plain := encryptionwiring.RegistrationEntry(tc.dek, tc.member, tc.epoch)
			final, err := wrap(plain)
			require.NoError(t, err)
			second, err := wrap(plain)
			require.NoError(t, err)
			require.NotEqual(t, final, second, "real nonces must produce distinct final seals")
			if tc.tamper {
				final[len(final)-1] ^= 1
			}
			if tc.trailing {
				final = append(bytes.Clone(final), 0)
			}
			var fixture *bootstrapAuthFixture
			inspect := bootstrapRegistrationCipherInspector(&fixture, projection, tc.stale, tc.invalidate)
			fixture = newBootstrapAuthFixture(t, 17, peersession.IdentityReady, nil,
				bootstrapAuthRegistrationOptions{prepared: true, inspectPayload: inspect})
			require.True(t, fixture.a.NeedsActivation(17))
			fixture.service.action = func(ctx context.Context) error {
				bound, err := fixture.a.BindProposal(ctx, 17, final)
				if err != nil {
					return err
				}
				digest := sha256.Sum256(final)
				if tc.substitute {
					digest = sha256.Sum256(second)
				}
				if tc.plainSeal {
					digest = sha256.Sum256(plain)
				}
				return fixture.a.WithNative(bound, 17, func(scope raftengine.NativeScope) error {
					return scope.Check(bound, raftengine.NativeRequest{Operation: raftengine.NativeProposal, PayloadDigest: digest})
				})
			}
			ctx, cancel := bootstrapGateContext(t, bootstrapGateTestToken)
			defer cancel()
			err = fixture.handle.Invoke(ctx, fixture.cc, pb.EncryptionAdmin_RegisterEncryptionWriter_FullMethodName,
				encryptionwiring.RegistrationRequest(7, 2, 3), &pb.RegisterEncryptionWriterResponse{})
			if tc.accepted {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
			require.EqualValues(t, 1, fixture.service.calls.Load(), "negative must reach native binding, not fail at unrelated auth")
			require.Zero(t, fixture.authCalls.Load())
		})
	}
}

func bootstrapRegistrationCipherInspector(fixture **bootstrapAuthFixture, projection clusterbootstrap.BootstrapPayloadProjection, stale, invalidate bool) func(raftadmission.Kind, uint64, []byte, raftbootstrap.Digest) (raftadmission.PayloadInspection, error) {
	return func(kind raftadmission.Kind, group uint64, payload []byte, _ raftbootstrap.Digest) (raftadmission.PayloadInspection, error) {
		generation, err := (*fixture).a.EncryptionFence()
		if err != nil {
			return raftadmission.PayloadInspection{}, err
		}
		current := projection
		current.Generation = generation
		validated, err := clusterbootstrap.ValidateBootstrapFinalPayload(raftbootstrap.UUID{1}, kind, group, payload, current)
		if err != nil {
			return raftadmission.PayloadInspection{}, err
		}
		if stale {
			validated.ProjectionGeneration++
		}
		if invalidate {
			(*fixture).a.InvalidateEncryption()
		}
		return raftadmission.PayloadInspection{PlaintextDigest: validated.PlaintextDigest,
			ProjectionGeneration: validated.ProjectionGeneration}, nil
	}
}
