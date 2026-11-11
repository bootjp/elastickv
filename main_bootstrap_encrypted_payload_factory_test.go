package main

import (
	"testing"

	"github.com/bootjp/elastickv/internal/raftadmission"
	"github.com/bootjp/elastickv/internal/raftbootstrap"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestBootstrapEncryptedPayloadFactoryExplicitDomainBinding(t *testing.T) {
	t.Parallel()
	cluster := raftbootstrap.UUID{1}
	projection, wrap := bootstrapEncryptedPayloadFixture(t, 17)
	final, err := wrap(bootstrapPayloadProto(t, bootstrapPayloadSingle, bootstrapPayloadRequest()))
	require.NoError(t, err)
	validator, err := newBootstrapEncryptedPayloadValidator(cluster, func(group uint64) (bootstrapPayloadProjection, error) {
		require.Equal(t, uint64(17), group)
		return projection, nil
	})
	require.NoError(t, err)
	require.NoError(t, validator(raftadmission.Data, 17, final))
	foreign, _ := bootstrapEncryptedPayloadFixture(t, 17)
	other, err := newBootstrapEncryptedPayloadValidator(cluster, func(uint64) (bootstrapPayloadProjection, error) { return foreign, nil })
	require.NoError(t, err)
	require.Error(t, other(raftadmission.Data, 17, final), "same numeric group does not make another installed cipher authoritative")
	_, err = newBootstrapEncryptedPayloadValidator(cluster, nil)
	require.ErrorIs(t, err, errBootstrapPayloadProjection)
	_, err = newBootstrapEncryptedPayloadValidator(raftbootstrap.UUID{}, func(uint64) (bootstrapPayloadProjection, error) { return projection, nil })
	require.ErrorIs(t, err, errBootstrapPayloadProjection)
	unbound, err := newBootstrapPayloadProjection(17, 1, nil, 0, false)
	require.NoError(t, err)
	noCipher, err := newBootstrapEncryptedPayloadValidator(cluster, func(uint64) (bootstrapPayloadProjection, error) { return unbound, nil })
	require.NoError(t, err)
	require.ErrorIs(t, noCipher(raftadmission.Data, 17, bootstrapPayloadProto(t, bootstrapPayloadSingle, bootstrapPayloadRequest())), errBootstrapPayloadProjection,
		"an encryption-configured factory must not silently downgrade an unbound cipher to plaintext")
}

func TestBootstrapEncryptedPayloadFactoryRejectsChangingPublication(t *testing.T) {
	t.Parallel()
	projection, wrap := bootstrapEncryptedPayloadFixture(t, 17)
	final, err := wrap(bootstrapPayloadProto(t, bootstrapPayloadSingle, bootstrapPayloadRequest()))
	require.NoError(t, err)
	for name, mutate := range map[string]func(*bootstrapPayloadProjection){
		"generation": func(p *bootstrapPayloadProjection) { p.Generation++ },
		"group":      func(p *bootstrapPayloadProjection) { p.Group++ },
		"cutover":    func(p *bootstrapPayloadProjection) { p.RaftEnvelopeActive = false },
		"key":        func(p *bootstrapPayloadProjection) { p.RaftKeyID++ },
		"cipher":     func(p *bootstrapPayloadProjection) { p.RaftCipher = nil },
		"recipe": func(p *bootstrapPayloadProjection) {
			p.rotationRecipes = []bootstrapRotationPayloadGrant{{group: p.Group}}
		},
	} {
		t.Run(name, func(t *testing.T) {
			calls := 0
			validator, err := newBootstrapFinalPayloadValidator(raftbootstrap.UUID{1}, func(uint64) (bootstrapPayloadProjection, error) {
				calls++
				image := projection
				if calls > 1 {
					mutate(&image)
				}
				return image, nil
			})
			require.NoError(t, err)
			proof, err := validator(raftadmission.Data, 17, final)
			require.ErrorIs(t, err, raftadmission.ErrStale)
			require.Zero(t, proof)
		})
	}
	closed := errors.New("installed projection is fenced")
	validator, err := newBootstrapFinalPayloadValidator(raftbootstrap.UUID{1}, func(uint64) (bootstrapPayloadProjection, error) {
		return bootstrapPayloadProjection{}, closed
	})
	require.NoError(t, err)
	proof, err := validator(raftadmission.Data, 17, final)
	require.ErrorIs(t, err, closed)
	require.Zero(t, proof)
}
