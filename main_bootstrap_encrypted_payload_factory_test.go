package main

import (
	"testing"

	"github.com/bootjp/elastickv/internal/clusterbootstrap"
	"github.com/bootjp/elastickv/internal/raftadmission"
	"github.com/bootjp/elastickv/internal/raftbootstrap"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestBootstrapEncryptedPayloadFactoryExplicitDomainBinding(t *testing.T) {
	t.Parallel()
	cluster := raftbootstrap.UUID{1}
	projection, wrap := bootstrapEncryptedPayloadFixture(t, 17)
	final, err := wrap(bootstrapPayloadProto(t, clusterbootstrap.BootstrapPayloadSingle, bootstrapPayloadRequest()))
	require.NoError(t, err)
	validator, err := clusterbootstrap.NewBootstrapEncryptedPayloadValidator(cluster, func(group uint64) (clusterbootstrap.BootstrapPayloadProjection, error) {
		require.Equal(t, uint64(17), group)
		return projection, nil
	})
	require.NoError(t, err)
	require.NoError(t, validator(raftadmission.Data, 17, final))
	foreign, _ := bootstrapEncryptedPayloadFixture(t, 17)
	other, err := clusterbootstrap.NewBootstrapEncryptedPayloadValidator(cluster, func(uint64) (clusterbootstrap.BootstrapPayloadProjection, error) { return foreign, nil })
	require.NoError(t, err)
	require.Error(t, other(raftadmission.Data, 17, final), "same numeric group does not make another installed cipher authoritative")
	_, err = clusterbootstrap.NewBootstrapEncryptedPayloadValidator(cluster, nil)
	require.ErrorIs(t, err, clusterbootstrap.ErrBootstrapPayloadProjection)
	_, err = clusterbootstrap.NewBootstrapEncryptedPayloadValidator(raftbootstrap.UUID{}, func(uint64) (clusterbootstrap.BootstrapPayloadProjection, error) { return projection, nil })
	require.ErrorIs(t, err, clusterbootstrap.ErrBootstrapPayloadProjection)
	unbound, err := clusterbootstrap.NewBootstrapPayloadProjection(17, 1, nil, 0, false)
	require.NoError(t, err)
	noCipher, err := clusterbootstrap.NewBootstrapEncryptedPayloadValidator(cluster, func(uint64) (clusterbootstrap.BootstrapPayloadProjection, error) { return unbound, nil })
	require.NoError(t, err)
	require.ErrorIs(t, noCipher(raftadmission.Data, 17, bootstrapPayloadProto(t, clusterbootstrap.BootstrapPayloadSingle, bootstrapPayloadRequest())), clusterbootstrap.ErrBootstrapPayloadProjection,
		"an encryption-configured factory must not silently downgrade an unbound cipher to plaintext")
}

func TestBootstrapEncryptedPayloadFactoryRejectsChangingPublication(t *testing.T) {
	t.Parallel()
	projection, wrap := bootstrapEncryptedPayloadFixture(t, 17)
	final, err := wrap(bootstrapPayloadProto(t, clusterbootstrap.BootstrapPayloadSingle, bootstrapPayloadRequest()))
	require.NoError(t, err)
	for name, mutate := range map[string]func(*clusterbootstrap.BootstrapPayloadProjection){
		"generation": func(p *clusterbootstrap.BootstrapPayloadProjection) { p.Generation++ },
		"group":      func(p *clusterbootstrap.BootstrapPayloadProjection) { p.Group++ },
		"cutover":    func(p *clusterbootstrap.BootstrapPayloadProjection) { p.RaftEnvelopeActive = false },
		"key":        func(p *clusterbootstrap.BootstrapPayloadProjection) { p.RaftKeyID++ },
		"cipher":     func(p *clusterbootstrap.BootstrapPayloadProjection) { p.RaftCipher = nil },
		"recipe": func(p *clusterbootstrap.BootstrapPayloadProjection) {
			p.RotationRecipes = []clusterbootstrap.BootstrapRotationPayloadGrant{{Group: p.Group}}
		},
	} {
		t.Run(name, func(t *testing.T) {
			calls := 0
			validator, err := clusterbootstrap.NewBootstrapFinalPayloadValidator(raftbootstrap.UUID{1}, func(uint64) (clusterbootstrap.BootstrapPayloadProjection, error) {
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
	validator, err := clusterbootstrap.NewBootstrapFinalPayloadValidator(raftbootstrap.UUID{1}, func(uint64) (clusterbootstrap.BootstrapPayloadProjection, error) {
		return clusterbootstrap.BootstrapPayloadProjection{}, closed
	})
	require.NoError(t, err)
	proof, err := validator(raftadmission.Data, 17, final)
	require.ErrorIs(t, err, closed)
	require.Zero(t, proof)
}
