package main

import (
	"testing"

	"github.com/bootjp/elastickv/internal/raftadmission"
	"github.com/bootjp/elastickv/internal/raftbootstrap"
	"github.com/bootjp/elastickv/kv"
	"github.com/stretchr/testify/require"
)

func TestBootstrapPayloadScopeRejectsForeignNativeTarget(t *testing.T) {
	t.Parallel()
	for _, kind := range []raftbootstrap.ControlKind{raftbootstrap.ControlActivate, raftbootstrap.ControlGuardedWatermark, raftbootstrap.ControlGuardedCatalog} {
		t.Run(string(rune('0'+kind)), func(t *testing.T) {
			var payload []byte
			var witness raftbootstrap.Witness
			lane := raftadmission.Catalog
			if kind == raftbootstrap.ControlActivate {
				payload = bootstrapPayloadActivation(t, 7)
				command, matched, err := raftbootstrap.ParseActivationCommand(payload)
				require.NoError(t, err)
				require.True(t, matched)
				witness = command.Witness
				lane = raftadmission.Recovery
			} else {
				payload = bootstrapPayloadGuarded(t, kind)
				command, matched, err := kv.ParseGuardedBootstrapCommand(payload)
				require.NoError(t, err)
				require.True(t, matched)
				witness = command.Witness
			}
			validator := bootstrapPayloadValidator(witness.Cluster)
			require.NoError(t, validator(lane, witness.Group, payload))
			require.ErrorIs(t, validator(lane, witness.Group+1, payload), raftadmission.ErrClosed)
			otherCluster := witness.Cluster
			otherCluster[0]++
			require.ErrorIs(t, bootstrapPayloadValidator(otherCluster)(lane, witness.Group, payload), raftadmission.ErrClosed)
			require.Error(t, validator(raftadmission.Data, witness.Group, payload))
		})
	}
}
