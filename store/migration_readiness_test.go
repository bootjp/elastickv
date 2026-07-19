package store

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTargetStagedReadinessCodecRoundTripSourceControls(t *testing.T) {
	t.Parallel()

	want := TargetStagedReadinessState{
		JobID:                  9,
		RouteStart:             []byte("m"),
		RouteEnd:               []byte("z"),
		ExpectedCutoverVersion: 11,
		MigrationJobID:         7,
		MinWriteTSExclusive:    100,
		Armed:                  true,
		SourceWriteFence:       true,
		SourceReadFence:        true,
		RetentionPinTS:         80,
		TrackWrites:            true,
		MinAdmittedTS:          70,
	}

	got, ok := decodeTargetStagedReadinessState(encodeTargetStagedReadinessState(want))
	require.True(t, ok)
	require.Equal(t, want, got)
}

func TestTargetStagedReadinessCodecDecodesV1(t *testing.T) {
	t.Parallel()

	data := []byte{targetReadinessCodecVersionV1, targetReadinessArmedFlag}
	for _, value := range []uint64{9, 11, 7, 100} {
		data = binary.BigEndian.AppendUint64(data, value)
	}
	data = appendVarBytes(data, []byte("m"))
	data = append(data, 1)
	data = appendVarBytes(data, []byte("z"))

	got, ok := decodeTargetStagedReadinessState(data)
	require.True(t, ok)
	require.Equal(t, TargetStagedReadinessState{
		JobID:                  9,
		RouteStart:             []byte("m"),
		RouteEnd:               []byte("z"),
		ExpectedCutoverVersion: 11,
		MigrationJobID:         7,
		MinWriteTSExclusive:    100,
		Armed:                  true,
	}, got)
}

func TestValidateTargetStagedReadinessSourceControls(t *testing.T) {
	t.Parallel()

	base := TargetStagedReadinessState{
		JobID:               9,
		MigrationJobID:      7,
		MinWriteTSExclusive: 100,
		Armed:               true,
	}

	readOnly := base
	readOnly.SourceReadFence = true
	readOnly.RetentionPinTS = 80
	require.ErrorContains(t, validateTargetStagedReadinessState(readOnly), "requires source write fence")

	withoutPin := base
	withoutPin.SourceWriteFence = true
	require.ErrorContains(t, validateTargetStagedReadinessState(withoutPin), "retention pin is required")

	trackerWithoutPin := base
	trackerWithoutPin.TrackWrites = true
	require.ErrorContains(t, validateTargetStagedReadinessState(trackerWithoutPin), "tracker retention pin is required")
}
