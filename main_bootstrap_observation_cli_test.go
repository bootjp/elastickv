package main

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/bootjp/elastickv/internal/clusterbootstrap"
	"github.com/bootjp/elastickv/internal/noderuntime"
	"github.com/bootjp/elastickv/internal/raftbootstrap"
	"github.com/stretchr/testify/require"
)

func observationSyntaxArgs(root, command string) []string {
	args := []string{command, "--operation-id", "01000000000000000000000000000000", "--output", filepath.Join(root, "out.bin")}
	if command == "probe" {
		return append(args, "--pinned-config", filepath.Join(root, "pins.json"), "--peer-config", filepath.Join(root, "peers.json"), "--raft-data-dir", filepath.Join(root, "node"), "--member-id", "1")
	}
	return append(args, "--expected-config", filepath.Join(root, "expected.json"), "--probe", filepath.Join(root, "probe.bin"))
}

func TestBootstrapObservationStrictSyntax(t *testing.T) {
	t.Parallel()
	for _, command := range []string{"probe", "manifest"} {
		args := observationSyntaxArgs(t.TempDir(), command)
		_, err := clusterbootstrap.ParseBootstrapObservationCommand(args)
		require.NoError(t, err)
		for name, invalid := range map[string][]string{
			"missing": {command}, "unknown": append(append([]string{}, args...), "--cut=5"),
			"duplicate":  append(append([]string{}, args...), "--output=/tmp/other"),
			"positional": append(append([]string{}, args...), "extra"),
			"separator":  append(append([]string{}, args...), "--"),
		} {
			t.Run(command+"/"+name, func(t *testing.T) {
				var output bytes.Buffer
				handled, err := clusterbootstrap.DispatchBootstrapObservationCommand(invalid, &output)
				require.True(t, handled)
				require.Error(t, err)
				require.Empty(t, output.Bytes())
			})
		}
	}
	handled, err := clusterbootstrap.DispatchBootstrapObservationCommand([]string{"attest"}, nil)
	require.NoError(t, err)
	require.False(t, handled)
}

func TestBootstrapObservationPinsAndMeasuredConfig(t *testing.T) {
	t.Parallel()
	r, raw, _, _ := baselineCLIFixtureWithSigners(t)
	expectedRaw, err := json.Marshal(raw)
	require.NoError(t, err)
	expected, err := clusterbootstrap.ParseBootstrapBaselineExpectedConfig(expectedRaw, r.OperationID)
	require.NoError(t, err)
	pins := clusterbootstrap.BootstrapObservationPinsJSON{Version: raw.Version, Cluster: raw.Cluster, Group: raw.Group, Minimum: raw.Minimum, Members: raw.Members}
	encoded, err := json.Marshal(pins)
	require.NoError(t, err)
	parsed, err := clusterbootstrap.ParseBootstrapObservationPins(encoded)
	require.NoError(t, err)
	require.Zero(t, parsed.CommitIndex)
	require.Zero(t, parsed.Configuration.Index)
	require.Empty(t, parsed.DataDigest)
	measured, err := clusterbootstrap.MarshalBootstrapObservedConfig(expected)
	require.NoError(t, err)
	roundtrip, err := clusterbootstrap.ParseBootstrapBaselineExpectedConfig(measured, r.OperationID)
	require.NoError(t, err)
	require.Equal(t, expected, roundtrip)
	require.Contains(t, string(measured), `"auto_leave": false`)
	require.NotEmpty(t, roundtrip.Configuration.State.VotersOutgoing)
	require.NotEmpty(t, roundtrip.Configuration.State.LearnersNext)
	raw.AutoLeave = true
	unstable, err := json.Marshal(raw)
	require.NoError(t, err)
	_, err = clusterbootstrap.ParseBootstrapBaselineExpectedConfig(unstable, r.OperationID)
	require.Error(t, err)
	pins.AutoLeave = true
	unstable, err = json.Marshal(pins)
	require.NoError(t, err)
	_, err = clusterbootstrap.ParseBootstrapObservationPins(unstable)
	require.Error(t, err, "auto-leave must not silently become false")
	expected.Configuration.State.AutoLeave = true
	unstable, err = clusterbootstrap.MarshalBootstrapObservedConfig(expected)
	require.NoError(t, err)
	require.Contains(t, string(unstable), `"auto_leave": true`)
	for _, invalid := range [][]byte{expectedRaw, append(encoded[:len(encoded)-1:len(encoded)-1], []byte(`,"group_id":"0"}`)...), []byte(`{} {}`)} {
		_, err := clusterbootstrap.ParseBootstrapObservationPins(invalid)
		require.Error(t, err)
	}
}

func observationManifestFixture(t *testing.T) ([]string, string) {
	t.Helper()
	r, expectedJSON, raw, _ := baselineCLIFixtureWithSigners(t)
	manifest, err := raftbootstrap.ParseBaselineManifest(raw)
	require.NoError(t, err)
	root := t.TempDir()
	expected, err := json.Marshal(expectedJSON)
	require.NoError(t, err)
	expectedPath := filepath.Join(root, "expected.json")
	require.NoError(t, os.WriteFile(expectedPath, expected, 0o600))
	args := []string{"bootstrap", "manifest", "--expected-config", expectedPath, "--operation-id", hex.EncodeToString(r.OperationID[:]), "--output", filepath.Join(root, "manifest.bin")}
	for i, member := range manifest.Members {
		encoded, err := raftbootstrap.MarshalBaselineProbe(raftbootstrap.BaselineProbe{BaselineContext: manifest.BaselineContext, Evidence: member})
		require.NoError(t, err)
		path := filepath.Join(root, "probe"+strconv.Itoa(i)+".bin")
		require.NoError(t, os.WriteFile(path, encoded, 0o600))
		args = append(args, "--probe", path)
	}
	return args, root
}

func TestBootstrapObservationManifestExactAndExclusive(t *testing.T) {
	t.Parallel()
	args, root := observationManifestFixture(t)
	before := offlineDataFiles(t, root)
	handled, err := clusterbootstrap.DispatchBootstrapOperator(args, io.Discard)
	require.True(t, handled)
	require.NoError(t, err)
	encoded, err := os.ReadFile(filepath.Join(root, "manifest.bin"))
	require.NoError(t, err)
	manifest, err := raftbootstrap.ParseBaselineManifest(encoded)
	require.NoError(t, err)
	require.Len(t, manifest.Members, 4)
	after := offlineDataFiles(t, root)
	delete(after, "manifest.bin")
	require.Equal(t, before, after)
	_, err = clusterbootstrap.DispatchBootstrapOperator(args, io.Discard)
	require.ErrorIs(t, err, os.ErrExist)
}

func TestBootstrapObservationManifestRejectsDivergence(t *testing.T) {
	t.Parallel()
	for _, scenario := range []string{"missing", "duplicate", "foreign operation", "bad signature", "review mismatch"} {
		t.Run(scenario, func(t *testing.T) {
			args, root := observationManifestFixture(t)
			switch scenario {
			case "missing":
				args = args[:len(args)-2]
			case "duplicate":
				data, err := os.ReadFile(filepath.Join(root, "probe0.bin"))
				require.NoError(t, err)
				require.NoError(t, os.WriteFile(filepath.Join(root, "probe3.bin"), data, 0o600))
			case "foreign operation":
				args[5] = "02000000000000000000000000000000"
			case "bad signature":
				require.NoError(t, os.WriteFile(filepath.Join(root, "probe3.bin"), []byte("corrupt"), 0o600))
			case "review mismatch":
				data, err := os.ReadFile(filepath.Join(root, "expected.json"))
				require.NoError(t, err)
				var expected clusterbootstrap.BootstrapBaselineExpectedJSON
				require.NoError(t, json.Unmarshal(data, &expected))
				expected.Watermark = "999999"
				data, err = json.Marshal(expected)
				require.NoError(t, err)
				require.NoError(t, os.WriteFile(filepath.Join(root, "expected.json"), data, 0o600))
			}
			before := offlineDataFiles(t, root)
			_, err := clusterbootstrap.DispatchBootstrapOperator(args, io.Discard)
			require.Error(t, err)
			require.Equal(t, before, offlineDataFiles(t, root))
		})
	}
}

func TestBootstrapObservationProbeRealLegacyTSO(t *testing.T) {
	r, manifest, _, peer := bootstrapArtifactNativeFixture(t)
	exerciseBootstrapObservationProbe(t, r, manifest, peer)
}

func TestBootstrapObservationProbeRealLegacyData(t *testing.T) {
	t.Setenv("ELASTICKV_RAFT_SNAPSHOT_COUNT", "1")
	peer, config := enablementPeerConfig(t)
	group := noderuntime.GroupSpec{ID: 1, Address: config.Local.Address}
	dir := enablementLegacyCut(t, t.TempDir(), group)
	manifest := enablementSignedBaseline(t, dir, config, group)
	exerciseBootstrapObservationProbe(t, clusterbootstrap.BootstrapBaselineCLIRequest{RaftDataDir: dir, MemberID: config.Local.Member, OperationID: manifest.OperationID}, manifest, peer)
}

func exerciseBootstrapObservationProbe(t *testing.T, r clusterbootstrap.BootstrapBaselineCLIRequest, manifest raftbootstrap.BaselineManifest, peer clusterbootstrap.BootstrapPeerJSON) {
	t.Helper()
	measured, err := clusterbootstrap.MarshalBootstrapObservedConfig(manifest.BaselineContext)
	require.NoError(t, err)
	var expected clusterbootstrap.BootstrapBaselineExpectedJSON
	require.NoError(t, json.Unmarshal(measured, &expected))
	pins, err := json.Marshal(clusterbootstrap.BootstrapObservationPinsJSON{Version: expected.Version, Cluster: expected.Cluster, Group: expected.Group, Minimum: expected.Minimum, Members: expected.Members})
	require.NoError(t, err)
	root := t.TempDir()
	pinsPath, peersPath, artifact := filepath.Join(root, "pins.json"), filepath.Join(root, "peers.json"), filepath.Join(root, "probe.bin")
	require.NoError(t, os.WriteFile(pinsPath, pins, 0o600))
	peerJSON, err := json.Marshal(peer)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(peersPath, peerJSON, 0o600))
	args := []string{"bootstrap", "probe", "--pinned-config", pinsPath, "--peer-config", peersPath, "--raft-data-dir", r.RaftDataDir,
		"--operation-id", hex.EncodeToString(r.OperationID[:]), "--member-id", strconv.FormatUint(r.MemberID, 10), "--output", artifact}
	before := offlineDataFiles(t, r.RaftDataDir)
	wrongMember := append([]string{}, args...)
	wrongMember[len(wrongMember)-3] = "99999"
	var rejected bytes.Buffer
	_, err = clusterbootstrap.DispatchBootstrapOperator(wrongMember, &rejected)
	require.Error(t, err)
	require.Empty(t, rejected.Bytes())
	_, err = os.Stat(artifact)
	require.ErrorIs(t, err, os.ErrNotExist)
	var output bytes.Buffer
	handled, err := clusterbootstrap.DispatchBootstrapOperator(args, &output)
	require.True(t, handled)
	require.NoError(t, err)
	data, err := os.ReadFile(artifact)
	require.NoError(t, err)
	probe, err := raftbootstrap.ParseBaselineProbe(data)
	require.NoError(t, err)
	require.NoError(t, raftbootstrap.VerifyBaselineProbe(probe, manifest.BaselineContext))
	reviewed, err := clusterbootstrap.ParseBootstrapBaselineExpectedConfig(output.Bytes(), r.OperationID)
	require.NoError(t, err)
	require.NoError(t, raftbootstrap.VerifyBaselineProbe(probe, reviewed))
	require.Equal(t, before, offlineDataFiles(t, r.RaftDataDir))
	_, err = clusterbootstrap.DispatchBootstrapOperator(args, io.Discard)
	require.ErrorIs(t, err, os.ErrExist)
}
