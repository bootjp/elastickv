package main

import (
	"bytes"
	"context"
	"crypto"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/bootjp/elastickv/internal/clusterbootstrap"
	"github.com/bootjp/elastickv/internal/noderuntime"
	"github.com/bootjp/elastickv/internal/raftbootstrap"
	"github.com/bootjp/elastickv/internal/raftengine/etcd"
	"github.com/bootjp/elastickv/kv"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func bootstrapArtifactSyntaxArgs(command string, root string) []string {
	args := []string{command, "--expected-config", filepath.Join(root, "expected.json"), "--manifest", filepath.Join(root, "manifest.bin"),
		"--operation-id", "01000000000000000000000000000000"}
	if command == clusterbootstrap.BootstrapArtifactAttest {
		return append(args, "--raft-data-dir", filepath.Join(root, "node"), "--member-id", "1", "--peer-config", filepath.Join(root, "peers.json"))
	}
	return append(args, "--ack", filepath.Join(root, "ack.json"), "--output", filepath.Join(root, "activation.bin"))
}

func TestBootstrapArtifactCommandStrictSyntax(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	for _, command := range []string{clusterbootstrap.BootstrapArtifactAttest, clusterbootstrap.BootstrapArtifactCertify} {
		t.Run(command, func(t *testing.T) {
			args := bootstrapArtifactSyntaxArgs(command, root)
			request, err := clusterbootstrap.ParseBootstrapArtifactCommand(args)
			require.NoError(t, err)
			require.Equal(t, command, request.Command)
			for name, invalid := range map[string][]string{
				"missing": {command}, "unknown": append(append([]string{}, args...), "--unknown=x"),
				"duplicate":        append(append([]string{}, args...), "--manifest="+filepath.Join(root, "other")),
				"trailing":         append(append([]string{}, args...), "extra"),
				"separator":        append(append([]string{}, args...), "--"),
				"digest authority": append(append([]string{}, args...), "--proof-digest=01"),
			} {
				t.Run(name, func(t *testing.T) {
					var output bytes.Buffer
					handled, err := clusterbootstrap.DispatchBootstrapArtifactCommand(invalid, &output)
					require.True(t, handled)
					require.Error(t, err)
					require.Empty(t, output.Bytes())
				})
			}
		})
	}
	for _, args := range [][]string{nil, {"inspect"}, {"activate"}, {"prepare"}, {"unknown"}} {
		handled, err := clusterbootstrap.DispatchBootstrapArtifactCommand(args, nil)
		require.NoError(t, err)
		require.False(t, handled)
	}
	for _, path := range []string{"relative", "/", filepath.Join(root, "x") + "/../x", root + "//x", root + "/x\n"} {
		args := bootstrapArtifactSyntaxArgs(clusterbootstrap.BootstrapArtifactCertify, root)
		args[len(args)-1] = path
		_, err := clusterbootstrap.ParseBootstrapArtifactCommand(args)
		require.Error(t, err)
	}
	args := bootstrapArtifactSyntaxArgs(clusterbootstrap.BootstrapArtifactCertify, root)
	_, err := clusterbootstrap.ParseBootstrapArtifactCommand(append(args, "--ack", filepath.Join(root, "ack.json")))
	require.Error(t, err, "same ack path cannot be imported twice")
	args[len(args)-1] = filepath.Join(root, "manifest.bin")
	_, err = clusterbootstrap.ParseBootstrapArtifactCommand(args)
	require.Error(t, err, "output cannot alias an input path")
}

func bootstrapCertifyCommandFixture(t *testing.T) ([]string, string) {
	t.Helper()
	request, expectedJSON, raw, signers := baselineCLIFixtureWithSigners(t)
	manifest, err := raftbootstrap.ParseBaselineManifest(raw)
	require.NoError(t, err)
	root := t.TempDir()
	expected, err := json.Marshal(expectedJSON)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(root, "expected.json"), expected, 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(root, "manifest.bin"), raw, 0o600))
	args := []string{clusterbootstrap.BootstrapArtifactCertify, "--expected-config", filepath.Join(root, "expected.json"), "--manifest", filepath.Join(root, "manifest.bin"),
		"--operation-id", hex.EncodeToString(request.OperationID[:]), "--output", filepath.Join(root, "activation.bin")}
	digest, err := raftbootstrap.BaselineManifestDigest(manifest)
	require.NoError(t, err)
	for i, signer := range signers {
		ack, err := raftbootstrap.SignPreparedMemberAck(raftbootstrap.PreparedMemberAck{MemberID: manifest.Members[i].MemberID,
			ManifestDigest: digest, Minimum: manifest.Minimum, PreparedStateDigest: raftbootstrap.Digest{byte(i + 1)}}, manifest, signer)
		require.NoError(t, err)
		data, err := clusterbootstrap.MarshalBootstrapPreparedAck(ack, manifest)
		require.NoError(t, err)
		path := filepath.Join(root, "ack"+strconv.Itoa(i)+".json")
		require.NoError(t, os.WriteFile(path, data, 0o600))
		args = append(args, "--ack", path)
	}
	return args, root
}

func TestBootstrapArtifactCommandCertifyExclusive(t *testing.T) {
	t.Parallel()
	args, root := bootstrapCertifyCommandFixture(t)
	before := offlineDataFiles(t, root)
	var output bytes.Buffer
	handled, err := clusterbootstrap.DispatchBootstrapArtifactCommand(args, &output)
	require.True(t, handled)
	require.NoError(t, err)
	require.Empty(t, output.Bytes(), "certify only writes the explicitly selected artifact")
	path := filepath.Join(root, "activation.bin")
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	command, matched, err := raftbootstrap.ParseActivationCommand(data)
	require.NoError(t, err)
	require.True(t, matched)
	require.Len(t, command.Readiness.Members, 4)
	info, err := os.Stat(path)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o600), info.Mode().Perm())
	after := offlineDataFiles(t, root)
	delete(after, "activation.bin")
	require.Equal(t, before, after)
	_, err = clusterbootstrap.DispatchBootstrapArtifactCommand(args, &output)
	require.ErrorIs(t, err, os.ErrExist)
	again, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, data, again)
}

func TestBootstrapArtifactCommandRejectsPartialCertificateWithoutOutput(t *testing.T) {
	t.Parallel()
	for _, scenario := range []string{"missing learner", "duplicate member", "bad ack", "foreign context", "symlink output"} {
		t.Run(scenario, func(t *testing.T) {
			args, root := bootstrapCertifyCommandFixture(t)
			switch scenario {
			case "missing learner":
				args = args[:len(args)-2]
			case "duplicate member":
				ack, err := os.ReadFile(filepath.Join(root, "ack0.json"))
				require.NoError(t, err)
				require.NoError(t, os.WriteFile(filepath.Join(root, "ack3.json"), ack, 0o600))
			case "bad ack":
				require.NoError(t, os.WriteFile(filepath.Join(root, "ack3.json"), []byte("{} {}"), 0o600))
			case "foreign context":
				args[6] = "02000000000000000000000000000000"
			case "symlink output":
				require.NoError(t, os.Symlink(filepath.Join(root, "manifest.bin"), filepath.Join(root, "activation.bin")))
			}
			before := offlineDataFiles(t, root)
			handled, err := clusterbootstrap.DispatchBootstrapArtifactCommand(args, io.Discard)
			require.True(t, handled)
			require.Error(t, err)
			require.Equal(t, before, offlineDataFiles(t, root))
		})
	}
}

func TestBootstrapArtifactInputAggregateBoundAndPeerSigner(t *testing.T) {
	t.Parallel()
	raw := bootstrapConfigFixture(t)
	// Runtime inputs are irrelevant to offline signing and must not be opened.
	raw.StartupTokenFile, raw.ActivationFile = "/nonexistent/startup", "/nonexistent/activation"
	data, err := json.Marshal(raw)
	require.NoError(t, err)
	path := filepath.Join(t.TempDir(), "peer.json")
	require.NoError(t, os.WriteFile(path, data, 0o600))
	budget := clusterbootstrap.BootstrapArtifactBudget(raftbootstrap.MaxFileSize)
	config, err := clusterbootstrap.ReadBootstrapArtifactPeer(path, &budget)
	require.NoError(t, err)
	require.Equal(t, raw.Local, config.Local.Member)
	require.Empty(t, config.StartupToken)
	require.Empty(t, config.Activation)
	budget = clusterbootstrap.BootstrapArtifactBudget(len(data))
	_, err = clusterbootstrap.ReadBootstrapArtifactPeer(path, &budget)
	require.Error(t, err, "TLS inputs share the same total budget")
	budget = clusterbootstrap.BootstrapArtifactBudget(raftbootstrap.MaxFileSize)
	_, err = budget.Read(t.TempDir())
	require.Error(t, err)
	for _, invalid := range [][]byte{append(bytes.Clone(data), []byte(" {}")...), []byte(`{"local_member":1,"local_member":1}`), []byte(`{"unknown":1}`)} {
		_, err := clusterbootstrap.ParseBootstrapArtifactPeer(invalid)
		require.Error(t, err)
	}
}

type bootstrapArtifactFencedSigner struct {
	crypto.Signer
	dir   string
	calls int
}

func (s *bootstrapArtifactFencedSigner) Sign(random io.Reader, digest []byte, opts crypto.SignerOpts) ([]byte, error) {
	s.calls++
	owner, err := etcd.AcquireOfflineBaselineOwnership(s.dir)
	if err == nil {
		_ = owner.Close()
		return nil, errors.New("signer escaped real native offline ownership fence")
	}
	signature, err := s.Signer.Sign(random, digest, opts)
	return signature, errors.WithStack(err)
}

func bootstrapArtifactNativeFixture(t *testing.T) (clusterbootstrap.BootstrapBaselineCLIRequest, raftbootstrap.BaselineManifest, *clusterbootstrap.BootstrapPeerConfig, clusterbootstrap.BootstrapPeerJSON) {
	t.Helper()
	t.Setenv("ELASTICKV_RAFT_SNAPSHOT_COUNT", "1")
	raw := bootstrapConfigFixture(t)
	raw.Local, raw.Members[0].ID = etcd.DeriveNodeID("n1"), etcd.DeriveNodeID("n1")
	raw.Members[0].Groups = []uint64{noderuntime.DedicatedTSORaftGroupID}
	config, err := clusterbootstrap.BuildBootstrapPeerConfig(raw)
	require.NoError(t, err)
	base := t.TempDir()
	runtime, _, err := buildDedicatedTSOGroup("n1", noderuntime.GroupSpec{ID: noderuntime.DedicatedTSORaftGroupID, Address: config.Local.Address},
		base, true, true, false, nil, nil, etcd.NewFactory(etcd.FactoryConfig{}), kv.NewHLC(), nil)
	require.NoError(t, err)
	t.Cleanup(runtime.Close)
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	_, err = runtime.Engine.Propose(ctx, binary.BigEndian.AppendUint64([]byte("\x07TSOF\x01"), 42))
	require.NoError(t, err)
	require.Eventually(t, func() bool { return runtime.Engine.Status().LastSnapshotIndex != 0 }, 5*time.Second, 10*time.Millisecond)
	runtime.Close()
	dir := noderuntime.GroupDataDir(base, "n1", noderuntime.DedicatedTSORaftGroupID, true)
	owner, err := etcd.AcquireOfflineBaselineOwnership(dir)
	require.NoError(t, err)
	defer func() { require.NoError(t, owner.Close()) }()
	state, proof, err := clusterbootstrap.ProbeOfflineTSOBaseline(ctx, dir, owner)
	require.NoError(t, err)
	source, err := etcd.ReadOfflineBaselineWALSource(ctx, dir, owner)
	require.NoError(t, err)
	term := source.CommitTerm
	require.NoError(t, source.Snapshot.Close())
	expected := raftbootstrap.BaselineContext{Version: raftbootstrap.BaselineVersion,
		Identity: raftbootstrap.Identity{Cluster: config.Local.Cluster, Group: noderuntime.DedicatedTSORaftGroupID}, OperationID: raftbootstrap.OperationID{1},
		Minimum: raftbootstrap.Versions{Entry: 1, Snapshot: 1, Session: 1}, CommitIndex: state.AppliedIndex, CommitTerm: term,
		DataDigest: state.DataDigest, Watermark: state.Watermark, Configuration: raftbootstrap.Configuration{Index: proof.ConfigurationIndex,
			State: raftbootstrap.ConfState{Voters: []uint64{config.Local.Member}}, Members: []raftbootstrap.Member{
				{ID: config.Local.Member, Address: config.Local.Address, PublicKeyDigest: config.Local.Key}}}}
	inspection, err := etcd.InspectOfflineBaseline(ctx, etcd.OfflineBaselineOptions{Ownership: owner, DataDir: dir,
		MemberID: config.Local.Member, Expected: expected, ProbeStore: func(context.Context) (etcd.BaselineStoreProof, error) { return proof, nil }})
	require.NoError(t, err)
	signer, ok := config.ClientTLS.Certificates[0].PrivateKey.(crypto.Signer)
	require.True(t, ok)
	probe, err := raftbootstrap.SignBaselineProbe(raftbootstrap.BaselineProbe{BaselineContext: expected, Evidence: inspection.Evidence}, signer)
	require.NoError(t, err)
	return clusterbootstrap.BootstrapBaselineCLIRequest{Command: "prepare", RaftDataDir: dir, MemberID: config.Local.Member, OperationID: expected.OperationID},
		raftbootstrap.BaselineManifest{BaselineContext: expected, Members: []raftbootstrap.BaselineMemberEvidence{probe.Evidence}}, config, raw
}

func TestBootstrapArtifactAttestNativeVerifierAndSignerFence(t *testing.T) {
	request, manifest, config, raw := bootstrapArtifactNativeFixture(t)
	signer, ok := config.ClientTLS.Certificates[0].PrivateKey.(crypto.Signer)
	require.True(t, ok)
	fenced := &bootstrapArtifactFencedSigner{Signer: signer, dir: request.RaftDataDir}
	config.ClientTLS.Certificates[0].PrivateKey = fenced
	before := offlineDataFiles(t, request.RaftDataDir)
	ack, err := clusterbootstrap.AttestBootstrapArtifact(t.Context(), request, manifest.BaselineContext, manifest, config)
	require.Error(t, err, "legacy/baseline probe alone cannot issue ready attestation")
	require.Empty(t, ack)
	require.Zero(t, fenced.calls)
	require.Equal(t, before, offlineDataFiles(t, request.RaftDataDir))
	_, err = clusterbootstrap.ExecuteBootstrapMigration(t.Context(), request, manifest.BaselineContext, manifest)
	require.NoError(t, err)
	ack, err = clusterbootstrap.AttestBootstrapArtifact(t.Context(), request, manifest.BaselineContext, manifest, config)
	require.Error(t, err, "prepared but not exchanged/bound cannot sign")
	require.Empty(t, ack)
	require.Zero(t, fenced.calls)
	request.Command = "resume"
	_, err = clusterbootstrap.ExecuteBootstrapMigration(t.Context(), request, manifest.BaselineContext, manifest)
	require.NoError(t, err)
	before = offlineDataFiles(t, request.RaftDataDir)
	ack, err = clusterbootstrap.AttestBootstrapArtifact(t.Context(), request, manifest.BaselineContext, manifest, config)
	require.NoError(t, err)
	require.Equal(t, 1, fenced.calls)
	_, err = clusterbootstrap.ParseBootstrapPreparedAck(ack, manifest)
	require.NoError(t, err)
	require.Equal(t, before, offlineDataFiles(t, request.RaftDataDir))
	verifyBootstrapArtifactAttestDispatch(t, request, manifest, raw)
	owner, err := etcd.AcquireOfflineBaselineOwnership(request.RaftDataDir)
	require.NoError(t, err, "ownership must be released after signed artifact completes")
	ack, err = clusterbootstrap.AttestBootstrapArtifact(t.Context(), request, manifest.BaselineContext, manifest, config)
	require.Error(t, err, "concurrent owner fences the signer")
	require.Empty(t, ack)
	require.Equal(t, 1, fenced.calls)
	require.NoError(t, owner.Close())
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	ack, err = clusterbootstrap.AttestBootstrapArtifact(ctx, request, manifest.BaselineContext, manifest, config)
	require.Error(t, err)
	require.Empty(t, ack)
	require.Equal(t, 1, fenced.calls)
}

func verifyBootstrapArtifactAttestDispatch(t *testing.T, request clusterbootstrap.BootstrapBaselineCLIRequest, manifest raftbootstrap.BaselineManifest, peer clusterbootstrap.BootstrapPeerJSON) {
	t.Helper()
	root := t.TempDir()
	args := bootstrapArtifactSyntaxArgs(clusterbootstrap.BootstrapArtifactAttest, root)
	args[6] = hex.EncodeToString(request.OperationID[:])
	args[8], args[10] = request.RaftDataDir, strconv.FormatUint(request.MemberID, 10)
	expected := clusterbootstrap.BootstrapBaselineExpectedJSON{Version: manifest.Version, Cluster: peer.Cluster,
		Group: strconv.FormatUint(manifest.Identity.Group, 10), ConfigurationIndex: strconv.FormatUint(manifest.Configuration.Index, 10),
		Minimum:     clusterbootstrap.BootstrapBaselineMinimumJSON{Entry: manifest.Minimum.Entry, Snapshot: manifest.Minimum.Snapshot, Session: manifest.Minimum.Session},
		CommitIndex: strconv.FormatUint(manifest.CommitIndex, 10), CommitTerm: strconv.FormatUint(manifest.CommitTerm, 10),
		Watermark: strconv.FormatUint(manifest.Watermark, 10), DataDigest: hex.EncodeToString(manifest.DataDigest[:]),
		Members: []clusterbootstrap.BootstrapBaselineMemberJSON{{ID: strconv.FormatUint(request.MemberID, 10), Address: peer.Members[0].Address,
			Key: peer.Members[0].Key, Roles: []string{"voter"}}}}
	expectedJSON, err := json.Marshal(expected)
	require.NoError(t, err)
	peerJSON, err := json.Marshal(peer)
	require.NoError(t, err)
	manifestRaw, err := raftbootstrap.MarshalBaselineManifest(manifest)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(args[2], expectedJSON, 0o600))
	require.NoError(t, os.WriteFile(args[4], manifestRaw, 0o600))
	require.NoError(t, os.WriteFile(args[12], peerJSON, 0o600))
	before := offlineDataFiles(t, request.RaftDataDir)
	var output bytes.Buffer
	handled, err := clusterbootstrap.DispatchBootstrapArtifactCommand(args, &output)
	require.True(t, handled)
	require.NoError(t, err)
	ack, err := clusterbootstrap.ParseBootstrapPreparedAck(output.Bytes(), manifest)
	require.NoError(t, err)
	canonical, err := clusterbootstrap.MarshalBootstrapPreparedAck(ack, manifest)
	require.NoError(t, err)
	require.Equal(t, canonical, output.Bytes(), "stdout contains canonical ack JSON only")
	require.Equal(t, before, offlineDataFiles(t, request.RaftDataDir))
}
