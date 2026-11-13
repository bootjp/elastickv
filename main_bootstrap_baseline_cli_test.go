package main

import (
	"bytes"
	"crypto"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"strings"
	"testing"

	"github.com/bootjp/elastickv/internal/clusterbootstrap"
	"github.com/bootjp/elastickv/internal/raftbootstrap"
	"github.com/stretchr/testify/require"
)

func baselineCLIArgs() []string {
	return []string{"inspect", "--expected-config", "/offline/expected.json", "--manifest", "/offline/baseline.bin",
		"--raft-data-dir", "/offline/node-1", "--operation-id", "01000000000000000000000000000000", "--member-id", "1"}
}

func baselineCLIFixture(t testing.TB) (clusterbootstrap.BootstrapBaselineCLIRequest, clusterbootstrap.BootstrapBaselineExpectedJSON, []byte) {
	t.Helper()
	request, expected, manifest, _ := baselineCLIFixtureWithSigners(t)
	return request, expected, manifest
}

func baselineCLIFixtureWithSigners(t testing.TB) (clusterbootstrap.BootstrapBaselineCLIRequest, clusterbootstrap.BootstrapBaselineExpectedJSON, []byte, []crypto.Signer) {
	t.Helper()
	request, err := clusterbootstrap.ParseBootstrapBaselineCLI(baselineCLIArgs())
	require.NoError(t, err)
	raw := clusterbootstrap.BootstrapBaselineExpectedJSON{Version: 1, Cluster: "01000000-0000-0000-0000-000000000000", Group: "0",
		ConfigurationIndex: "8", Minimum: clusterbootstrap.BootstrapBaselineMinimumJSON{Entry: 1, Snapshot: 1, Session: 1}, CommitIndex: "12", CommitTerm: "3", Watermark: "0",
		DataDigest: strings.Repeat("01", 32)}
	roles := [][]string{{"voter", "outgoing"}, {"voter"}, {"outgoing", "next"}, {"learner"}}
	keys := make([]crypto.Signer, len(roles))
	for i := range keys {
		public, private, err := ed25519.GenerateKey(rand.Reader)
		require.NoError(t, err)
		keys[i] = private
		der, err := x509.MarshalPKIXPublicKey(public)
		require.NoError(t, err)
		digest := sha256.Sum256(der)
		raw.Members = append(raw.Members, clusterbootstrap.BootstrapBaselineMemberJSON{ID: string(rune('1' + i)), Address: "localhost:1234", Key: hex.EncodeToString(digest[:]), Roles: roles[i]})
	}
	config, err := json.Marshal(raw)
	require.NoError(t, err)
	expected, err := clusterbootstrap.ParseBootstrapBaselineExpectedConfig(config, request.OperationID)
	require.NoError(t, err)
	digest, err := raftbootstrap.ConfigurationDigest(expected.Configuration)
	require.NoError(t, err)
	m := raftbootstrap.BaselineManifest{BaselineContext: expected}
	for i, key := range keys {
		id := uint64(i + 1)
		p, err := raftbootstrap.SignBaselineProbe(raftbootstrap.BaselineProbe{BaselineContext: expected, Evidence: raftbootstrap.BaselineMemberEvidence{
			MemberID: id, CommitIndex: 12, CommitTerm: 3, AppliedIndex: 12, LastLogIndex: 12,
			ConfigurationDigest: digest, NativeStateDigest: raftbootstrap.Digest{byte(id)}, DataDigest: expected.DataDigest,
		}}, key)
		require.NoError(t, err)
		m.Members = append(m.Members, p.Evidence)
	}
	manifest, err := raftbootstrap.MarshalBaselineManifest(m)
	require.NoError(t, err)
	return request, raw, manifest, keys
}

func TestBaselineCLIRequest(t *testing.T) {
	t.Parallel()
	r, err := clusterbootstrap.ParseBootstrapBaselineCLI(baselineCLIArgs())
	require.NoError(t, err)
	require.Equal(t, uint64(1), r.MemberID)
	require.Equal(t, raftbootstrap.OperationID{1}, r.OperationID)
	args := []string{"inspect", "-manifest=/offline/baseline.bin", "--expected-config=/offline/expected.json",
		"--member-id=1", "--raft-data-dir=/offline/node-1", "--operation-id=01000000000000000000000000000000"}
	other, err := clusterbootstrap.ParseBootstrapBaselineCLI(args)
	require.NoError(t, err)
	require.Equal(t, r, other)
}

func TestBaselineCLIRejectsRequests(t *testing.T) {
	t.Parallel()
	cases := map[string][]string{
		"empty": nil, "prepare": {"prepare"}, "activate": {"activate"}, "unknown": {"delete"},
		"missing": {"inspect"}, "missing value": {"inspect", "--manifest"},
		"unknown flag":        append(baselineCLIArgs(), "--dry-run"),
		"fake completion":     append(baselineCLIArgs(), "--activate=true"),
		"positional":          append(baselineCLIArgs(), "extra"),
		"duplicate":           append(baselineCLIArgs(), "-member-id=1"),
		"duplicate operation": append(baselineCLIArgs(), "--operation-id=02000000000000000000000000000000"),
		"too large":           {"inspect", strings.Repeat("x", raftbootstrap.MaxFileSize)},
	}
	for name, args := range cases {
		t.Run(name, func(t *testing.T) { _, err := clusterbootstrap.ParseBootstrapBaselineCLI(args); require.Error(t, err) })
	}
	for _, text := range []string{"", "0", "00", "01", "+1", "-1", " 1", "1 ", "0x1", "18446744073709551616"} {
		args := baselineCLIArgs()
		args[len(args)-1] = text
		_, err := clusterbootstrap.ParseBootstrapBaselineCLI(args)
		require.Error(t, err)
	}
	for _, text := range []string{"", strings.Repeat("0", 32), strings.Repeat("A", 32), strings.Repeat("g", 32), strings.Repeat("1", 31)} {
		args := baselineCLIArgs()
		args[len(args)-3] = text
		_, err := clusterbootstrap.ParseBootstrapBaselineCLI(args)
		require.Error(t, err)
	}
	for _, path := range []string{"", "-", "/", "relative", "/offline/../expected.json", "/offline//file", "/offline/file/", "/bad\x00", "/bad\n", string([]byte{'/', 0xff})} {
		args := baselineCLIArgs()
		args[2] = path
		_, err := clusterbootstrap.ParseBootstrapBaselineCLI(args)
		require.Error(t, err)
	}
	args := baselineCLIArgs()
	args[4] = args[2]
	_, err := clusterbootstrap.ParseBootstrapBaselineCLI(args)
	require.Error(t, err)
}

func TestBaselineCLIConfigAndSummary(t *testing.T) {
	t.Parallel()
	request, raw, manifest := baselineCLIFixture(t)
	config, err := json.Marshal(raw)
	require.NoError(t, err)
	c, err := clusterbootstrap.ParseBootstrapBaselineExpectedConfig(config, request.OperationID)
	require.NoError(t, err)
	require.Zero(t, c.Identity.Group)
	require.Equal(t, []uint64{1, 2}, c.Configuration.State.Voters)
	require.Equal(t, []uint64{1, 3}, c.Configuration.State.VotersOutgoing)
	require.Equal(t, []uint64{3}, c.Configuration.State.LearnersNext)
	require.Equal(t, []uint64{4}, c.Configuration.State.Learners)
	summary, err := clusterbootstrap.BootstrapBaselineDryRun(request, config, manifest)
	require.NoError(t, err)
	require.Equal(t, "signatures-verified-dry-run-only", summary.Status)
	require.Equal(t, 4, summary.MemberCount)
	require.Equal(t, raw.Cluster, summary.ClusterUUID)
	require.Zero(t, summary.GroupID)
	require.Equal(t, uint64(8), summary.ConfigurationIndex)
	require.Equal(t, raw.DataDigest, summary.DataDigest)
	require.Equal(t, uint64(12), summary.CommitIndex)
	require.False(t, summary.NativeStateVerified)
	require.False(t, summary.Prepared)
	require.False(t, summary.Activated)
	require.False(t, summary.Mutated)
	digest := sha256.Sum256(manifest)
	require.Equal(t, hex.EncodeToString(digest[:]), summary.ManifestDigest)
	// JSON summary contains no key material or paths.
	encoded, err := json.Marshal(summary)
	require.NoError(t, err)
	require.NotContains(t, string(encoded), "key")
	require.NotContains(t, string(encoded), "/offline")
	raw.Members[0], raw.Members[3] = raw.Members[3], raw.Members[0]
	raw.Members[3].Roles = []string{"outgoing", "voter"}
	config, err = json.Marshal(raw)
	require.NoError(t, err)
	reordered, err := clusterbootstrap.BootstrapBaselineDryRun(request, config, manifest)
	require.NoError(t, err)
	require.Equal(t, summary, reordered)
}

func TestBaselineCLIConfigRejectsMalformedJSON(t *testing.T) {
	t.Parallel()
	request, raw, _ := baselineCLIFixture(t)
	config, err := json.Marshal(raw)
	require.NoError(t, err)
	cases := [][]byte{nil, []byte("null"), []byte("[]"), []byte("{}"), []byte(`{"version":null}`),
		append(bytes.Clone(config), []byte(" {}")...), append(bytes.Clone(config), 'x'),
		bytes.Replace(config, []byte(`"version":1`), []byte(`"version":1,"version":1`), 1),
		bytes.Replace(config, []byte(`"version":1`), []byte(`"Version":1`), 1),
		bytes.Replace(config, []byte(`"version":1`), []byte(`"surprise":1,"version":1`), 1),
		bytes.Replace(config, []byte(`"entry":1`), []byte(`"entry":1,"entry":1`), 1),
		bytes.Replace(config, []byte(`"member_id":"1"`), []byte(`"member_id":"1","member_id":"1"`), 1),
		bytes.Replace(config, []byte(`"group_id":"0"`), []byte(`"group_id":0`), 1),
		bytes.Replace(config, []byte(`"group_id":"0",`), nil, 1),
		bytes.Replace(config, []byte(`"watermark":"0",`), nil, 1),
		[]byte(`{"members":[[[[[0]]]]]}`), bytes.Repeat([]byte{' '}, raftbootstrap.MaxFileSize+1),
		{0xff},
	}
	for i, data := range cases {
		t.Run(string(rune('A'+i)), func(t *testing.T) {
			_, err := clusterbootstrap.ParseBootstrapBaselineExpectedConfig(data, request.OperationID)
			require.Error(t, err)
		})
	}
}

func TestBaselineCLIExpectedContextRejectsInvalidValues(t *testing.T) {
	t.Parallel()
	cases := map[string]func(*clusterbootstrap.BootstrapBaselineExpectedJSON){
		"version": func(r *clusterbootstrap.BootstrapBaselineExpectedJSON) { r.Version++ },
		"cluster": func(r *clusterbootstrap.BootstrapBaselineExpectedJSON) {
			r.Cluster = strings.ToUpper(r.Cluster[:8]) + "X"
		},
		"missing member":   func(r *clusterbootstrap.BootstrapBaselineExpectedJSON) { r.Members = nil },
		"zero member":      func(r *clusterbootstrap.BootstrapBaselineExpectedJSON) { r.Members[0].ID = "0" },
		"duplicate member": func(r *clusterbootstrap.BootstrapBaselineExpectedJSON) { r.Members[1] = r.Members[0] },
		"missing pin":      func(r *clusterbootstrap.BootstrapBaselineExpectedJSON) { r.Members[0].Key = "" },
		"zero pin":         func(r *clusterbootstrap.BootstrapBaselineExpectedJSON) { r.Members[0].Key = strings.Repeat("0", 64) },
		"uppercase pin":    func(r *clusterbootstrap.BootstrapBaselineExpectedJSON) { r.Members[0].Key = strings.Repeat("A", 64) },
		"bad address":      func(r *clusterbootstrap.BootstrapBaselineExpectedJSON) { r.Members[0].Address = "invalid" },
		"unknown role":     func(r *clusterbootstrap.BootstrapBaselineExpectedJSON) { r.Members[0].Roles = []string{"observer"} },
		"duplicate role": func(r *clusterbootstrap.BootstrapBaselineExpectedJSON) {
			r.Members[0].Roles = []string{"voter", "voter"}
		},
		"no role": func(r *clusterbootstrap.BootstrapBaselineExpectedJSON) { r.Members[0].Roles = nil },
		"role conflict": func(r *clusterbootstrap.BootstrapBaselineExpectedJSON) {
			r.Members[0].Roles = []string{"voter", "learner"}
		},
		"no outgoing for next":      func(r *clusterbootstrap.BootstrapBaselineExpectedJSON) { r.Members[2].Roles = []string{"next"} },
		"uncommitted configuration": func(r *clusterbootstrap.BootstrapBaselineExpectedJSON) { r.ConfigurationIndex = "13" },
		"zero configuration":        func(r *clusterbootstrap.BootstrapBaselineExpectedJSON) { r.ConfigurationIndex = "0" },
		"zero commit":               func(r *clusterbootstrap.BootstrapBaselineExpectedJSON) { r.CommitIndex = "0" },
		"zero term":                 func(r *clusterbootstrap.BootstrapBaselineExpectedJSON) { r.CommitTerm = "0" },
		"watermark sentinel":        func(r *clusterbootstrap.BootstrapBaselineExpectedJSON) { r.Watermark = "18446744073709551615" },
		"unsupported minimum":       func(r *clusterbootstrap.BootstrapBaselineExpectedJSON) { r.Minimum.Session = 2 },
		"missing minimum": func(r *clusterbootstrap.BootstrapBaselineExpectedJSON) {
			r.Minimum = clusterbootstrap.BootstrapBaselineMinimumJSON{}
		},
		"missing data":   func(r *clusterbootstrap.BootstrapBaselineExpectedJSON) { r.DataDigest = "" },
		"leading zero":   func(r *clusterbootstrap.BootstrapBaselineExpectedJSON) { r.Group = "00" },
		"signed integer": func(r *clusterbootstrap.BootstrapBaselineExpectedJSON) { r.Watermark = "+0" },
		"overflow":       func(r *clusterbootstrap.BootstrapBaselineExpectedJSON) { r.CommitIndex = "18446744073709551616" },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			request, raw, _ := baselineCLIFixture(t)
			mutate(&raw)
			data, err := json.Marshal(raw)
			require.NoError(t, err)
			_, err = clusterbootstrap.ParseBootstrapBaselineExpectedConfig(data, request.OperationID)
			require.Error(t, err)
		})
	}
}

func FuzzBootstrapBaselineExpectedConfig(f *testing.F) {
	request, raw, _ := baselineCLIFixture(f)
	data, err := json.Marshal(raw)
	require.NoError(f, err)
	f.Add(data)
	f.Add([]byte(`{}`))
	f.Fuzz(func(t *testing.T, data []byte) {
		c, err := clusterbootstrap.ParseBootstrapBaselineExpectedConfig(data, request.OperationID)
		if err == nil {
			require.Equal(t, request.OperationID, c.OperationID)
			require.Equal(t, raftbootstrap.BaselineVersion, c.Version)
			_, err := raftbootstrap.MarshalIntent(raftbootstrap.Intent{Identity: c.Identity, BaselineKind: raftbootstrap.Accepted, Baseline: c.Configuration, Minimum: c.Minimum})
			require.NoError(t, err)
		}
	})
}

func TestBaselineCLIDryRunIndependentExpectation(t *testing.T) {
	t.Parallel()
	for _, field := range []string{"operation", "member", "pin", "cut", "group", "data", "path", "manifest"} {
		t.Run(field, func(t *testing.T) {
			request, raw, manifest := baselineCLIFixture(t)
			switch field {
			case "operation":
				request.OperationID[0]++
			case "member":
				request.MemberID = 99
			case "pin":
				raw.Members[0].Key = strings.Repeat("02", 32)
			case "cut":
				raw.CommitIndex = "13"
			case "group":
				raw.Group = "1"
			case "data":
				raw.DataDigest = strings.Repeat("02", 32)
			case "path":
				request.ManifestPath = request.ExpectedConfigPath
			case "manifest":
				manifest[len(manifest)-1]++
			}
			config, err := json.Marshal(raw)
			require.NoError(t, err)
			summary, err := clusterbootstrap.BootstrapBaselineDryRun(request, config, manifest)
			require.Error(t, err)
			require.Equal(t, clusterbootstrap.BootstrapBaselineDryRunSummary{}, summary)
		})
	}
	request, raw, manifest := baselineCLIFixture(t)
	config, err := json.Marshal(raw)
	require.NoError(t, err)
	_, err = clusterbootstrap.BootstrapBaselineDryRun(request, config, make([]byte, raftbootstrap.MaxFileSize))
	require.Error(t, err)
	_, err = clusterbootstrap.BootstrapBaselineDryRun(request, make([]byte, raftbootstrap.MaxFileSize+1), manifest)
	require.Error(t, err)
}
