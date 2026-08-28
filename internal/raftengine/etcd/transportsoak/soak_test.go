package transportsoak

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestRunProducesVerifiableMultiGroupEvidence(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	evidence, err := Run(ctx, Config{
		Groups:                   2,
		NodesPerGroup:            3,
		MessagesPerLink:          2,
		RegularPayloadBytes:      128,
		BackpressurePayloadBytes: 1 << 20,
		SnapshotPayloadBytes:     4096,
	})
	require.NoError(t, err)
	require.NoError(t, Verify(evidence))
	require.Len(t, evidence.Groups, 2)
}

func TestVerifyRejectsMissingFaultEvidence(t *testing.T) {
	t.Parallel()
	evidence := validEvidence()
	evidence.Groups[0].DisconnectErrors = 0

	err := Verify(evidence)
	require.ErrorContains(t, err, "no disconnect error evidence")
}

func TestVerifyRejectsIncompleteOrInconsistentEvidence(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		mutate func(*Evidence)
		want   string
	}{
		{name: "schema", mutate: func(e *Evidence) { e.SchemaVersion++ }, want: "unsupported evidence schema"},
		{name: "timestamps", mutate: func(e *Evidence) { e.FinishedAt = e.StartedAt.Add(-time.Second) }, want: "timestamps are missing or out of order"},
		{name: "result", mutate: func(e *Evidence) { e.Result = "unknown" }, want: `evidence result is "unknown"`},
		{name: "group count", mutate: func(e *Evidence) { e.Groups = e.Groups[:1] }, want: "evidence has 1 groups"},
		{name: "group range", mutate: func(e *Evidence) { e.Groups[0].GroupID = 3 }, want: "outside expected range"},
		{name: "duplicate group", mutate: func(e *Evidence) { e.Groups[1].GroupID = 1 }, want: "duplicate group 1"},
		{name: "duplicate node", mutate: func(e *Evidence) { e.Groups[0].Nodes[1].Node = 1 }, want: "duplicate node 1"},
		{name: "empty address", mutate: func(e *Evidence) { e.Groups[0].Nodes[0].Address = "" }, want: "empty address"},
		{name: "duplicate address", mutate: func(e *Evidence) { e.Groups[1].Nodes[0].Address = e.Groups[0].Nodes[0].Address }, want: "duplicate transport address"},
		{name: "missing directed link", mutate: func(e *Evidence) { e.Groups[0].Nodes[0].RegularReceivedBySender[2] = 1 }, want: "link 2 -> 1 delivered 1"},
		{name: "unexpected sender", mutate: func(e *Evidence) { e.Groups[0].Nodes[0].RegularReceivedBySender[99] = 1 }, want: "unexpected sender 99"},
		{name: "total mismatch", mutate: func(e *Evidence) { e.Groups[0].Nodes[0].RegularReceived++ }, want: "differs from sender total"},
		{name: "stream opens", mutate: func(e *Evidence) { e.Groups[0].Nodes[0].SendStreamOpens = 1 }, want: "opened only 1 peer streams"},
		{name: "stream messages", mutate: func(e *Evidence) { e.Groups[0].Nodes[0].SendStreamMessages = 3 }, want: "streamed 3 regular messages"},
		{name: "snapshot", mutate: func(e *Evidence) { e.Groups[0].Nodes[0].SnapshotPayloadBytes = 0 }, want: "snapshot evidence incomplete"},
		{name: "backpressure", mutate: func(e *Evidence) { e.Groups[0].BackpressureErrors = 0 }, want: "no backpressure error evidence"},
		{name: "recovery", mutate: func(e *Evidence) { e.Groups[0].RecoveryMessages = 1 }, want: "only 1 recovery messages"},
		{name: "reconnect", mutate: func(e *Evidence) { e.Groups[0].Nodes[0].SendStreamReconnects = 0 }, want: "no successful stream reconnect"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			evidence := validEvidence()
			tt.mutate(&evidence)
			require.ErrorContains(t, Verify(evidence), tt.want)
		})
	}
}

func TestEvidenceRoundTrip(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "evidence.json")
	want := validEvidence()
	require.NoError(t, WriteEvidence(path, want))
	got, err := ReadEvidence(path)
	require.NoError(t, err)
	require.Equal(t, want, got)
}

func TestMetricsVerifierRequiresDistinctActiveNodes(t *testing.T) {
	t.Parallel()
	repoRoot := repositoryRoot(t)
	script := filepath.Join(repoRoot, "scripts", "verify-raft-streaming-multigroup-metrics.sh")
	dir := t.TempDir()
	files := make([]string, 3)
	for i := range files {
		files[i] = filepath.Join(dir, fmt.Sprintf("n%d.prom", i+1))
		require.NoError(t, os.WriteFile(files[i], []byte(metricsFixture(fmt.Sprintf("n%d", i+1), i == 0, i == 0, true)), 0o600))
	}

	out, err := runMetricsVerifier(t, script, files)
	require.NoError(t, err, string(out))
	require.Contains(t, string(out), "group=3 activity_nodes=3")
	require.Contains(t, string(out), "result=pass")

	require.NoError(t, os.WriteFile(files[2], []byte(metricsFixture("n2", false, false, true)), 0o600))
	out, err = runMetricsVerifier(t, script, files)
	require.Error(t, err)
	require.Contains(t, string(out), "duplicate node_id n2")

	require.NoError(t, os.WriteFile(files[2], []byte(metricsFixture("n3", false, false, false)), 0o600))
	out, err = runMetricsVerifier(t, script, files)
	require.Error(t, err)
	require.Contains(t, string(out), "group 1 has stream activity on only 2 nodes")

	require.NoError(t, os.WriteFile(files[0], []byte(metricsFixture("n1", true, false, true)), 0o600))
	require.NoError(t, os.WriteFile(files[2], []byte(metricsFixture("n3", false, false, true)), 0o600))
	out, err = runMetricsVerifier(t, script, files)
	require.Error(t, err)
	require.Contains(t, string(out), "group 1 missing backpressure evidence")

	require.NoError(t, os.WriteFile(files[0], []byte(metricsFixture("n1", true, true, true)+"elastickv_raft_send_stream_messages_total{group=\"1\",node_address=\"n1:50051\",node_id=\"n1\"} NaN\n"), 0o600))
	out, err = runMetricsVerifier(t, script, files)
	require.Error(t, err)
	require.Contains(t, string(out), "invalid metric value")
}

func validEvidence() Evidence {
	started := time.Unix(1_700_000_000, 0).UTC()
	evidence := Evidence{
		SchemaVersion: EvidenceSchemaVersion,
		StartedAt:     started,
		FinishedAt:    started.Add(time.Second),
		Config: Config{
			Groups:                   2,
			NodesPerGroup:            3,
			MessagesPerLink:          2,
			RegularPayloadBytes:      128,
			BackpressurePayloadBytes: 1 << 20,
			SnapshotPayloadBytes:     4096,
		},
		Result: "pass",
	}
	for groupID := 1; groupID <= evidence.Config.Groups; groupID++ {
		group := GroupEvidence{
			GroupID:            groupID,
			DisconnectErrors:   1,
			BackpressureErrors: 1,
			RecoveryMessages:   2,
		}
		for nodeID := 1; nodeID <= evidence.Config.NodesPerGroup; nodeID++ {
			bySender := make(map[uint64]uint64, evidence.Config.NodesPerGroup-1)
			for sender := 1; sender <= evidence.Config.NodesPerGroup; sender++ {
				if sender != nodeID {
					bySender[positiveIntToUint64(sender)] = positiveIntToUint64(evidence.Config.MessagesPerLink)
				}
			}
			reconnects := uint64(0)
			if nodeID == 1 {
				reconnects = 1
			}
			group.Nodes = append(group.Nodes, NodeEvidence{
				Node:                    positiveIntToUint64(nodeID),
				Address:                 fmt.Sprintf("127.0.0.1:%d", 50000+groupID*10+nodeID),
				RegularReceived:         positiveIntToUint64((evidence.Config.NodesPerGroup - 1) * evidence.Config.MessagesPerLink),
				RegularReceivedBySender: bySender,
				SnapshotsReceived:       1,
				SendStreamOpens:         positiveIntToUint64(evidence.Config.NodesPerGroup - 1),
				SendStreamReconnects:    reconnects,
				SendStreamMessages:      positiveIntToUint64((evidence.Config.NodesPerGroup - 1) * evidence.Config.MessagesPerLink),
				SnapshotStreamSends:     1,
				SnapshotPayloadBytes:    positiveIntToUint64(evidence.Config.SnapshotPayloadBytes),
			})
		}
		evidence.Groups = append(evidence.Groups, group)
	}
	return evidence
}

func repositoryRoot(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	require.True(t, ok)
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", "..", ".."))
}

func runMetricsVerifier(t *testing.T, script string, files []string) ([]byte, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	args := append([]string{script}, files...)
	cmd := exec.CommandContext(ctx, "bash", args...) //nolint:gosec // script and files are repository/test-controlled paths.
	cmd.Env = append(os.Environ(), "EXPECTED_GROUPS=1,2,3", "MIN_NODES=3")
	return cmd.CombinedOutput()
}

func metricsFixture(nodeID string, reconnect, backpressure, active bool) string {
	var out strings.Builder
	messages := 0
	if active {
		messages = 10
	}
	for groupID := 1; groupID <= 3; groupID++ {
		labels := fmt.Sprintf(`group="%d",node_address="%s:50051",node_id="%s"`, groupID, nodeID, nodeID)
		fmt.Fprintf(&out, "elastickv_raft_send_stream_opens_total{%s} 2\n", labels)
		fmt.Fprintf(&out, "elastickv_raft_send_stream_reconnects_total{%s} %d\n", labels, boolCount(reconnect))
		fmt.Fprintf(&out, "elastickv_raft_send_stream_messages_total{%s} %d\n", labels, messages)
		fmt.Fprintf(&out, "elastickv_raft_snapshot_stream_sends_total{%s} 1\n", labels)
		fmt.Fprintf(&out, "elastickv_raft_snapshot_stream_payload_bytes_total{%s} 4096\n", labels)
		fmt.Fprintf(&out, "elastickv_raft_dispatch_errors_by_code_total{code=\"DeadlineExceeded\",%s} %d\n", labels, boolCount(backpressure))
	}
	return out.String()
}

func boolCount(value bool) int {
	if value {
		return 1
	}
	return 0
}
