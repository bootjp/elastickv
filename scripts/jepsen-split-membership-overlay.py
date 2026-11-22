#!/usr/bin/env python3
"""Add process-local observation and scheduling hooks to a test-only build."""

import json
import sys
from pathlib import Path


repo = Path(sys.argv[1]).resolve()
out = Path(sys.argv[2]).resolve()
out.mkdir()
replace = {}


def edit(name, replacements, appended=""):
    path = repo / name
    source = path.read_text()
    for old, new in replacements:
        if source.count(old) != 1:
            raise SystemExit(f"fixture anchor changed: {name}: {old!r}")
        source = source.replace(old, new, 1)
    overlay = out / name
    overlay.parent.mkdir(parents=True, exist_ok=True)
    overlay.write_text(source + appended)
    replace[str(path)] = str(overlay)


edit(
    "adapter/split_job_runner.go",
    [
        ('\t"io"\n', '\t"fmt"\n\t"io"\n\t"os"\n\t"path/filepath"\n'),
        (
            "\treturn s.commitSplitJobCutover(ctx, snapshot, sourceRoute, job, expectedVersion)",
            """	evidence, err := s.testPauseMembershipCutover(ctx, job, snapshot.Version)
	if err != nil {
		return err
	}
	if err := s.commitSplitJobCutover(ctx, snapshot, sourceRoute, job, expectedVersion); err != nil {
		return err
	}
	if evidence != nil {
		return os.WriteFile(filepath.Join(os.Getenv("SPLIT_MEMBERSHIP_FIXTURE"), "published-attempt"), evidence, 0600)
	}
	return nil""",
        ),
    ],
    """
func (s *DistributionServer) testPauseMembershipCutover(ctx context.Context, job distribution.SplitJob, version uint64) ([]byte, error) {
	dir := os.Getenv("SPLIT_MEMBERSHIP_FIXTURE")
	if dir == "" || job.OnlineCaptureVersion == 0 {
		return nil, nil
	}
	release := filepath.Join(dir, "release-publish")
	if _, err := os.Stat(release); err == nil {
		return nil, nil
	}
	members, err := s.splitTransferApplyMembers(ctx, job.TargetGroupID)
	if err != nil {
		return nil, err
	}
	marker := filepath.Join(dir, "before-publish")
	tmp := marker + "." + os.Getenv("SPLIT_MEMBERSHIP_NODE")
	data := fmt.Appendf(nil, "%d %d %d %d %d\\n", job.JobID, version, members.ConfigurationIndex, job.OnlineSealSequence, time.Now().UnixNano())
	if err := os.WriteFile(tmp, data, 0600); err != nil {
		return nil, err
	}
	if err := os.Rename(tmp, marker); err != nil {
		return nil, err
	}
	bounded, cancel := context.WithTimeout(ctx, 90*time.Second)
	defer cancel()
	poll := time.NewTicker(20*time.Millisecond)
	defer poll.Stop()
	for {
		if _, err := os.Stat(release); err == nil {
			return data, bounded.Err()
		} else if !os.IsNotExist(err) {
			return nil, err
		}
		select {
		case <-bounded.Done():
			return nil, bounded.Err()
		case <-poll.C:
		}
	}
}
""",
)

edit(
    "distribution/engine.go",
    [
        ('\t"bytes"\n', '\t"bytes"\n\t"fmt"\n\t"os"\n\t"path/filepath"\n\t"strconv"\n\t"strings"\n'),
        (
            "func (e *Engine) ApplySnapshot(snapshot CatalogSnapshot) error {",
            """func (e *Engine) ApplySnapshot(snapshot CatalogSnapshot) error {
	if err := e.testHoldOldRoute(snapshot.Version); err != nil {
		return err
	}""",
        ),
        (
            "func (e *Engine) ApplyDelta(delta CatalogDelta) error {",
            """func (e *Engine) ApplyDelta(delta CatalogDelta) error {
	if err := e.testHoldOldRoute(delta.Version); err != nil {
		return err
	}""",
        ),
        ("\te.catalogVersion = snapshot.Version", "\te.catalogVersion = snapshot.Version\n\ttestRecordRouteCatchup(snapshot.Version)"),
        ("\te.catalogVersion = delta.Version", "\te.catalogVersion = delta.Version\n\ttestRecordRouteCatchup(delta.Version)"),
    ],
    """
func testRecordRouteCatchup(version uint64) {
	dir := os.Getenv("SPLIT_MEMBERSHIP_FIXTURE")
	if dir == "" {
		return
	}
	switch os.Getenv("SPLIT_MEMBERSHIP_NODE") {
	case "n4":
		_ = os.WriteFile(filepath.Join(dir, "caught-n4"), fmt.Appendf(nil, "%d\\n", version), 0600)
	case "n3":
		testRecordOldIngressCatchup(dir, version)
	}
}

func testRecordOldIngressCatchup(dir string, version uint64) {
	if _, err := os.Stat(filepath.Join(dir, "held-n3")); err == nil {
		if _, held := os.Stat(filepath.Join(dir, "hold-n3")); os.IsNotExist(held) {
			_ = os.WriteFile(filepath.Join(dir, "caught-n3"), fmt.Appendf(nil, "%d\\n", version), 0600)
		}
	}
}

func (e *Engine) testHoldOldRoute(incoming uint64) error {
	dir := os.Getenv("SPLIT_MEMBERSHIP_FIXTURE")
	node := os.Getenv("SPLIT_MEMBERSHIP_NODE")
	if dir == "" || node != "n3" {
		return nil
	}
	raw, err := os.ReadFile(filepath.Join(dir, "hold-n3"))
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	limit, err := strconv.ParseUint(strings.TrimSpace(string(raw)), 10, 64)
	if err != nil {
		return err
	}
	if incoming <= limit {
		return nil
	}
	route, version, found := e.GetRouteWithVersion([]byte("jepsen-grpc/raw/1"))
	if !found {
		return errors.New("fixture moving route missing")
	}
	record := fmt.Appendf(nil, "%d %d %d\\n", version, route.GroupID, incoming)
	marker := filepath.Join(dir, "held-n3")
	f, err := os.OpenFile(marker, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err == nil {
		if _, err := f.Write(record); err != nil {
			_ = f.Close()
			return err
		}
		if err := f.Close(); err != nil {
			return err
		}
	} else if !os.IsExist(err) {
		return err
	}
	return errors.New("fixture held newer route on stale ingress n3")
}
""",
)

edit(
    "internal/noderuntime/peers.go",
    [
        ('\t"maps"\n', '\t"maps"\n\t"os"\n'),
        (
            "\tservers := cfg.ServersForGroup(groupID)",
            """	servers := cfg.ServersForGroup(groupID)
	if os.Getenv("SPLIT_MEMBERSHIP_NODE") == "n4" &&
	   os.Getenv("SPLIT_MEMBERSHIP_FIXTURE") != "" {
		// The normal CLI supports discovery-only join for one group. In
		// this fixture both groups use the configured peers for discovery
		// without forming a new group. All membership is applied by Raft.
		return false, servers, nil
	}""",
        ),
    ],
)
if sys.argv[3] == "partition":
    edit(
        "internal/raftengine/etcd/engine.go",
        [
            (
                "func (e *Engine) dispatchTransport(ctx context.Context, req dispatchRequest) error {",
                """func (e *Engine) dispatchTransport(ctx context.Context, req dispatchRequest) error {
	if err := e.testSplitMembershipPartition(req.msg); err != nil {
		return err
	}""",
            ),
        ],
        """
// Intercept only group-2 Raft traffic across the n4 partition. Public and
// admin RPCs remain connected so the test can inspect the isolated replica.
func (e *Engine) testSplitMembershipPartition(msg raftpb.Message) error {
	candidate := DeriveNodeID("n4")
	if e.groupID != 2 || (e.nodeID != candidate && msg.GetTo() != candidate) {
		return nil
	}
	dir := os.Getenv("SPLIT_MEMBERSHIP_FIXTURE")
	if _, err := os.Stat(filepath.Join(dir, "partition-n4")); err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	if e.nodeID != candidate {
		marker := filepath.Join(dir, "partition-drop-" + os.Getenv("SPLIT_MEMBERSHIP_NODE"))
		if f, err := os.OpenFile(marker, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600); err == nil {
			_, _ = f.WriteString("group 2 to n4\\n")
			_ = f.Close()
		}
	}
	return errors.New("split membership test dropped partitioned Raft traffic")
}
""",
    )

(out / "overlay.json").write_text(json.dumps({"Replace": replace}))
