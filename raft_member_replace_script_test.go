package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRaftMemberReplaceScriptCompletesFencedLearnerLifecycle(t *testing.T) {
	repoRoot, err := os.Getwd()
	require.NoError(t, err)
	stateDir := t.TempDir()
	fakeBin := filepath.Join(stateDir, "bin")
	require.NoError(t, os.Mkdir(fakeBin, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(stateDir, "member"), []byte("voter\n"), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(stateDir, "config-index"), []byte("10\n"), 0o600))

	raftadmin := writeExecutableTestFile(t, fakeBin, "raftadmin", fakeRaftadminScript)
	ssh := writeExecutableTestFile(t, fakeBin, "ssh", fakeReplacementSSHScript)
	rolling := writeExecutableTestFile(t, fakeBin, "rolling-update", fakeReplacementRollingScript)
	envFile := filepath.Join(stateDir, "deploy.env")
	require.NoError(t, os.WriteFile(envFile, []byte(strings.Join([]string{
		"NODES=n1=127.0.0.1,n2=127.0.0.2,n3=127.0.0.3",
		"RAFT_PORT=50051",
		"SSH_USER=tester",
		"CONTAINER_NAME=elastickv",
		"DATA_DIR=/var/lib/./elastickv/.",
		"TARGET_NODE=n3",
		"REPLACEMENT_CONFIRM=n3",
		"REPLACEMENT_VERIFY_COMMAND=false",
		"REPLACEMENT_FENCE_VERIFY_SECONDS=999",
		"REPLACEMENT_STABILITY_SECONDS=999",
		"REPLACEMENT_TIMEOUT_SECONDS=999",
		"REPLACEMENT_POLL_SECONDS=999",
		"REPLACEMENT_KEEP_STATE=false",
		"DRY_RUN=true",
	}, "\n")+"\n"), 0o600))

	stateFile := filepath.Join(stateDir, "replace.state")
	output := runReplacementScript(t, repoRoot, []string{
		"TARGET_NODE=n2",
		"REPLACEMENT_CONFIRM=n2",
		"REPLACEMENT_FENCE_MODE=container",
		"REPLACEMENT_FENCE_VERIFY_SECONDS=1",
		"REPLACEMENT_VERIFY_COMMAND=touch $FAKE_STATE_DIR/verified",
		"REPLACEMENT_STABILITY_SECONDS=1",
		"REPLACEMENT_TIMEOUT_SECONDS=5",
		"REPLACEMENT_POLL_SECONDS=1",
		"REPLACEMENT_STATE_FILE=" + stateFile,
		"ROLLING_UPDATE_ENV_FILE=" + envFile,
		"ROLLING_UPDATE_SCRIPT=" + rolling,
		"RAFTADMIN_BIN=" + raftadmin,
		"SSH_BIN=" + ssh,
		"FAKE_STATE_DIR=" + stateDir,
	})
	require.Contains(t, output, "replacement completed: target=n2")
	require.FileExists(t, filepath.Join(stateDir, "verified"))
	require.FileExists(t, filepath.Join(stateDir, "data-reset"))
	require.Equal(t, "voter\n", mustReadTestFile(t, filepath.Join(stateDir, "member")))
	require.Equal(t, "n2:n2\nnormal:n2\n", mustReadTestFile(t, filepath.Join(stateDir, "rollouts")))

	state := mustReadTestFile(t, stateFile)
	require.Contains(t, state, "stage=9\n")
	require.Contains(t, state, "expected_voters=n1,n2,n3\n")
	require.Contains(t, state, "expected_members=n1|127.0.0.1:50051|voter,n2|127.0.0.2:50051|voter,n3|127.0.0.3:50051|voter\n")
	require.Contains(t, state, "expected_config_index=13\n")
	require.Contains(t, state, "min_catch_up_index=100\n")
	require.Contains(t, state, "data_mode=archive\n")
	require.Contains(t, state, "data_dir=/var/lib/elastickv\n")
	require.Contains(t, state, "archive_path=/var/lib/elastickv.replacement-n2-")
	require.NotContains(t, state, "archive_path=/var/lib/elastickv/.replacement")
	require.Equal(t, "stop\nreset-data\n", mustReadTestFile(t, filepath.Join(stateDir, "remote-actions")))

	resumeCmd := exec.CommandContext(t.Context(), "bash", "scripts/raft-member-replace.sh", "--execute")
	resumeCmd.Dir = repoRoot
	resumeCmd.Env = append(os.Environ(),
		"TARGET_NODE=n2",
		"REPLACEMENT_CONFIRM=n2",
		"REPLACEMENT_FENCE_MODE=container",
		"REPLACEMENT_FENCE_VERIFY_SECONDS=1",
		"REPLACEMENT_VERIFY_COMMAND=true",
		"REPLACEMENT_STABILITY_SECONDS=1",
		"REPLACEMENT_TIMEOUT_SECONDS=5",
		"REPLACEMENT_POLL_SECONDS=1",
		"REPLACEMENT_STATE_FILE="+stateFile,
		"ROLLING_UPDATE_ENV_FILE="+envFile,
		"ROLLING_UPDATE_SCRIPT="+rolling,
		"RAFTADMIN_BIN="+raftadmin,
		"SSH_BIN="+ssh,
		"FAKE_STATE_DIR="+stateDir,
	)
	resumeOutput, resumeErr := resumeCmd.CombinedOutput()
	require.Error(t, resumeErr)
	require.Contains(t, string(resumeOutput), "replacement state is already complete")
	require.Equal(t, "n2:n2\nnormal:n2\n", mustReadTestFile(t, filepath.Join(stateDir, "rollouts")), "completed resume must not repeat deployment")
}

func TestRaftMemberReplaceScriptStopsProcessAfterExternalFence(t *testing.T) {
	repoRoot, err := os.Getwd()
	require.NoError(t, err)
	stateDir := t.TempDir()
	fakeBin := filepath.Join(stateDir, "bin")
	require.NoError(t, os.Mkdir(fakeBin, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(stateDir, "member"), []byte("voter\n"), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(stateDir, "config-index"), []byte("10\n"), 0o600))

	raftadmin := writeExecutableTestFile(t, fakeBin, "raftadmin", fakeRaftadminScript)
	ssh := writeExecutableTestFile(t, fakeBin, "ssh", fakeReplacementSSHScript)
	rolling := writeExecutableTestFile(t, fakeBin, "rolling-update", fakeReplacementRollingScript)
	envFile := filepath.Join(stateDir, "deploy.env")
	require.NoError(t, os.WriteFile(envFile, []byte("NODES=n1=127.0.0.1,n2=127.0.0.2,n3=127.0.0.3\n"), 0o600))

	output := runReplacementScript(t, repoRoot, []string{
		"TARGET_NODE=n2",
		"REPLACEMENT_CONFIRM=n2",
		"REPLACEMENT_FENCE_MODE=external",
		`REPLACEMENT_FENCE_COMMAND=touch "$FAKE_STATE_DIR/external-fenced"; touch "$FAKE_STATE_DIR/down-n2"`,
		"REPLACEMENT_FENCE_VERIFY_SECONDS=1",
		"REPLACEMENT_VERIFY_COMMAND=true",
		"REPLACEMENT_STABILITY_SECONDS=1",
		"REPLACEMENT_TIMEOUT_SECONDS=5",
		"REPLACEMENT_POLL_SECONDS=1",
		"REPLACEMENT_STATE_FILE=" + filepath.Join(stateDir, "replace.state"),
		"ROLLING_UPDATE_ENV_FILE=" + envFile,
		"ROLLING_UPDATE_SCRIPT=" + rolling,
		"RAFTADMIN_BIN=" + raftadmin,
		"SSH_BIN=" + ssh,
		"FAKE_STATE_DIR=" + stateDir,
	})
	require.Contains(t, output, "external fence verified; stopping the target process")
	require.FileExists(t, filepath.Join(stateDir, "external-fenced"))
	require.Equal(t, "stop\nreset-data\n", mustReadTestFile(t, filepath.Join(stateDir, "remote-actions")))
}

func TestRaftMemberReplaceScriptRequiresInvocationControls(t *testing.T) {
	repoRoot, err := os.Getwd()
	require.NoError(t, err)
	stateDir := t.TempDir()
	rolling := writeExecutableTestFile(t, stateDir, "rolling-update", "#!/usr/bin/env bash\nexit 0\n")
	envFile := filepath.Join(stateDir, "deploy.env")
	require.NoError(t, os.WriteFile(envFile, []byte(strings.Join([]string{
		"NODES=n1=127.0.0.1,n2=127.0.0.2,n3=127.0.0.3",
		"TARGET_NODE=n2",
		"REPLACEMENT_CONFIRM=n2",
		"REPLACEMENT_FENCE_MODE=container",
		"REPLACEMENT_VERIFY_COMMAND=true",
	}, "\n")+"\n"), 0o600))

	cmd := exec.CommandContext(t.Context(), "bash", "scripts/raft-member-replace.sh", "--execute")
	cmd.Dir = repoRoot
	cmd.Env = append(os.Environ(),
		"ROLLING_UPDATE_ENV_FILE="+envFile,
		"ROLLING_UPDATE_SCRIPT="+rolling,
	)
	output, err := cmd.CombinedOutput()
	require.Error(t, err)
	require.Contains(t, string(output), "TARGET_NODE is required")
}

func TestRaftMemberReplaceScriptRejectsConfigChangeAfterPreflight(t *testing.T) {
	repoRoot, err := os.Getwd()
	require.NoError(t, err)
	stateDir := t.TempDir()
	fakeBin := filepath.Join(stateDir, "bin")
	require.NoError(t, os.Mkdir(fakeBin, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(stateDir, "member"), []byte("voter\n"), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(stateDir, "config-index"), []byte("10\n"), 0o600))

	raftadmin := writeExecutableTestFile(t, fakeBin, "raftadmin", fakeRaftadminScript)
	ssh := writeExecutableTestFile(t, fakeBin, "ssh", fakeReplacementSSHScript)
	rolling := writeExecutableTestFile(t, fakeBin, "rolling-update", fakeReplacementRollingScript)
	envFile := filepath.Join(stateDir, "deploy.env")
	require.NoError(t, os.WriteFile(envFile, []byte("NODES=n1=127.0.0.1,n2=127.0.0.2,n3=127.0.0.3\n"), 0o600))

	cmd := exec.CommandContext(t.Context(), "bash", "scripts/raft-member-replace.sh", "--execute")
	cmd.Dir = repoRoot
	cmd.Env = append(os.Environ(),
		"TARGET_NODE=n2",
		"REPLACEMENT_CONFIRM=n2",
		"REPLACEMENT_FENCE_MODE=container",
		"REPLACEMENT_FENCE_VERIFY_SECONDS=1",
		"REPLACEMENT_VERIFY_COMMAND=true",
		"REPLACEMENT_TIMEOUT_SECONDS=5",
		"REPLACEMENT_POLL_SECONDS=1",
		"REPLACEMENT_STATE_FILE="+filepath.Join(stateDir, "replace.state"),
		"ROLLING_UPDATE_ENV_FILE="+envFile,
		"ROLLING_UPDATE_SCRIPT="+rolling,
		"RAFTADMIN_BIN="+raftadmin,
		"SSH_BIN="+ssh,
		"FAKE_STATE_DIR="+stateDir,
		"FAKE_BUMP_CONFIG_ON_FENCE=true",
	)
	output, err := cmd.CombinedOutput()
	require.Error(t, err)
	require.Contains(t, string(output), "remove_server configuration index changed: expected=10 actual=11")
	require.Equal(t, "voter\n", mustReadTestFile(t, filepath.Join(stateDir, "member")))
}

func TestRaftMemberReplaceScriptRejectsInsufficientSurvivingQuorum(t *testing.T) {
	repoRoot, err := os.Getwd()
	require.NoError(t, err)
	stateDir := t.TempDir()
	fakeBin := filepath.Join(stateDir, "bin")
	require.NoError(t, os.Mkdir(fakeBin, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(stateDir, "member"), []byte("voter\n"), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(stateDir, "config-index"), []byte("10\n"), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(stateDir, "candidate-n3"), nil, 0o600))

	raftadmin := writeExecutableTestFile(t, fakeBin, "raftadmin", fakeRaftadminScript)
	ssh := writeExecutableTestFile(t, fakeBin, "ssh", fakeReplacementSSHScript)
	rolling := writeExecutableTestFile(t, fakeBin, "rolling-update", fakeReplacementRollingScript)
	envFile := filepath.Join(stateDir, "deploy.env")
	require.NoError(t, os.WriteFile(envFile, []byte("NODES=n1=127.0.0.1,n2=127.0.0.2,n3=127.0.0.3\n"), 0o600))

	cmd := exec.CommandContext(t.Context(), "bash", "scripts/raft-member-replace.sh", "--execute")
	cmd.Dir = repoRoot
	cmd.Env = append(os.Environ(),
		"TARGET_NODE=n2",
		"REPLACEMENT_CONFIRM=n2",
		"REPLACEMENT_FENCE_MODE=container",
		"REPLACEMENT_VERIFY_COMMAND=true",
		"REPLACEMENT_STATE_FILE="+filepath.Join(stateDir, "replace.state"),
		"ROLLING_UPDATE_ENV_FILE="+envFile,
		"ROLLING_UPDATE_SCRIPT="+rolling,
		"RAFTADMIN_BIN="+raftadmin,
		"SSH_BIN="+ssh,
		"FAKE_STATE_DIR="+stateDir,
	)
	output, err := cmd.CombinedOutput()
	require.Error(t, err)
	require.Contains(t, string(output), "only 1 non-target voters are reachable; 2 are required")
	require.NoFileExists(t, filepath.Join(stateDir, "down-n2"), "fencing must not start without surviving quorum")
}

func TestRaftMemberReplaceScriptResumesAfterLearnerCommitResponseLoss(t *testing.T) {
	repoRoot, err := os.Getwd()
	require.NoError(t, err)
	stateDir := t.TempDir()
	fakeBin := filepath.Join(stateDir, "bin")
	require.NoError(t, os.Mkdir(fakeBin, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(stateDir, "member"), []byte("voter\n"), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(stateDir, "config-index"), []byte("10\n"), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(stateDir, "fail-add-once"), nil, 0o600))

	raftadmin := writeExecutableTestFile(t, fakeBin, "raftadmin", fakeRaftadminScript)
	ssh := writeExecutableTestFile(t, fakeBin, "ssh", fakeReplacementSSHScript)
	rolling := writeExecutableTestFile(t, fakeBin, "rolling-update", fakeReplacementRollingScript)
	envFile := filepath.Join(stateDir, "deploy.env")
	require.NoError(t, os.WriteFile(envFile, []byte("NODES=n1=127.0.0.1,n2=127.0.0.2,n3=127.0.0.3\n"), 0o600))
	stateFile := filepath.Join(stateDir, "replace.state")
	commandEnv := []string{
		"TARGET_NODE=n2",
		"REPLACEMENT_CONFIRM=n2",
		"REPLACEMENT_FENCE_MODE=container",
		"REPLACEMENT_FENCE_VERIFY_SECONDS=1",
		"REPLACEMENT_VERIFY_COMMAND=true",
		"REPLACEMENT_STABILITY_SECONDS=1",
		"REPLACEMENT_TIMEOUT_SECONDS=5",
		"REPLACEMENT_POLL_SECONDS=1",
		"REPLACEMENT_STATE_FILE=" + stateFile,
		"ROLLING_UPDATE_ENV_FILE=" + envFile,
		"ROLLING_UPDATE_SCRIPT=" + rolling,
		"RAFTADMIN_BIN=" + raftadmin,
		"SSH_BIN=" + ssh,
		"FAKE_STATE_DIR=" + stateDir,
	}

	cmd := exec.CommandContext(t.Context(), "bash", "scripts/raft-member-replace.sh", "--execute")
	cmd.Dir = repoRoot
	cmd.Env = append(os.Environ(), commandEnv...)
	output, err := cmd.CombinedOutput()
	require.Error(t, err, "%s", output)
	require.Equal(t, "learner\n", mustReadTestFile(t, filepath.Join(stateDir, "member")), "the simulated RPC response is lost after commit")
	interruptedState := mustReadTestFile(t, stateFile)
	require.Contains(t, interruptedState, "stage=5\n")
	require.Contains(t, interruptedState, "min_catch_up_index=100\n", "catch-up floor must precede the membership proposal")
	require.Contains(t, interruptedState, "data_dir=/var/lib/elastickv\n")

	changedDataDirCmd := exec.CommandContext(t.Context(), "bash", "scripts/raft-member-replace.sh", "--execute")
	changedDataDirCmd.Dir = repoRoot
	changedDataDirEnv := append(append([]string{}, commandEnv...), "DATA_DIR=/srv/elastickv")
	changedDataDirCmd.Env = append(os.Environ(), changedDataDirEnv...)
	changedDataDirOutput, changedDataDirErr := changedDataDirCmd.CombinedOutput()
	require.Error(t, changedDataDirErr)
	require.Contains(t, string(changedDataDirOutput), "DATA_DIR changed from /var/lib/elastickv to /srv/elastickv")

	resumeOutput := runReplacementScript(t, repoRoot, commandEnv)
	require.Contains(t, resumeOutput, "target is already a learner; resuming after committed add")
	require.Contains(t, resumeOutput, "replacement completed: target=n2")
	require.Equal(t, "voter\n", mustReadTestFile(t, filepath.Join(stateDir, "member")))
}

func TestRaftMemberReplaceScriptResumesAfterJoinRolloutResponseLoss(t *testing.T) {
	repoRoot, err := os.Getwd()
	require.NoError(t, err)
	stateDir := t.TempDir()
	fakeBin := filepath.Join(stateDir, "bin")
	require.NoError(t, os.Mkdir(fakeBin, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(stateDir, "member"), []byte("voter\n"), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(stateDir, "config-index"), []byte("10\n"), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(stateDir, "fail-join-rollout-once"), nil, 0o600))

	raftadmin := writeExecutableTestFile(t, fakeBin, "raftadmin", fakeRaftadminScript)
	ssh := writeExecutableTestFile(t, fakeBin, "ssh", fakeReplacementSSHScript)
	rolling := writeExecutableTestFile(t, fakeBin, "rolling-update", fakeReplacementRollingScript)
	envFile := filepath.Join(stateDir, "deploy.env")
	require.NoError(t, os.WriteFile(envFile, []byte("NODES=n1=127.0.0.1,n2=127.0.0.2,n3=127.0.0.3\n"), 0o600))
	stateFile := filepath.Join(stateDir, "replace.state")
	commandEnv := []string{
		"TARGET_NODE=n2",
		"REPLACEMENT_CONFIRM=n2",
		"REPLACEMENT_FENCE_MODE=container",
		"REPLACEMENT_FENCE_VERIFY_SECONDS=1",
		"REPLACEMENT_VERIFY_COMMAND=true",
		"REPLACEMENT_STABILITY_SECONDS=1",
		"REPLACEMENT_TIMEOUT_SECONDS=5",
		"REPLACEMENT_POLL_SECONDS=1",
		"REPLACEMENT_STATE_FILE=" + stateFile,
		"ROLLING_UPDATE_ENV_FILE=" + envFile,
		"ROLLING_UPDATE_SCRIPT=" + rolling,
		"RAFTADMIN_BIN=" + raftadmin,
		"SSH_BIN=" + ssh,
		"FAKE_STATE_DIR=" + stateDir,
	}

	cmd := exec.CommandContext(t.Context(), "bash", "scripts/raft-member-replace.sh", "--execute")
	cmd.Dir = repoRoot
	cmd.Env = append(os.Environ(), commandEnv...)
	output, err := cmd.CombinedOutput()
	require.Error(t, err, "%s", output)
	require.Contains(t, mustReadTestFile(t, stateFile), "stage=4\n")
	require.Equal(t, "absent\n", mustReadTestFile(t, filepath.Join(stateDir, "member")))
	require.Equal(t, "n2:n2\n", mustReadTestFile(t, filepath.Join(stateDir, "rollouts")))

	resumeOutput := runReplacementScript(t, repoRoot, commandEnv)
	require.Contains(t, resumeOutput, "replacement completed: target=n2")
	require.Equal(t, "n2:n2\nn2:n2\nnormal:n2\n", mustReadTestFile(t, filepath.Join(stateDir, "rollouts")))
}

func TestRaftMemberReplaceScriptDoesNotRemoveAnotherProcessLock(t *testing.T) {
	repoRoot, err := os.Getwd()
	require.NoError(t, err)
	stateDir := t.TempDir()
	rolling := writeExecutableTestFile(t, stateDir, "rolling-update", "#!/usr/bin/env bash\nexit 0\n")
	stateFile := filepath.Join(stateDir, "replace.state")
	lockDir := stateFile + ".lock"
	require.NoError(t, os.Mkdir(lockDir, 0o700))

	cmd := exec.CommandContext(t.Context(), "bash", "scripts/raft-member-replace.sh", "--execute")
	cmd.Dir = repoRoot
	cmd.Env = append(os.Environ(),
		"TARGET_NODE=n2",
		"NODES=n1=127.0.0.1,n2=127.0.0.2,n3=127.0.0.3",
		"REPLACEMENT_CONFIRM=n2",
		"REPLACEMENT_FENCE_MODE=container",
		"REPLACEMENT_VERIFY_COMMAND=true",
		"REPLACEMENT_STATE_FILE="+stateFile,
		"ROLLING_UPDATE_SCRIPT="+rolling,
	)
	output, err := cmd.CombinedOutput()
	require.Error(t, err)
	require.Contains(t, string(output), "another replacement process holds")
	info, statErr := os.Stat(lockDir)
	require.NoError(t, statErr, "a process that failed to acquire the lock must not remove it")
	require.True(t, info.IsDir())
}

func runReplacementScript(t *testing.T, repoRoot string, env []string) string {
	t.Helper()
	cmd := exec.CommandContext(t.Context(), "bash", "scripts/raft-member-replace.sh", "--execute")
	cmd.Dir = repoRoot
	cmd.Env = append(os.Environ(), env...)
	output, err := cmd.CombinedOutput()
	require.NoError(t, err, "%s", output)
	return string(output)
}

func writeExecutableTestFile(t *testing.T, dir, name, contents string) string {
	t.Helper()
	path := filepath.Join(dir, name)
	require.NoError(t, os.WriteFile(path, []byte(contents), 0o600))
	require.NoError(t, os.Chmod(path, 0o700))
	return path
}

func mustReadTestFile(t *testing.T, path string) string {
	t.Helper()
	contents, err := os.ReadFile(path)
	require.NoError(t, err)
	return string(contents)
}

const fakeRaftadminScript = `#!/usr/bin/env bash
set -euo pipefail
addr="$1"
command="$2"
state_dir="$FAKE_STATE_DIR"

if [[ "$command" == "status" ]]; then
  if [[ "$addr" == "127.0.0.2:50051" && -f "$state_dir/down-n2" ]]; then
    exit 1
  fi
  if [[ "$addr" == "127.0.0.1:50051" ]]; then
    raft_state=LEADER
  elif [[ "$addr" == "127.0.0.3:50051" && -f "$state_dir/candidate-n3" ]]; then
    raft_state=CANDIDATE
  else
    raft_state=FOLLOWER
  fi
  cat <<EOF
state: $raft_state
leader_id: "n1"
leader_address: "127.0.0.1:50051"
term: 5
commit_index: 100
applied_index: 100
last_log_index: 100
last_snapshot_index: 50
fsm_pending: 0
num_peers: 2
last_contact_nanos: 0
configuration_index: $(cat "$state_dir/config-index")
pending_conf_change: false
EOF
  exit 0
fi

print_server() {
  local id="$1" address="$2" suffrage="$3"
  cat <<EOF
servers {
  id: "$id"
  address: "$address"
  suffrage: "$suffrage"
}
EOF
}

if [[ "$command" == "configuration" ]]; then
  print_server n1 127.0.0.1:50051 voter
  member="$(cat "$state_dir/member")"
  if [[ "$member" != "absent" ]]; then
    print_server n2 127.0.0.2:50051 "$member"
  fi
  print_server n3 127.0.0.3:50051 voter
  exit 0
fi

increment_index() {
  local current
  current="$(cat "$state_dir/config-index")"
  printf '%s\n' "$((current + 1))" > "$state_dir/config-index"
}

case "$command" in
  remove_server)
    [[ "$3" == "n2" && "$4" == "$(cat "$state_dir/config-index")" ]]
    printf 'absent\n' > "$state_dir/member"
    increment_index
    ;;
  add_learner)
    [[ "$3" == "n2" && "$4" == "127.0.0.2:50051" && "$5" == "$(cat "$state_dir/config-index")" ]]
    printf 'learner\n' > "$state_dir/member"
    increment_index
    if [[ -f "$state_dir/fail-add-once" ]]; then
      rm -f "$state_dir/fail-add-once"
      exit 73
    fi
    ;;
  promote_learner)
    [[ "$3" == "n2" && "$4" == "$(cat "$state_dir/config-index")" && "$5" == "100" && "$6" == "false" ]]
    printf 'voter\n' > "$state_dir/member"
    increment_index
    ;;
  leadership_transfer_to_server)
    ;;
  *)
    echo "unexpected raftadmin command: $*" >&2
    exit 1
    ;;
esac
printf 'index: %s\n' "$(cat "$state_dir/config-index")"
`

const fakeReplacementSSHScript = `#!/usr/bin/env bash
set -euo pipefail
cat >/dev/null
while [[ "$#" -gt 0 && "$1" != "--" ]]; do shift; done
[[ "$#" -ge 2 ]]
action="$2"
printf '%s\n' "$action" >> "$FAKE_STATE_DIR/remote-actions"
case "$action" in
  stop)
    touch "$FAKE_STATE_DIR/down-n2"
    if [[ "${FAKE_BUMP_CONFIG_ON_FENCE:-false}" == "true" ]]; then
      current="$(cat "$FAKE_STATE_DIR/config-index")"
      printf '%s\n' "$((current + 1))" > "$FAKE_STATE_DIR/config-index"
    fi
    ;;
  reset-data) touch "$FAKE_STATE_DIR/data-reset" ;;
  *) echo "unexpected SSH action: $action" >&2; exit 1 ;;
esac
`

const fakeReplacementRollingScript = `#!/usr/bin/env bash
set -euo pipefail
[[ "${DRY_RUN:-}" == "false" ]]
printf '%s:%s\n' "${RAFT_JOIN_NODE:-normal}" "$ROLLING_ORDER" >> "$FAKE_STATE_DIR/rollouts"
if [[ "${RAFT_JOIN_NODE:-}" == "n2" ]]; then
  rm -f "$FAKE_STATE_DIR/down-n2"
  if [[ -f "$FAKE_STATE_DIR/fail-join-rollout-once" ]]; then
    rm -f "$FAKE_STATE_DIR/fail-join-rollout-once"
    exit 74
  fi
fi
`
