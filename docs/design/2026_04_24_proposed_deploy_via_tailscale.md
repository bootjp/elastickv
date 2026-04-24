# Deploy via Tailscale + GitHub Actions

**Status:** Proposed
**Author:** bootjp
**Date:** 2026-04-24

---

## 1. Background

Today the rolling-update flow is manual: an operator SSHes to their workstation,
exports the required env vars (`NODES`, `SSH_TARGETS`, image tag, etc.),
invokes `scripts/rolling-update.sh`, and watches it roll the cluster.

Problems:

- **No audit trail.** Who rolled what, when, and from which commit is only
  visible in each operator's local shell history.
- **Manual secret handling.** SSH keys, Tailscale auth, and S3 creds live on
  operator workstations. Joining and leaving the ops rotation requires key
  shuffling.
- **No approval gate.** The production cluster is rolled by whoever types the
  command. A typo can take out the cluster before anyone else sees it.
- **No dry-run.** The script supports neither `--dry-run` nor a preview mode;
  operators who want to verify targeting have to read the script.

The 2026-04-24 incident compounds the risk: the cluster is fragile enough that
a rolling update executed against the wrong `NODES` list could cascade into an
election storm.

## 2. Proposal

Move rolling deploys to a GitHub Actions workflow that joins the Tailnet via
`tailscale/github-action`, SSHes into each node over Tailscale MagicDNS, and
invokes the existing `scripts/rolling-update.sh`. All secrets live in GitHub
environments; every deploy becomes a PR-linked, reviewable event.

**Precondition (operator responsibility):** Tailscale is already installed and
logged in on every node, with SSH access enabled over the tailnet.

### 2.1 Workflow shape

```
name: Rolling update
on: workflow_dispatch:
  inputs:
    ref:           # git sha/tag of the image to deploy
    image_tag:     # defaults to $ref; override only for rollbacks
    nodes:         # subset of raft IDs; empty = full roll
    dry_run:       # bool, default TRUE — renders plan but doesn't roll

jobs:
  deploy:
    environment: production    # requires approval
    concurrency:
      group: rolling-update
      cancel-in-progress: false
    runs-on: ubuntu-latest
    steps:
      - checkout
      - join tailnet (tailscale/github-action, ephemeral)
      - configure SSH (add DEPLOY_SSH_PRIVATE_KEY to agent)
      - render NODES + SSH_TARGETS from repo config
      - if dry_run: print the derived env and exit
      - else: ./scripts/rolling-update.sh
```

### 2.2 Secrets and variables

Stored in a GitHub `production` environment (not repo-wide):

**Secrets:**
- `TS_OAUTH_CLIENT_ID`, `TS_OAUTH_SECRET` — Tailscale OAuth client scoped to
  "devices:write" on a single tag (e.g., `tag:ci-deploy`). Ephemeral nodes;
  cleaned up automatically after the job.
- `DEPLOY_SSH_PRIVATE_KEY` — SSH key authorized on every node. Restricted to
  the `deploy` user (if we split it out) or `bootjp` (initial).
- `DEPLOY_KNOWN_HOSTS` — pre-populated `known_hosts` with the Tailnet MagicDNS
  entries. Prevents the first-connect TOFU prompt.

**Variables (non-secret):**
- `NODES_RAFT_MAP` — `n1=kv01,n2=kv02,...` (advertised hostnames as seen
  from inside the tailnet; the script appends `RAFT_PORT` automatically,
  so do NOT include a port here).
- `SSH_TARGETS_MAP` — `n1=kv01.tailnet.ts.net,...` (MagicDNS).
- `IMAGE_BASE` — `ghcr.io/bootjp/elastickv` (tag is appended from the input).
- `SSH_USER` — e.g., `bootjp`.

### 2.3 Tailscale authentication

Use OAuth ephemeral nodes (not a long-lived auth key):

- Create an OAuth client in Tailscale admin console with scope
  `auth_keys` (write) on tag `tag:ci-deploy`. (`tailscale/github-action`
  uses the OAuth client to mint a short-lived auth key on each run;
  recent action versions may also require `devices:core` — consult the
  action's README for the current scope list.)
- Store client ID + secret in GitHub env secrets.
- `tailscale/github-action@v3` joins the tailnet for the duration of the job
  as an ephemeral tagged node; disconnects automatically on job exit.

ACLs on the Tailnet side should limit `tag:ci-deploy` to SSH (tcp/22) on
`tag:elastickv-node` only. No other ports, no other tags.

### 2.4 SSH

Two options:

- **A. Tailscale SSH.** Lets CI SSH in without managing an SSH keypair: the
  Tailnet ACL is the authorization model. Requires the nodes to have
  `--ssh` flag on `tailscaled` (or `tailscale up --ssh`) and the Tailnet ACL
  to grant `tag:ci-deploy` SSH access to node tag + user. No SSH keys in
  GitHub at all.
- **B. Plain SSH over Tailscale.** CI brings an SSH key; nodes continue to
  use `~/.ssh/authorized_keys`. Tailscale is just the network layer.

**Recommendation for v1: B** (plain SSH). Nodes already have `authorized_keys`
for the current manual flow; nothing to change on the node side. Tailscale
SSH (A) can be a follow-up once the key-rotation story is written up.

### 2.5 Dry-run semantics

With `dry_run: true` (the default):

- Everything up to script invocation runs (checkout, tailnet join, SSH agent
  load, `NODES`/`SSH_TARGETS` render).
- The script is invoked with `--help` + the rendered env is printed as a
  collapsed log group.
- `tailscale ping` is run against each SSH target to confirm reachability.
- The actual `docker stop/rm/run` loop does NOT execute.

This catches the common failure modes (bad secret, bad env mapping, a node
unreachable over the tailnet) before touching any live container.

### 2.6 Production environment approval

Mark the `production` GitHub environment as requiring approval from a list of
reviewers. A non-dry-run deploy will pause until approved; the dry-run run
itself does not need approval (it only needs the tailnet join).

Alternative: require approval unconditionally and treat the dry-run as a
"preview" that an approver must ack. Simpler policy, slightly more friction.

**Recommendation:** approval required for non-dry-run only. Dry-runs are
cheap and useful.

### 2.7 Rollback

Rolling back uses the same workflow with `image_tag: <previous-sha>`. The
script already supports the rollout order env var (`ROLLING_ORDER`) so an
operator can force-roll only the affected nodes.

**Gap:** there is no "stop mid-rollout" control today. If the workflow is
cancelled via GitHub UI during a roll, the in-flight node may be mid-recreate.
`rolling-update.sh` is supposed to be idempotent and crash-safe, but this
should be verified before we call the workflow production-ready.

## 3. Open questions

- **SSH user.** Continue using `bootjp` (personal) or provision a shared
  `deploy` user on each node? v1 sticks with `bootjp` to keep scope tight;
  follow-up can introduce `deploy` with a limited sudo rule for `docker`.
- **Secret scope.** Environment-scoped secrets (as proposed) vs.
  repository-scoped. Environment-scoped wins on blast radius but requires
  the GitHub environment to be pre-created. Assume pre-created.
- **Image availability check.** Should the workflow verify the image tag
  exists on ghcr.io before starting the roll? Cheap to add (`docker manifest
  inspect` in a pre-step) and prevents a half-rolled cluster when the tag is
  typo'd.
- **Jepsen gating.** The existing `jepsen-test.yml` workflow exists.
  Option: require a green Jepsen run on `ref` within the last N hours before
  allowing deploy. Skipped for v1; worth revisiting before rolling this out
  to high-traffic periods.

## 4. Out of scope for v1

- Automatic deploys on merge to main (needs more test coverage before we'd
  trust it).
- Blue-green or canary strategies (we don't have the traffic-routing layer
  for it).
- Metrics-based rollback trigger (watch p99, auto-revert if it jumps).
- Tailscale SSH (option A above).
- A shared `deploy` user with restricted sudo.

## 5. Implementation plan

1. Write `.github/workflows/rolling-update.yml` implementing §2.1.
2. Document the secrets/variables setup in
   `docs/operations/deploy_runbook.md` (new).
3. Run once with `dry_run: true` on a feature branch to validate secrets
   wiring without touching prod.
4. Run once with `dry_run: false` targeting a single node (via the `nodes`
   input) to prove the happy path.
5. Cut over: archive the operator-local rolling flow, document the new one
   as the canonical path.
