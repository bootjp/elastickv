# Deploy via Tailscale + GitHub Actions — Runbook

Companion doc to `docs/design/2026_04_24_proposed_deploy_via_tailscale.md`. This
runbook is for operators: what to configure on GitHub and Tailscale so the
`rolling-update` workflow can execute a production deploy.

## 1. Precondition: Tailscale on every node

Each cluster node must have `tailscale` installed, logged into the tailnet, and
tagged so the CI runner's ACL can reach it.

```
# on each kv0X node
sudo tailscale up \
  --ssh=false \
  --advertise-tags=tag:elastickv-node \
  --accept-routes=false
```

`--ssh=false` disables Tailscale SSH, so the node's regular system
sshd must be running and authorised to accept connections on the
tailnet interface. The workflow uses plain SSH over the tailnet
(Tailscale is only the network layer); if you rely on Tailscale SSH
for operator access elsewhere, drop this flag but keep in mind the
workflow still connects to the system sshd.

Verify the node is reachable by MagicDNS from another tailnet peer:

```
tailscale status | grep kv0X
ping kv0X.<tailnet>.ts.net
```

## 2. Tailscale ACL

In the Tailscale admin console, add the deploy rule to the tailnet ACL:

```jsonc
"tagOwners": {
  "tag:ci-deploy":      ["autogroup:admin"],
  "tag:elastickv-node": ["autogroup:admin"],
},
"acls": [
  {
    "action": "accept",
    "src":    ["tag:ci-deploy"],
    "dst":    ["tag:elastickv-node:22"],
  },
],
```

`tag:ci-deploy` must NOT have access to any other port on the tailnet. The
deploy workflow only needs SSH.

## 3. Tailscale OAuth client

Admin console → Settings → OAuth clients → New client:

- Description: `elastickv GitHub Actions deploy`
- Scopes: `auth_keys` (write). Recent `tailscale/github-action` versions
  may additionally require `devices:write` (to register and clean up
  the ephemeral node); enable that if the join step fails with an
  authorization error. The action's README is the definitive source
  for current scope requirements. `devices:core` is NOT a valid
  Tailscale OAuth scope — earlier drafts of this runbook named it and
  would have produced an auth failure.
- Tags: `tag:ci-deploy`

Copy the client ID and secret; they go into GitHub in the next step.

## 4. GitHub environment: `production`

Repo → Settings → Environments → New environment: `production`.

### Required reviewers
Configure "Required reviewers" on the environment. **Every run that targets
this environment pauses for approval** — including dry-runs, because
GitHub's native environment-protection rules cannot be made conditional on
workflow inputs. Three ways to handle the dry-run-approval friction:

1. **Accept the prompt for dry-runs too.** A dry-run requires one approver
   click before it proceeds; still cheap and keeps the policy simple.
2. **Add a second environment `production-dry-run` without required
   reviewers** and change the workflow to pick the environment via
   `environment: ${{ inputs.dry_run && 'production-dry-run' || 'production' }}`.
   Cleanest but doubles the secrets/vars you must keep in sync.
3. **Install a deployment-protection-rule GitHub App** (custom or
   marketplace) that approves runs whose inputs show `dry_run == true`.
   Most flexible; most setup.

v1 ships with approach 1 (single environment, prompt on every run).
Approach 2 is the recommended upgrade once the friction becomes annoying.

### Environment secrets

| Name | Value |
|------|-------|
| `TS_OAUTH_CLIENT_ID`        | Tailscale OAuth client ID from step 3 |
| `TS_OAUTH_SECRET`           | Tailscale OAuth secret from step 3 |
| `DEPLOY_SSH_PRIVATE_KEY`    | OpenSSH private key, authorized on every node under the deploy user |
| `DEPLOY_KNOWN_HOSTS`        | `ssh-keyscan -H kv01.<tailnet>.ts.net kv02.<tailnet>.ts.net …` output. Use `-H` to hash hostnames so the secret's contents don't leak the tailnet topology if the runner environment is compromised. |

The SSH key should be ed25519, dedicated to CI (not a reused developer key).
Regenerate on operator rotation.

### Environment variables

| Name | Value | Example |
|------|-------|---------|
| `IMAGE_BASE`      | Container image path (no tag)     | `ghcr.io/bootjp/elastickv` |
| `SSH_USER`        | SSH login on every node           | `bootjp` |
| `NODES_RAFT_MAP`  | Comma-separated `raftId=host` (no port — the script appends `RAFT_PORT`). Use full MagicDNS FQDNs so every node can resolve the advertised address regardless of local DNS search domains. The workflow renders this into the script's `NODES` env var. | `n1=kv01.<tailnet>.ts.net,n2=kv02.<tailnet>.ts.net,n3=kv03.<tailnet>.ts.net,n4=kv04.<tailnet>.ts.net,n5=kv05.<tailnet>.ts.net` |
| `SSH_TARGETS_MAP` | Comma-separated `raftId=ssh-host`. The workflow renders this into the script's `SSH_TARGETS` env var. Usually identical to `NODES_RAFT_MAP` unless SSH access uses a different hostname. | `n1=kv01.<tailnet>.ts.net,n2=kv02.<tailnet>.ts.net,...` |

**Why two names?** The workflow uses `NODES_RAFT_MAP` / `SSH_TARGETS_MAP`
in the `production` environment to keep the GitHub-side names
distinct from the script-side env var names it hands to
`rolling-update.sh`. If you run the script by hand from a workstation
you must export `NODES` and `SSH_TARGETS` directly — the workflow-side
names are only understood by the workflow's render step.

## 5. Running a deploy

Actions tab → "Rolling update" → Run workflow.

Inputs:

- `ref` — the git tag or sha to deploy (also used as the container image tag)
- `image_tag` — override only for rollbacks (e.g., deploy tag `v1.2.3` of a
  commit that was also `v1.2.3`)
- `nodes` — subset of raft IDs, e.g., `n1,n2`. Empty rolls all nodes.
- `dry_run` — default `true`. Renders the plan and checks reachability without
  touching containers.

Recommended first-run sequence:

1. `dry_run: true`, `nodes: n1`, `ref: <target>` — confirms tailnet join,
   SSH config, image availability, target mapping. No production impact.
2. `dry_run: false`, `nodes: n1` — roll a single node, verify the cluster
   stays healthy and the image is correct.
3. `dry_run: false`, `nodes:` (empty) — full roll.

## 6. Rollback

Re-run the workflow with `image_tag` set to the previous-known-good sha. The
`nodes` input can target specific nodes if only some carry the bad image.

### If a running workflow is cancelled mid-rollout

GitHub cancelling the job between node steps is the one operational
hazard that needs manual cleanup.

1. **Look at the last log line from the `Roll cluster` step.** The
   script emits `==> [<raft-id>@<host>] start` at the beginning of
   each per-node recreate (see `scripts/rolling-update.sh:398`).
   Whichever `<raft-id>` appears in the last such line is the one in
   flight when the cancel signal landed.
2. **SSH into that node** over Tailscale and run `docker ps`. If the
   container is absent or `Exited`, finish the recreate by hand. The
   `docker run` invocation itself is redirected to `/dev/null` by the
   script, so the workflow log does NOT contain the full argv. To
   reconstruct it, read the `Roll cluster` step's rendered
   environment — the workflow exports `IMAGE`, `DATA_DIR`,
   `RAFT_PORT`, `REDIS_PORT`, `S3_PORT`, `ENABLE_S3`, `NODES`,
   `SSH_TARGETS`, and the merged `EXTRA_ENV` before invoking the
   script. Anything not explicitly set (e.g., `RAFT_PORT` in a
   minimally-overridden deploy) falls back to the script's default
   (`RAFT_PORT=50051`, `REDIS_PORT=6379`, `S3_PORT=9000`,
   `ENABLE_S3=true`). GOMEMLIMIT / CONTAINER_MEMORY_LIMIT (PR #617)
   are propagated via `EXTRA_ENV` once that PR lands. Together the
   rendered env + the node's `deploy.env` is enough to reconstruct
   the same `docker run` you would see if you re-ran with the same
   inputs.
3. **Confirm the new leader via `raftadmin` or metrics** before re-running
   the workflow with `nodes:` scoped to the remaining untouched IDs. Do
   NOT re-run the full rollout if the partial one is still in flight —
   it will stop the same node you are trying to recover.
4. **File a ticket** with the log excerpt so we can eventually teach the
   workflow to set a start-marker on each node and fast-skip completed
   nodes on re-run.

The script is idempotent. `scripts/rolling-update.sh:794-798` skips a
node when its running image id equals the target image and its gRPC
endpoint is healthy — an already-rolled node is a no-op, not a
redundant stop/recreate. Re-running the workflow with the same
`ref` after confirming the interrupted node is healthy is therefore
safe: nodes that already match the target image are passed over,
and only the still-stale one gets recreated.

## 7. What the workflow does NOT do (yet)

- **No post-deploy health verification beyond tailnet reachability.** The
  script itself blocks on `raftadmin` leadership transfer and health-gate
  timeouts, but the workflow does not independently probe Prometheus or
  Redis after the roll. Add this when we have a canonical post-deploy
  assertion suite.
- **No auto-rollback on failure.** If the script exits non-zero mid-roll,
  the cluster is left in whatever state the script reached. The operator
  must inspect and either re-roll or roll back manually.
- **No Jepsen gate.** The deploy does not require a green Jepsen run on
  `ref` before proceeding.
- **No image-signature check.** `cosign verify` on the image is a follow-up.

## 8. Troubleshooting

### Job pauses indefinitely at "Waiting for approval"
Expected for **every** run in v1 — `.github/workflows/rolling-update.yml`
sets `environment: production` unconditionally, so both dry-run and
non-dry-run executions pause for approval. A reviewer from the
`production` environment must click Approve. Check the "Required
reviewers" list in the environment settings. See §4 "GitHub
environment" for the dry-run-approval alternatives (approach 2: add a
second `production-dry-run` environment without required reviewers)
if the friction becomes intolerable.

### `tailscale ping` fails for a node
The node may not be running `tailscaled`, not tagged `tag:elastickv-node`, or
the tailnet ACL may have drifted. `tailscale status` on the node should show
the tag; the admin console should show the IP in the `tag:elastickv-node`
group.

### `image ... not found on ghcr.io`
The verification step hit the ghcr manifest API and got a 404. Either the
image tag was not pushed (check the `Docker Image CI` workflow for `ref`) or
the tag is a moving tag (`latest`) that the verification step can't
distinguish from stale. Specify an immutable tag.

### SSH `Host key verification failed`
`DEPLOY_KNOWN_HOSTS` is stale. Re-run `ssh-keyscan -H` against every node and
update the secret.
