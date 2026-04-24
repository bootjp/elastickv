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
  may additionally require `devices:core` (write); enable that if the
  join step fails with an authorization error. The action's README is
  the definitive source for current scope requirements.
- Tags: `tag:ci-deploy`

Copy the client ID and secret; they go into GitHub in the next step.

## 4. GitHub environment: `production`

Repo → Settings → Environments → New environment: `production`.

### Required reviewers
Configure "Required reviewers" on the environment. Non-dry-run deploys will
pause until one of the reviewers approves. Configure "Deployment protection
rules" to auto-approve if the workflow input `dry_run == true` (optional; cuts
friction for previews).

### Environment secrets

| Name | Value |
|------|-------|
| `TS_OAUTH_CLIENT_ID`        | Tailscale OAuth client ID from step 3 |
| `TS_OAUTH_SECRET`           | Tailscale OAuth secret from step 3 |
| `DEPLOY_SSH_PRIVATE_KEY`    | OpenSSH private key, authorized on every node under the deploy user |
| `DEPLOY_KNOWN_HOSTS`        | `ssh-keyscan kv01.<tailnet>.ts.net kv02.<tailnet>.ts.net …` output (one host per line) |

The SSH key should be ed25519, dedicated to CI (not a reused developer key).
Regenerate on operator rotation.

### Environment variables

| Name | Value | Example |
|------|-------|---------|
| `IMAGE_BASE`      | Container image path (no tag)     | `ghcr.io/bootjp/elastickv` |
| `SSH_USER`        | SSH login on every node           | `bootjp` |
| `NODES_RAFT_MAP`  | Comma-separated `raftId=host` (no port — the script appends `RAFT_PORT`) | `n1=kv01,n2=kv02,n3=kv03,n4=kv04,n5=kv05` |
| `SSH_TARGETS_MAP` | Comma-separated `raftId=ssh-host` | `n1=kv01.<tailnet>.ts.net,n2=kv02.<tailnet>.ts.net,...` |

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
Expected for non-dry-run deploys — a reviewer from the `production` environment
must click Approve. Check the "Required reviewers" list in the environment
settings.

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
`DEPLOY_KNOWN_HOSTS` is stale. Re-run `ssh-keyscan` against every node and
update the secret.
