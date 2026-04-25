# Design Documents

Forward-looking design proposals and implementation plans for elastickv live in this directory. Operational runbooks, reference material, and running TODO lists stay under `docs/` itself.

## Filename convention

```text
YYYY_MM_DD_<status>_<name>.md
```

- `YYYY_MM_DD` — **propose date**: the date the proposal was first written. Use the author date of the first commit that added the file:

  ```sh
  git log --follow --diff-filter=A --format='%aI' -- docs/design/<file> | tail -1
  ```

  Do not change this date when the doc is later revised or its status is promoted. It is a historical anchor, not a "last updated" marker.

- `<status>` — current **implementation state**, assigned conservatively from code evidence on `main`:

  | Value | Meaning |
  |---|---|
  | `proposed` | No matching code on `main`, or the doc itself declares itself a proposal. |
  | `partial` | Some components exist but the full scope described in the doc is not yet merged. |
  | `implemented` | Concrete Go code exists on `main` that matches the design's central subsystem (the functions, types, or files named in the doc). |

  Promote the status by renaming the file in a dedicated commit (e.g. `docs: promote <name> from proposed to implemented`). Do not introduce new statuses without updating this README.

- `<name>` — short slug, lowercase with underscores, **without** the `_design` suffix (`s3_compatible_adapter`, not `s3_compatible_adapter_design`).

### Examples

```text
2026_04_20_implemented_lease_read.md
2026_04_18_proposed_raft_grpc_streaming_transport.md
2026_02_18_partial_hotspot_shard_split.md
2026_04_24_proposed_sqs_compatible_adapter.md
```

## Document header

New design docs should open with the following metadata block under the title:

```markdown
# <Title>

Status: <Proposed | Partial | Implemented>
Author: <github handle>
Date: YYYY-MM-DD
```

The header `Status` mirrors the filename `<status>` token. The header `Date` matches the filename propose date.

## What belongs here vs. `docs/`

Kept under `docs/design/`:

- Proposals for new subsystems, adapters, or protocol surfaces.
- Concrete implementation plans (milestone PR plans, migration plans).
- Implemented proposals preserved for historical reference.

Kept under `docs/` (not design proposals):

- `architecture_overview.md` — reference.
- `docker_multinode_manual_run.md`, `etcd_raft_migration_operations.md`, `redis-proxy-deployment.md` — operations.
- `redis_hotpath_dashboard.md` — observability runbook.
- `review_todo.md` — running TODO list.

## Moving or renaming

Always use `git mv` so rename history and `git log --follow` continue to trace prior revisions. When promoting status, keep the propose date and the slug untouched — only the status token changes.
