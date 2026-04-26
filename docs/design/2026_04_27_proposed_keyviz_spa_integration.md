---
status: proposed
phase: 2-B
parent_design: docs/admin_ui_key_visualizer_design.md
date: 2026-04-27
---

# KeyViz SPA Integration (Phase 2-B)

## 1. Background

Phase 2 of the Key Visualizer design (`docs/admin_ui_key_visualizer_design.md`)
landed the **server side** end-to-end:

- `keyviz.MemSampler` with COW route table, ring-buffer history, and
  bytes counters (PR #639).
- `ShardedCoordinator` write- and read-path observation (PR #645,
  PR #661).
- `adapter.AdminServer.GetKeyVizMatrix` gRPC RPC (PR #646).
- `internal/admin` HTTP handler at `/admin/api/v1/keyviz/matrix`
  (PR #660 + PR #672 follow-up).
- `main.go` end-to-end wiring (PR #647 / PR #651).

The remaining piece is the **frontend** — the admin SPA at `web/admin/`
already serves Overview / DynamoDB / SQS / S3, but has no KeyViz page.
This doc proposes Phase 2-B: integrate the heatmap into the existing
SPA rather than building a separate dashboard.

## 2. Why integrate, not build separately

The original §3 of the parent design left open the question of where
the SPA lives. Inventory of what already exists:

- `web/admin/` is a Vite + React 18 + TypeScript + Tailwind SPA, built
  into `internal/admin/dist` and embedded via `embed.go`.
- It is served from the same Go process as the API (`internal/admin`),
  on the same admin listener, so there is **no second origin**.
- Auth is HttpOnly `admin_session` cookie + double-submit `admin_csrf`
  cookie, applied uniformly by `apiFetch` in `src/api/client.ts`.
- The KeyViz HTTP handler is already mounted on the same `apiBase`
  (`/admin/api/v1`) and the same authn / CSRF middleware stack.
- Layout + nav (`src/components/Layout.tsx`) is already a list-driven
  pattern — adding a tab is one entry.

Building a second SPA would duplicate:

- the Vite / Tailwind / ESLint / tsconfig toolchain,
- auth, session, CSRF, and 401 redirect logic (`auth.tsx` + `useApi.ts`),
- the embed pipeline (`internal/admin/embed.go` + `dist` glob),
- the cookie origin (a separate origin would force CORS or a reverse
  proxy hack just to read `admin_session`).

Net cost of integration is **three new files plus three line edits**.
Net cost of a parallel SPA is on the order of weeks of toolchain and
auth re-plumbing, with no upside the user would observe.

**Decision: integrate into `web/admin/`.**

## 3. Surface area

### 3.1 New page

`web/admin/src/pages/KeyViz.tsx` mounted at route `/keyviz`. The page
contains:

- A header with the series picker (`writes` / `reads` / `write_bytes` /
  `read_bytes`), a row-budget input (default 1024, capped server-side),
  a refresh button, and a small "auto-refresh: off / 5 s / 30 s" toggle.
- The heatmap canvas itself: `<canvas>` rendered from the `Values[][]`
  matrix the API returns. Rows on the Y axis are routes (one per
  `KeyVizRow`), columns on the X axis are time bins from
  `ColumnUnixMs`. Cell colour intensity is normalised against the
  per-matrix max so a quiet column does not look identical to a hot
  one.
- A row-detail flyout: clicking a row reveals `bucket_id`, `start`,
  `end`, `aggregate`, `route_count`, and (when present) `route_ids`
  with a `route_ids_truncated` indicator.

The page is read-only and does not need the `full` role; both
`read_only` and `full` sessions can view it.

### 3.2 API client

Three additions to `web/admin/src/api/client.ts`:

```ts
export type KeyVizSeries = "reads" | "writes" | "read_bytes" | "write_bytes";

export interface KeyVizRow {
  bucket_id: string;
  start: string;            // base64 from Go []byte
  end: string;
  aggregate: boolean;
  route_ids?: number[];
  route_ids_truncated?: boolean;
  route_count: number;
  values: number[];
}

export interface KeyVizMatrix {
  column_unix_ms: number[];
  rows: KeyVizRow[];
  series: KeyVizSeries;
  generated_at: string;
}

export interface KeyVizParams {
  series?: KeyVizSeries;
  from_unix_ms?: number;
  to_unix_ms?: number;
  rows?: number;
}

api.keyVizMatrix = (params, signal) =>
  apiFetch<KeyVizMatrix>("/keyviz/matrix", { query: params, signal });
```

The query passes through `apiFetch`'s existing CSRF-free GET path; no
mutation route is needed for Phase 2-B.

### 3.3 Routing and navigation

- `web/admin/src/App.tsx`: add `<Route path="keyviz" element={<KeyVizPage />} />`
  alongside the existing dynamo / sqs / s3 routes.
- `web/admin/src/components/Layout.tsx`: add `{ to: "/keyviz", label: "Key Visualizer" }`
  to `navItems`.

### 3.4 What this proposal does NOT do

- **No charting library.** Pure `<canvas>` + a fixed colour ramp. The
  full matrix is at most 1024 rows × a few hundred columns; that fits
  trivially on a single canvas without virtualisation. If we later
  want zoom/pan, we'll revisit the dependency cost in a follow-up.
- **No auto-correlation with Routes / Raft Groups pages.** Those
  pages are not yet built; correlation is a Phase 1 task and will be
  added when those pages land.
- **No drill-down view.** Phase 3 territory (per-route sparkline +
  hot-key preview labels). Out of scope.
- **No multi-node fan-out.** The handler is currently node-local (it
  only sees the local sampler). A separate Phase 2-A item will add a
  fan-out admin RPC; this proposal renders whatever the handler
  returns, and will pick up fan-out for free once that ships.

## 4. Heatmap rendering specifics

### 4.1 Colour mapping

Per design §4.1, the default series is `writes`. Cell value `v` is
normalised against the per-matrix max `M` (`v / M`, clamped to `[0,1]`)
and mapped through a perceptually-monotonic ramp. We will use a
hand-rolled 5-stop ramp (transparent → blue → green → yellow → red)
to avoid pulling in `d3-interpolate`. The ramp is in `lib/colorRamp.ts`
so a future swap is one file.

Empty cells (`v === 0`) render as the page background, not a faint blue
— this is critical for spotting actually-cold routes.

### 4.2 Layout

Cell width: `min(8 px, container_width / column_count)`. Cell height:
`min(4 px, container_height / row_count)`. Cap row count at 1024 so
the canvas height stays under ~4096 px even at the maximum budget.

Time axis labels: every Nth column where `N = ceil(column_count / 10)`,
formatted as `HH:mm:ss` from `column_unix_ms[i]`.

Route axis labels: `bucket_id` truncated to 12 chars with a tooltip
on hover. The full row data is available in the row-detail flyout.

### 4.3 Performance budget

Phase 2 §10 sets ≤120 ms render budget for a 1024×500 matrix. Our
single-pass `ImageData.data` write fits under that on every browser
we've tested with similar sizes. We do **not** use SVG (one element
per cell would be 500k DOM nodes at the max).

### 4.4 Refresh

Auto-refresh polls `api.keyVizMatrix({ series, rows })` and re-renders.
The poll uses the same `useApiQuery` reload mechanism the other pages
use, so 401 → forced logout falls out for free.

5-second cadence is the lower bound; the sampler's flush is 1 s, so
polling faster would mostly redraw the same matrix. 30-second cadence
is for users leaving the tab open.

## 5. Testing

Phase 2-B is a pure-frontend change. The Go test suite is unchanged.

- **Manual verification** (recorded in the PR description):
  1. `cd web/admin && npm install && npm run build` produces
     `internal/admin/dist/index.html` containing the new bundle.
  2. `make run` starts the demo cluster; opening
     `http://127.0.0.1:8080/admin/` and navigating to **Key Visualizer**
     renders the heatmap.
  3. With no traffic, the heatmap shows the route grid in the
     background colour (no false-colour blue).
  4. With `make client` driving writes, hot routes light up red within
     ~5 s.
  5. The series picker switches the displayed counter; row-budget
     input clamps server-side at 1024.

- **Type check**: `npm run lint` (which is `tsc -b --noEmit`) is the
  CI gate for the SPA.

- **Lint and unit tests for backend**: unchanged from existing CI
  (`make lint`, `go test ./...`). No backend code changes in this
  proposal.

## 6. Five-lens review checklist

Per `CLAUDE.md`, recorded for completeness even on a frontend change:

1. **Data loss** — n/a; SPA is read-only against an existing handler.
2. **Concurrency / distributed failures** — n/a; a single browser tab
   polls a single handler instance. The handler itself is already
   tested for concurrent observers.
3. **Performance** — Phase 2 §10 budget honoured by canvas + single
   `putImageData`. No new dependency. Polling defaults to off.
4. **Data consistency** — The SPA renders whatever the handler
   returns; consistency guarantees come from the existing sampler
   (in-memory, leader-issued counters per Phase 2 design §5.1).
5. **Test coverage** — Type-check via `tsc -b --noEmit`. Manual
   verification steps documented in §5; KeyViz is the kind of feature
   where a screenshot or video in the PR description is more useful
   than a unit test.

## 7. Lifecycle

- Land this doc and the implementation in the same PR (doc commit
  first, then implementation).
- On merge: rename `docs/admin_ui_key_visualizer_design.md`'s phase
  table from "Phase 2 KeyViz MVP" to mark 2-B (SPA) as shipped, and
  rename this doc from `*_proposed_*` to `*_implemented_*` once the
  parent design's Phase 2 fan-out item also ships.

## 8. Open questions

1. Should the row-budget input be free-form (any integer ≤ 1024) or
   stepped (256 / 512 / 1024)? Proposing free-form for ergonomics; the
   server clamps anyway.
2. Should the page remember series + rows + auto-refresh in
   `localStorage`? Probably yes, but punt to a follow-up — the URL
   query can carry the same state for now if needed.
3. Should we colour-blind-safe the ramp by default (e.g., viridis)?
   Worth doing eventually; for Phase 2-B the operator audience is
   small enough that a follow-up swap is acceptable.
