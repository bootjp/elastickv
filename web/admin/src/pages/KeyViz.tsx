import { useEffect, useMemo, useRef, useState } from "react";
import type {
  KeyVizFanoutResult,
  KeyVizHotKeysResponse,
  KeyVizMatrix,
  KeyVizRow,
  KeyVizSeries,
} from "../api/client";
import { ApiError, api } from "../api/client";
import { ramp } from "../lib/colorRamp";
import { formatApiError, useApiQuery } from "../lib/useApi";

type RefreshMode = "off" | "5s" | "30s";

const seriesOptions: ReadonlyArray<{ value: KeyVizSeries; label: string }> = [
  { value: "writes", label: "Writes" },
  { value: "reads", label: "Reads" },
  { value: "write_bytes", label: "Write bytes" },
  { value: "read_bytes", label: "Read bytes" },
];

const refreshOptions: ReadonlyArray<{ value: RefreshMode; label: string; ms: number }> = [
  { value: "off", label: "Manual", ms: 0 },
  { value: "5s", label: "5 s", ms: 5_000 },
  { value: "30s", label: "30 s", ms: 30_000 },
];

const rowsCap = 1024;

export function KeyVizPage() {
  const [series, setSeries] = useState<KeyVizSeries>("writes");
  const [rows, setRows] = useState<number>(rowsCap);
  const [refreshMode, setRefreshMode] = useState<RefreshMode>("off");

  // useApiQuery refetches whenever any dep changes, so series + rows
  // are tracked here. Refresh-mode polls re-bump `tick` to force a
  // refetch without changing the visible parameters.
  const [tick, setTick] = useState(0);
  const matrix = useApiQuery(
    (signal) => api.keyVizMatrix({ series, rows }, signal),
    [series, rows, tick],
  );

  useEffect(() => {
    const opt = refreshOptions.find((o) => o.value === refreshMode);
    if (!opt || opt.ms === 0) return undefined;
    const id = window.setInterval(() => setTick((t) => t + 1), opt.ms);
    return () => window.clearInterval(id);
  }, [refreshMode]);

  return (
    <div className="space-y-6">
      <header className="flex items-center justify-between gap-4 flex-wrap">
        <h1 className="text-xl font-semibold">Key Visualizer</h1>
        <div className="flex items-center gap-2 text-sm">
          <SeriesPicker value={series} onChange={setSeries} />
          <RowsInput value={rows} onChange={setRows} />
          <RefreshPicker value={refreshMode} onChange={setRefreshMode} />
          <button type="button" className="btn-secondary" onClick={matrix.reload}>
            Refresh
          </button>
        </div>
      </header>

      {matrix.data?.fanout && <FanoutBanner fanout={matrix.data.fanout} />}

      <section className="card">
        {matrix.loading && !matrix.data && (
          <div className="text-sm text-muted">Loading…</div>
        )}
        {matrix.error && matrix.error.status === 404 && (
          <div className="text-sm text-muted">
            Endpoint pending — KeyViz handler not mounted on this node.
          </div>
        )}
        {matrix.error && matrix.error.status === 503 && (
          <div className="text-sm text-muted">
            KeyViz sampler is disabled on this node. Start the server with
            <code className="font-mono"> --keyvizEnabled</code> to enable.
          </div>
        )}
        {matrix.error &&
          matrix.error.status !== 404 &&
          matrix.error.status !== 503 && (
            <div className="text-sm text-danger">{formatApiError(matrix.error)}</div>
          )}
        {matrix.data && <Heatmap matrix={matrix.data} />}
      </section>
    </div>
  );
}

interface HeatmapProps {
  matrix: KeyVizMatrix;
}

function Heatmap({ matrix }: HeatmapProps) {
  const canvasRef = useRef<HTMLCanvasElement | null>(null);
  const [hoverRow, setHoverRow] = useState<number | null>(null);
  // selectedCell drives the hot-key drill-down panel. Null = no
  // selection. Clicking the same cell twice clears it (toggle) so the
  // user can dismiss the panel without scrolling for a close button.
  const [selectedCell, setSelectedCell] = useState<{ row: number; col: number } | null>(null);
  // Clear any stale selection when the matrix identity changes (new
  // refresh, series switch, rows resize): the selected row index may
  // now point to a completely different bucket, so dropping the
  // selection is safer than silently re-pointing the drill-down.
  useEffect(() => {
    setSelectedCell(null);
  }, [matrix]);
  // dprTick re-runs the canvas effect when the user drags the window
  // between displays of different pixel densities or changes the
  // browser zoom; window.devicePixelRatio is not reactive on its own,
  // so we listen via matchMedia and bump a tick. The tick is in the
  // canvas effect's dep list further down.
  const [dprTick, setDprTick] = useState(0);
  useEffect(() => {
    if (typeof window === "undefined" || !window.matchMedia) return undefined;
    // The matched DPR changes every time the browser hops between
    // densities; a single MQ fires on each crossing of the resolution
    // floor we list. Use the current dpr as the floor so the listener
    // fires reliably on the *next* change in either direction.
    const mq = window.matchMedia(`(resolution: ${window.devicePixelRatio}dppx)`);
    const onChange = () => setDprTick((t) => t + 1);
    mq.addEventListener("change", onChange);
    return () => mq.removeEventListener("change", onChange);
  }, [dprTick]);

  // maxValue is computed once per matrix and used to normalise every
  // cell. A zero max means no traffic at all → render the canvas as
  // transparent (the page background reads as "cold everywhere").
  const maxValue = useMemo(() => {
    let m = 0;
    for (const r of matrix.rows) {
      for (const v of r.values) {
        if (v > m) m = v;
      }
    }
    return m;
  }, [matrix]);

  const cellW = matrix.column_unix_ms.length > 0 ? Math.max(2, Math.min(8, Math.floor(960 / matrix.column_unix_ms.length))) : 8;
  const cellH = matrix.rows.length > 0 ? Math.max(2, Math.min(4, Math.floor(4096 / Math.max(1, matrix.rows.length)))) : 4;
  const width = matrix.column_unix_ms.length * cellW;
  const height = matrix.rows.length * cellH;

  useEffect(() => {
    const canvas = canvasRef.current;
    if (!canvas) return;
    // Scale the backing buffer to physical pixels and keep CSS at
    // logical pixels: on a 2x display every cell edge is otherwise
    // rendered against a half-resolution buffer and reads as blurry.
    // We clamp the ratio so a future browser quirk reporting an
    // absurd value (e.g. Firefox's experimental zoom-aware DPR > 8)
    // does not balloon canvas memory beyond reason; at the maximum
    // matrix size 4 x dpr is already 16384 x 16384 px of buffer.
    const dpr = Math.min(window.devicePixelRatio || 1, 4);
    canvas.width = Math.max(1, Math.floor(width * dpr));
    canvas.height = Math.max(1, Math.floor(height * dpr));
    const ctx = canvas.getContext("2d");
    if (!ctx) return;
    if (matrix.rows.length === 0 || matrix.column_unix_ms.length === 0) {
      // Nothing to draw — reset the transform on the off-chance the
      // canvas was reused between renders, then clear and bail.
      ctx.setTransform(1, 0, 0, 1, 0, 0);
      ctx.clearRect(0, 0, canvas.width, canvas.height);
      return;
    }
    // Use the buffer/logical ratio as the transform so a fractional
    // DPR (1.25x, 1.5x) maps logical coordinates exactly onto the
    // floored buffer. Setting the transform from the raw `dpr`
    // could draw at a fractional pixel that the buffer cannot
    // represent, leaving sub-pixel blur at the edges. setTransform
    // also resets the matrix so repeated runs do not stack scales.
    ctx.setTransform(canvas.width / width, 0, 0, canvas.height / height, 0, 0);
    ctx.clearRect(0, 0, width, height);

    // One fillRect per cell keeps render under the §10 budget at
    // 1024 x 500: the colour ramp runs once per cell rather than per
    // pixel, and zero-value cells are skipped so the only work on a
    // quiet matrix is the initial clearRect.
    // The `v === 0` short-circuit guarantees `maxValue > 0` by the
    // time we reach the divide, so an explicit zero-divide guard is
    // unreachable: every row that would trip it has already continued.
    for (let i = 0; i < matrix.rows.length; i++) {
      const row = matrix.rows[i];
      for (let j = 0; j < row.values.length; j++) {
        const v = row.values[j];
        if (v === 0) continue;
        const t = v / maxValue;
        const [r, g, b, a] = ramp(t);
        ctx.fillStyle = `rgba(${r}, ${g}, ${b}, ${a / 255})`;
        ctx.fillRect(j * cellW, i * cellH, cellW, cellH);
      }
    }
  }, [matrix, maxValue, width, height, cellW, cellH, dprTick]);

  const onMove = (e: React.MouseEvent<HTMLCanvasElement>) => {
    const rect = e.currentTarget.getBoundingClientRect();
    const y = e.clientY - rect.top;
    const idx = Math.floor(y / cellH);
    if (idx < 0 || idx >= matrix.rows.length) return;
    // mousemove fires per-pixel; the functional update form lets React
    // bail out cheaply on intra-row movement so we only schedule a
    // re-render when the cursor actually crosses into a new row.
    setHoverRow((prev) => (prev === idx ? prev : idx));
  };

  const onLeave = () => setHoverRow(null);

  const onClick = (e: React.MouseEvent<HTMLCanvasElement>) => {
    const rect = e.currentTarget.getBoundingClientRect();
    const x = e.clientX - rect.left;
    const y = e.clientY - rect.top;
    const row = Math.floor(y / cellH);
    const col = Math.floor(x / cellW);
    if (row < 0 || row >= matrix.rows.length) return;
    if (col < 0 || col >= matrix.column_unix_ms.length) return;
    setSelectedCell((prev) =>
      prev && prev.row === row && prev.col === col ? null : { row, col },
    );
  };

  return (
    <div className="space-y-3">
      <div className="flex items-center justify-between text-xs text-muted">
        <span>
          {matrix.rows.length} rows × {matrix.column_unix_ms.length} columns ·
          series <code className="font-mono">{matrix.series}</code> · max ={" "}
          {maxValue.toLocaleString()}
          {matrix.fanout && (
            <>
              {" · "}
              <span>
                cluster view ({matrix.fanout.responded} of {matrix.fanout.expected} nodes)
              </span>
            </>
          )}
        </span>
        <span>{new Date(matrix.generated_at).toLocaleString()}</span>
      </div>
      {matrix.rows.length === 0 ? (
        <div className="text-sm text-muted">
          No tracked routes — drive some traffic and refresh.
        </div>
      ) : (
        // TimeAxis lives inside the scroll container so its labels —
        // which are absolutely positioned at `idx * cellW` — track the
        // canvas as the user scrolls horizontally. Putting it outside
        // would freeze the labels under the left edge whenever the
        // canvas overflows.
        <div className="overflow-auto border border-border rounded relative">
          <canvas
            ref={canvasRef}
            onMouseMove={onMove}
            onMouseLeave={onLeave}
            onClick={onClick}
            style={{ display: "block", width, height, cursor: "crosshair" }}
          />
          <ConflictOverlay rows={matrix.rows} cellH={cellH} cellW={cellW} width={width} />
          <SelectionOverlay selected={selectedCell} cellH={cellH} cellW={cellW} />
          <TimeAxis columnUnixMs={matrix.column_unix_ms} cellW={cellW} />
        </div>
      )}
      {hoverRow !== null && matrix.rows[hoverRow] && (
        <RowDetail row={matrix.rows[hoverRow]} index={hoverRow} />
      )}
      {selectedCell && matrix.rows[selectedCell.row] && (
        <HotKeysPanel
          row={matrix.rows[selectedCell.row]}
          column={selectedCell.col}
          columnUnixMs={matrix.column_unix_ms[selectedCell.col]}
          series={matrix.series}
          onClose={() => setSelectedCell(null)}
        />
      )}
    </div>
  );
}

// SelectionOverlay outlines the clicked cell so the user can correlate
// the heatmap position with the drill-down panel below. SVG sits in
// the same scroll container as ConflictOverlay so it tracks horizontal
// scrolling without manual offset math.
interface SelectionOverlayProps {
  selected: { row: number; col: number } | null;
  cellH: number;
  cellW: number;
}

function SelectionOverlay({ selected, cellH, cellW }: SelectionOverlayProps) {
  if (!selected) return null;
  // Position the SVG container ITSELF at the selected cell so the
  // viewport is one-cell-sized regardless of where the cell sits in
  // the heatmap. At a max matrix of 1024 rows × ~1000 cols this
  // avoids spawning a ~megapixel SVG just to draw one 8x4 outline
  // at the far corner. 2px stroke at full opacity reads against both
  // light and dark ramp colours; the rect itself is transparent so
  // the underlying intensity stays visible through the selection box.
  return (
    <svg
      width={cellW + 2}
      height={cellH + 2}
      style={{
        position: "absolute",
        top: selected.row * cellH - 1,
        left: selected.col * cellW - 1,
        pointerEvents: "none",
        overflow: "visible",
      }}
      aria-hidden="true"
    >
      <rect
        x={0}
        y={0}
        width={cellW + 2}
        height={cellH + 2}
        fill="none"
        stroke="currentColor"
        strokeWidth={2}
      />
    </svg>
  );
}

// FanoutBanner renders the degraded-mode strip above the heatmap when
// at least one peer failed to respond. Hidden when responded ===
// expected so a healthy cluster keeps the page clean. Lists every
// failed node with its error so operators can debug without checking
// per-node logs.
interface FanoutBannerProps {
  fanout: KeyVizFanoutResult;
}

function FanoutBanner({ fanout }: FanoutBannerProps) {
  if (fanout.responded >= fanout.expected) return null;
  const failed = fanout.nodes.filter((n) => !n.ok);
  return (
    <div className="card border-danger/40">
      <div className="text-sm font-semibold text-danger mb-2">
        Cluster view degraded — {fanout.responded} of {fanout.expected} nodes responded
      </div>
      <ul className="text-xs text-muted space-y-0.5">
        {failed.map((n) => (
          <li key={n.node} className="font-mono">
            <span className="text-danger">×</span> {n.node}
            {n.error && <span className="text-muted"> — {n.error}</span>}
          </li>
        ))}
      </ul>
      <p className="text-xs text-muted mt-2">
        The heatmap reflects the {fanout.responded} responding node{fanout.responded === 1 ? "" : "s"} only;
        traffic served by the failed peers is not shown until they recover.
      </p>
    </div>
  );
}

// ConflictOverlay layers a thin striped hatch over the cells whose
// fan-out merge produced disagreeing per-node values. Per design §9.1
// this signals that the cell's total is a best-effort dedup during a
// leadership flip and may understate the true window. The overlay is
// an SVG layered inside the scroll container so it tracks the canvas's
// scroll position (same idiom as TimeAxis).
//
// When a row carries per-cell `conflicts[]` (PR-3d server) we hatch
// only the affected columns; when it carries only the row-level
// `conflict` flag (legacy server, no per-cell array) we hatch the
// whole row as before. Patterns rather than colour because the canvas
// already uses colour for intensity; a hatch communicates "soft data
// here" without competing with the heatmap signal.
interface ConflictOverlayProps {
  rows: KeyVizRow[];
  cellH: number;
  cellW: number;
  width: number;
}

interface ConflictRect {
  x: number;
  y: number;
  w: number;
}

function ConflictOverlay({ rows, cellH, cellW, width }: ConflictOverlayProps) {
  const rects = useMemo(() => {
    const out: ConflictRect[] = [];
    for (let i = 0; i < rows.length; i++) {
      const row = rows[i];
      const cells = row.conflicts;
      if (cells && cells.length > 0) {
        // Per-cell: hatch the disagreeing columns, coalescing adjacent
        // runs into one <rect> so a long stretch of conflicting cells
        // does not emit hundreds of SVG nodes.
        let j = 0;
        while (j < cells.length) {
          if (!cells[j]) {
            j++;
            continue;
          }
          const startCol = j;
          while (j < cells.length && cells[j]) j++;
          out.push({ x: startCol * cellW, y: i * cellH, w: (j - startCol) * cellW });
        }
      } else if (row.conflict) {
        // Legacy server: only the row-level flag is available, so fall
        // back to hatching the entire row across all columns.
        out.push({ x: 0, y: i * cellH, w: width });
      }
    }
    return out;
  }, [rows, cellH, cellW, width]);
  if (rects.length === 0) return null;
  const totalH = rows.length * cellH;
  return (
    <svg
      width={width}
      height={totalH}
      style={{
        position: "absolute",
        top: 0,
        left: 0,
        pointerEvents: "none",
      }}
      aria-hidden="true"
    >
      <defs>
        {/* Diagonal hatch — 4px stride, 1px lines. The stroke uses
            currentColor so the overlay inherits the text colour and
            stays visible against both light and dark themes. */}
        <pattern
          id="keyviz-conflict-hatch"
          width={4}
          height={4}
          patternUnits="userSpaceOnUse"
          patternTransform="rotate(45)"
        >
          <line x1={0} y1={0} x2={0} y2={4} stroke="currentColor" strokeWidth={1} opacity={0.45} />
        </pattern>
      </defs>
      {rects.map((r, idx) => (
        <rect
          key={idx}
          x={r.x}
          y={r.y}
          width={r.w}
          height={cellH}
          fill="url(#keyviz-conflict-hatch)"
        />
      ))}
    </svg>
  );
}

interface TimeAxisProps {
  columnUnixMs: number[];
  cellW: number;
}

// timeAxisLabelMinPxGap is a conservative lower bound for the rendered
// width of an `HH:mm:ss` label at the 10px font size used by the axis
// (including a small inter-label gap). Using it as a minimum keeps
// labels from overlapping when cellW is small — without it, a 2px
// cell width with the previous "every column_count/10" stride would
// pack labels so tightly they would visually merge.
const timeAxisLabelMinPxGap = 56;

function TimeAxis({ columnUnixMs, cellW }: TimeAxisProps) {
  if (columnUnixMs.length === 0) return null;
  const minStrideForLabels =
    cellW > 0 ? Math.ceil(timeAxisLabelMinPxGap / cellW) : 1;
  const stride = Math.max(
    1,
    minStrideForLabels,
    Math.ceil(columnUnixMs.length / 10),
  );
  const ticks: { idx: number; label: string }[] = [];
  for (let i = 0; i < columnUnixMs.length; i += stride) {
    const d = new Date(columnUnixMs[i]);
    ticks.push({
      idx: i,
      label: `${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}`,
    });
  }
  return (
    <div className="relative h-4 text-[10px] text-muted">
      {ticks.map((t) => (
        <span
          key={t.idx}
          className="absolute font-mono"
          style={{ left: t.idx * cellW }}
        >
          {t.label}
        </span>
      ))}
    </div>
  );
}

function pad(n: number): string {
  return n < 10 ? `0${n}` : String(n);
}

interface RowDetailProps {
  row: KeyVizRow;
  index: number;
}

function RowDetail({ row, index }: RowDetailProps) {
  const total = row.values.reduce((a, b) => a + b, 0);
  return (
    <div className="card text-sm">
      <div className="flex items-center gap-2 mb-2 flex-wrap">
        <span className="text-xs text-muted">Row {index}</span>
        <span className="font-mono">{row.bucket_id}</span>
        {row.aggregate && <span className="pill-muted text-xs">aggregate</span>}
        {row.conflict && (
          <span
            className="pill-muted text-xs"
            title="Two or more nodes reported a non-zero value for the same cell — typical during a leadership flip mid-window. Displayed totals may understate the true count by up to one leader's pre-transfer slice."
          >
            conflict
          </span>
        )}
      </div>
      <dl className="grid grid-cols-2 gap-x-4 gap-y-1 text-xs">
        <dt className="text-muted">Start</dt>
        <dd className="font-mono break-all">{decodePreview(row.start)}</dd>
        <dt className="text-muted">End</dt>
        <dd className="font-mono break-all">{decodePreview(row.end)}</dd>
        <dt className="text-muted">Routes</dt>
        <dd className="font-mono">
          {row.route_count.toLocaleString()}
          {row.route_ids_truncated && (
            <span className="ml-1 text-muted">(truncated)</span>
          )}
        </dd>
        <dt className="text-muted">Total</dt>
        <dd className="font-mono">{total.toLocaleString()}</dd>
        {row.route_ids && row.route_ids.length > 0 && (
          <>
            <dt className="text-muted">Route IDs</dt>
            <dd className="font-mono break-all">
              {row.route_ids.slice(0, 12).join(", ")}
              {row.route_ids.length > 12 && "…"}
            </dd>
          </>
        )}
      </dl>
    </div>
  );
}

// parseBucketID maps a row's bucket_id back to the (routeID, subBucket,
// aggregate) tuple the hot-keys endpoint expects. The wire format
// (server: bucketIDFor) is:
//
//   "virtual:<routeID>"            — coarsened aggregate, no drill-down
//   "route:<routeID>"              — K=1 route, no sub-bucket axis
//   "route:<routeID>#<subBucket>"  — sub-bucket row of a K>1 route
//
// Returns null when the format is unrecognised so the panel can render
// a clear "unsupported bucket type" placeholder rather than throw.
interface BucketIDParts {
  kind: "virtual" | "route";
  routeID: number;
  subBucket?: number;
}

function parseBucketID(id: string): BucketIDParts | null {
  // Use Number() rather than Number.parseInt() so a malformed id
  // like "route:42abc" returns null instead of silently being read
  // as 42 (parseInt stops at the first non-numeric character;
  // Number() rejects trailing garbage as NaN). The server's
  // strconv.FormatUint output never contains garbage today, but
  // belt-and-braces parsing keeps a future server-side format
  // change from quietly mis-routing the hot-keys API call.
  if (id.startsWith("virtual:")) {
    const n = Number(id.slice("virtual:".length));
    if (!Number.isFinite(n)) return null;
    return { kind: "virtual", routeID: n };
  }
  if (id.startsWith("route:")) {
    const rest = id.slice("route:".length);
    const hash = rest.indexOf("#");
    if (hash < 0) {
      const n = Number(rest);
      if (!Number.isFinite(n)) return null;
      return { kind: "route", routeID: n };
    }
    const routeID = Number(rest.slice(0, hash));
    const sub = Number(rest.slice(hash + 1));
    if (!Number.isFinite(routeID) || !Number.isFinite(sub)) return null;
    return { kind: "route", routeID, subBucket: sub };
  }
  return null;
}

interface HotKeysPanelProps {
  row: KeyVizRow;
  column: number;
  columnUnixMs: number;
  series: KeyVizSeries;
  onClose: () => void;
}

// hotKeysTopK is the Top-K we ask the server for. The server-side
// capacity defaults to 64 and clamps top at the per-route capacity, so
// 20 always fits and matches the "hottest keys" density the design
// targets (§5). A future picker can lift this without server changes.
const hotKeysTopK = 20;

// HotKeysPanel renders the per-cell drill-down for the clicked
// (route, sub-bucket) tuple. Three branches the panel surfaces
// cleanly so users don't see a blank box waiting on a 503/404:
//
//   - series !== "writes"   — server only tracks the writes stream
//                              (design §3); show a static notice.
//   - bucket is aggregate   — design §2.2 excludes virtual buckets
//                              from sampling; nothing to drill into.
//   - 404 no_snapshot       — the aggregator hasn't published a window
//                              for this route yet (cold or just-reset).
//   - 503 hot_keys_disabled — operator started without
//                              --keyvizHotKeysEnabled.
function HotKeysPanel({ row, column, columnUnixMs, series, onClose }: HotKeysPanelProps) {
  const parts = useMemo(() => parseBucketID(row.bucket_id), [row.bucket_id]);
  const supported =
    series === "writes" && parts !== null && parts.kind === "route";

  return (
    <div className="card text-sm border-accent/40">
      <div className="flex items-center justify-between gap-2 mb-2 flex-wrap">
        <div className="flex items-center gap-2 flex-wrap">
          <span className="text-xs text-muted">Hot keys · cell</span>
          <span className="font-mono text-xs">{row.bucket_id}</span>
          {/* The clicked column locates the cell on the heatmap, but
              the hot-keys API always returns the LATEST snapshot
              (the SPA omits from/to_unix_ms by design). Label the
              column timestamp as "clicked" so users don't read it as
              the data's temporal anchor — the actual anchor is the
              `Snapshot at` row in the table below. */}
          <span className="text-xs text-muted">
            clicked column {column} (
            {new Date(columnUnixMs).toLocaleTimeString()})
          </span>
        </div>
        <button
          type="button"
          className="btn-secondary text-xs"
          onClick={onClose}
        >
          Close
        </button>
      </div>
      {!supported && (
        <HotKeysUnsupportedNotice row={row} parts={parts} series={series} />
      )}
      {supported && parts && parts.kind === "route" && (
        <HotKeysContent routeID={parts.routeID} subBucket={parts.subBucket} />
      )}
    </div>
  );
}

interface HotKeysUnsupportedNoticeProps {
  row: KeyVizRow;
  parts: BucketIDParts | null;
  series: KeyVizSeries;
}

function HotKeysUnsupportedNotice({ row, parts, series }: HotKeysUnsupportedNoticeProps) {
  if (series !== "writes") {
    return (
      <p className="text-xs text-muted">
        Hot-key drill-down is only sampled for the{" "}
        <code className="font-mono">writes</code> series. Switch the
        series picker to <code className="font-mono">writes</code> and
        click a cell to inspect the hottest keys in that route.
      </p>
    );
  }
  if (parts === null) {
    return (
      <p className="text-xs text-muted">
        Bucket id <code className="font-mono">{row.bucket_id}</code> is
        not in a recognised format; nothing to drill into.
      </p>
    );
  }
  // parts.kind === "virtual" — the row is a coarsened aggregate that
  // covers many routes (design §2.2 excludes these from per-route
  // sampling).
  return (
    <p className="text-xs text-muted">
      This row is an aggregate bucket covering{" "}
      {row.route_count.toLocaleString()} routes. Per-route hot-key
      drill-down is only available for individually tracked routes —
      lift{" "}
      <code className="font-mono">--keyvizMaxTrackedRoutes</code> to
      track this route directly.
    </p>
  );
}

interface HotKeysContentProps {
  routeID: number;
  subBucket?: number;
}

function HotKeysContent({ routeID, subBucket }: HotKeysContentProps) {
  const query = useApiQuery(
    (signal) =>
      api.keyVizHotKeys(
        {
          route_id: routeID,
          sub_bucket: subBucket,
          series: "writes",
          top: hotKeysTopK,
        },
        signal,
      ),
    [routeID, subBucket],
  );
  // useApiQuery keeps the previous response in `data` while a new
  // request is in flight (it doesn't reset on dep change), so when the
  // user clicks a different bucket the table would briefly render the
  // OLD bucket's keys under the NEW header. Compare the loaded
  // response's identity to the current (routeID, subBucket) so the
  // "loading" branch fires whenever they disagree. sub_bucket compares
  // by strict equality including undefined === undefined for K=1
  // routes.
  const hasMatchingData =
    !!query.data &&
    query.data.route_id === routeID &&
    query.data.sub_bucket === subBucket;
  if (query.loading && !hasMatchingData) {
    return <p className="text-xs text-muted">Loading hot keys…</p>;
  }
  if (query.error) {
    return <HotKeysErrorNotice error={query.error} />;
  }
  if (!hasMatchingData || !query.data) return null;
  return <HotKeysTable data={query.data} />;
}

interface HotKeysErrorNoticeProps {
  error: ApiError;
}

function HotKeysErrorNotice({ error }: HotKeysErrorNoticeProps) {
  if (error.status === 404) {
    return (
      <p className="text-xs text-muted">
        No snapshot yet for this route — the aggregator hasn't
        published a window since the route was registered, or every
        recent observe was length-skipped. Drive some traffic and
        click again.
      </p>
    );
  }
  if (error.status === 503) {
    // The handler returns two distinct 503 codes
    // (keyviz_hotkeys_handler.go): "keyviz_disabled" when the whole
    // sampler isn't configured, vs "hotkeys_disabled" when only
    // hot-keys is off. Pointing operators at the wrong flag wastes
    // a restart, so branch on error.code. Render the flags in
    // separate <code> elements so a long compound message doesn't
    // read as if the parenthetical is part of the flag name.
    if (error.code === "keyviz_disabled") {
      return (
        <p className="text-xs text-muted">
          KeyViz sampling is disabled on this node. Start the server
          with <code className="font-mono">--keyvizEnabled</code> and{" "}
          <code className="font-mono">--keyvizHotKeysEnabled</code>.
        </p>
      );
    }
    return (
      <p className="text-xs text-muted">
        Hot-key sampling is disabled on this node. Start the server
        with <code className="font-mono">--keyvizHotKeysEnabled</code>.
      </p>
    );
  }
  return (
    <p className="text-xs text-danger">{formatApiError(error)}</p>
  );
}

interface HotKeysTableProps {
  data: KeyVizHotKeysResponse;
}

function HotKeysTable({ data }: HotKeysTableProps) {
  return (
    <div className="space-y-2">
      <dl className="grid grid-cols-2 gap-x-4 gap-y-1 text-xs">
        <dt className="text-muted">Snapshot at</dt>
        <dd className="font-mono">
          {new Date(data.snapshot_at).toLocaleString()}
        </dd>
        <dt className="text-muted">Sampled N</dt>
        <dd className="font-mono">
          {data.sampled_n.toLocaleString()}
          <span className="ml-1 text-muted">
            (1 in {data.sample_rate.toLocaleString()})
          </span>
        </dd>
        <dt className="text-muted">Error bound</dt>
        <dd className="font-mono">
          ±{data.error_bound.toLocaleString()}
          {data.approximate && (
            <span className="ml-1 text-muted">(scaled estimate)</span>
          )}
        </dd>
        {(data.dropped_samples > 0 || data.skipped_long_keys > 0) && (
          <>
            <dt className="text-muted">Dropped / skipped</dt>
            <dd className="font-mono">
              {data.dropped_samples.toLocaleString()} dropped ·{" "}
              {data.skipped_long_keys.toLocaleString()} long-key skipped
            </dd>
          </>
        )}
      </dl>
      {data.degraded && (
        // Reusing the danger colour the FanoutBanner ("Cluster view
        // degraded") already uses keeps a consistent "degraded sample"
        // visual signal across the page rather than introducing a
        // third semantic colour just for this row.
        <p className="text-xs text-danger">
          Degraded window — some samples were dropped or skipped, so a
          true hot key might be missing from the list or its count
          under-reported.
        </p>
      )}
      {data.keys.length === 0 ? (
        <p className="text-xs text-muted">
          No keys in the snapshot — every observe in this window was
          length-skipped or hit a queue-full drop.
        </p>
      ) : (
        <table className="w-full text-xs">
          <thead className="text-muted">
            <tr>
              <th className="text-left font-normal w-6">#</th>
              <th className="text-left font-normal">Key (preview)</th>
              <th className="text-right font-normal">Count (scaled)</th>
            </tr>
          </thead>
          <tbody>
            {data.keys.map((k, i) => (
              <tr key={`${i}-${k.key_b64}`}>
                <td className="font-mono text-muted">{i + 1}</td>
                <td className="font-mono break-all">
                  {decodePreview(k.key_b64)}
                </td>
                <td className="font-mono text-right">
                  {k.count.toLocaleString()}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </div>
  );
}

// decodePreview turns a base64-encoded []byte from the wire into a
// short human-readable preview. Printable ASCII passes through; any
// byte outside [0x20, 0x7e] forces the hex form so binary keys do not
// render as garbled mojibake.
function decodePreview(b64: string): string {
  if (!b64) return "(empty)";
  let bin: string;
  try {
    bin = atob(b64);
  } catch {
    return `(invalid base64: ${b64})`;
  }
  let printable = true;
  for (let i = 0; i < bin.length; i++) {
    const c = bin.charCodeAt(i);
    if (c < 0x20 || c > 0x7e) {
      printable = false;
      break;
    }
  }
  if (printable) return bin;
  let hex = "0x";
  for (let i = 0; i < Math.min(bin.length, 32); i++) {
    hex += bin.charCodeAt(i).toString(16).padStart(2, "0");
  }
  if (bin.length > 32) hex += "…";
  return hex;
}

interface SeriesPickerProps {
  value: KeyVizSeries;
  onChange: (v: KeyVizSeries) => void;
}

function SeriesPicker({ value, onChange }: SeriesPickerProps) {
  return (
    <label className="flex items-center gap-1">
      <span className="text-xs text-muted">Series</span>
      <select
        className="input text-sm"
        value={value}
        onChange={(e) => onChange(e.target.value as KeyVizSeries)}
      >
        {seriesOptions.map((o) => (
          <option key={o.value} value={o.value}>
            {o.label}
          </option>
        ))}
      </select>
    </label>
  );
}

interface RowsInputProps {
  value: number;
  onChange: (v: number) => void;
}

function RowsInput({ value, onChange }: RowsInputProps) {
  // The committed row budget is held by the parent so the heatmap
  // refetches only when a valid value is committed. The `<input>`
  // value is a local string so the field can be cleared mid-edit
  // without forcing the parent to round-trip through 0/1 placeholder
  // values; we commit on blur and on Enter, and revert to the parent
  // value if the field ends up empty or invalid.
  const [draft, setDraft] = useState<string>(String(value));
  useEffect(() => {
    setDraft(String(value));
  }, [value]);

  const commit = () => {
    const n = Number.parseInt(draft, 10);
    if (Number.isFinite(n) && n > 0) {
      const clamped = Math.min(n, rowsCap);
      if (clamped !== value) onChange(clamped);
      setDraft(String(clamped));
    } else {
      setDraft(String(value));
    }
  };

  return (
    <label className="flex items-center gap-1">
      <span className="text-xs text-muted">Rows</span>
      <input
        type="number"
        min={1}
        max={rowsCap}
        step={1}
        className="input text-sm w-24"
        value={draft}
        onChange={(e) => setDraft(e.target.value)}
        onBlur={commit}
        onKeyDown={(e) => {
          if (e.key === "Enter") {
            commit();
            (e.target as HTMLInputElement).blur();
          }
        }}
      />
    </label>
  );
}

interface RefreshPickerProps {
  value: RefreshMode;
  onChange: (v: RefreshMode) => void;
}

function RefreshPicker({ value, onChange }: RefreshPickerProps) {
  return (
    <label className="flex items-center gap-1">
      <span className="text-xs text-muted">Auto</span>
      <select
        className="input text-sm"
        value={value}
        onChange={(e) => onChange(e.target.value as RefreshMode)}
      >
        {refreshOptions.map((o) => (
          <option key={o.value} value={o.value}>
            {o.label}
          </option>
        ))}
      </select>
    </label>
  );
}
