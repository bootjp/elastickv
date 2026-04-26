import { useEffect, useMemo, useRef, useState } from "react";
import type { KeyVizMatrix, KeyVizRow, KeyVizSeries } from "../api/client";
import { api } from "../api/client";
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
    canvas.width = width;
    canvas.height = height;
    const ctx = canvas.getContext("2d");
    if (!ctx) return;
    ctx.clearRect(0, 0, width, height);
    if (matrix.rows.length === 0 || matrix.column_unix_ms.length === 0) return;

    // putImageData over the full canvas keeps render under the
    // §10 budget at 1024 × 500. We build the buffer column-major and
    // expand each cell into a cellW × cellH block via fillRect; this
    // avoids per-pixel iteration on the larger axis while still
    // letting the colour ramp run once per cell.
    for (let i = 0; i < matrix.rows.length; i++) {
      const row = matrix.rows[i];
      for (let j = 0; j < row.values.length; j++) {
        const v = row.values[j];
        if (v === 0) continue;
        const t = maxValue === 0 ? 0 : v / maxValue;
        const [r, g, b, a] = ramp(t);
        ctx.fillStyle = `rgba(${r}, ${g}, ${b}, ${a / 255})`;
        ctx.fillRect(j * cellW, i * cellH, cellW, cellH);
      }
    }
  }, [matrix, maxValue, width, height, cellW, cellH]);

  const onMove = (e: React.MouseEvent<HTMLCanvasElement>) => {
    const rect = e.currentTarget.getBoundingClientRect();
    const y = e.clientY - rect.top;
    const idx = Math.floor(y / cellH);
    if (idx >= 0 && idx < matrix.rows.length) setHoverRow(idx);
  };

  const onLeave = () => setHoverRow(null);

  return (
    <div className="space-y-3">
      <div className="flex items-center justify-between text-xs text-muted">
        <span>
          {matrix.rows.length} rows × {matrix.column_unix_ms.length} columns ·
          series <code className="font-mono">{matrix.series}</code> · max ={" "}
          {maxValue.toLocaleString()}
        </span>
        <span>{new Date(matrix.generated_at).toLocaleString()}</span>
      </div>
      {matrix.rows.length === 0 ? (
        <div className="text-sm text-muted">
          No tracked routes — drive some traffic and refresh.
        </div>
      ) : (
        <div className="overflow-auto border border-border rounded">
          <canvas
            ref={canvasRef}
            onMouseMove={onMove}
            onMouseLeave={onLeave}
            style={{ display: "block", width, height }}
          />
        </div>
      )}
      <TimeAxis columnUnixMs={matrix.column_unix_ms} cellW={cellW} />
      {hoverRow !== null && matrix.rows[hoverRow] && (
        <RowDetail row={matrix.rows[hoverRow]} index={hoverRow} />
      )}
    </div>
  );
}

interface TimeAxisProps {
  columnUnixMs: number[];
  cellW: number;
}

function TimeAxis({ columnUnixMs, cellW }: TimeAxisProps) {
  if (columnUnixMs.length === 0) return null;
  const stride = Math.max(1, Math.ceil(columnUnixMs.length / 10));
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
      <div className="flex items-center gap-2 mb-2">
        <span className="text-xs text-muted">Row {index}</span>
        <span className="font-mono">{row.bucket_id}</span>
        {row.aggregate && <span className="pill-muted text-xs">aggregate</span>}
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
  return (
    <label className="flex items-center gap-1">
      <span className="text-xs text-muted">Rows</span>
      <input
        type="number"
        min={1}
        max={rowsCap}
        step={1}
        className="input text-sm w-24"
        value={value}
        onChange={(e) => {
          const n = Number.parseInt(e.target.value, 10);
          if (Number.isFinite(n) && n > 0) onChange(Math.min(n, rowsCap));
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
