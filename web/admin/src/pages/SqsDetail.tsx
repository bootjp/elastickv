import { type FormEvent, useCallback, useEffect, useMemo, useRef, useState } from "react";
import { Link, useNavigate, useParams } from "react-router-dom";
import {
  api,
  type SqsPeekResult,
  type SqsPeekedMessage,
} from "../api/client";
import { Modal } from "../components/Modal";
import { formatApiError, useApiQuery } from "../lib/useApi";

// kPeekPageSize is the documented Phase 5 default the SPA sends on
// every Messages-tab fetch. The server clamps to [1, 100]; staying
// at 20 keeps the worst-case response (20 rows × 256 KiB) at 5 MiB
// well under typical network and JSON-parse budgets.
const kPeekPageSize = 20;

// kPeekBodyMaxBytes is the eager full-body fetch size: 256 KiB matches
// SQS's hard cap on stored message size, so the detail modal renders
// directly from the row already in memory — no re-peek round-trip on
// modal open, eliminating the "row disappeared between list and
// modal" failure modes (concurrent purge, ReceiveMessage from another
// client, visibility timer started). Trade is initial fetch size
// for modal-open consistency; design doc §3.5 lays out the reasoning.
const kPeekBodyMaxBytes = 262144;

export function SqsDetailPage() {
  const { name = "" } = useParams<{ name: string }>();
  const detail = useApiQuery((signal) => api.describeQueue(name, signal), [name]);
  const queues = useApiQuery((signal) => api.listQueues(signal), []);
  const [confirmDelete, setConfirmDelete] = useState(false);
  const [deleting, setDeleting] = useState(false);
  const [deleteError, setDeleteError] = useState<string | null>(null);
  const navigate = useNavigate();
  // The delete / purge buttons are gated by the backend's live-role
  // check (internal/admin/sqs_handler.go principalForWrite), not the
  // JWT role cached in this session. See SqsDetail's pre-Phase-5
  // comment for the rationale.

  const onDelete = async () => {
    setDeleting(true);
    setDeleteError(null);
    try {
      await api.deleteQueue(name);
      navigate("/sqs", { replace: true });
    } catch (err) {
      setDeleteError(formatApiError(err));
      setDeleting(false);
    }
  };

  return (
    <div className="space-y-4">
      <header className="flex items-center gap-3">
        <Link to="/sqs" className="text-sm text-muted hover:text-ink">← All queues</Link>
        <h1 className="text-xl font-semibold font-mono ml-2">{name}</h1>
        {detail.data && (
          <span className={detail.data.is_fifo ? "pill-accent" : "pill-muted"}>
            {detail.data.is_fifo ? "FIFO" : "Standard"}
          </span>
        )}
        {detail.data?.is_dlq && (
          <span className="pill-accent" title="Another queue's RedrivePolicy points at this queue">
            DLQ
          </span>
        )}
        {detail.data && (
          <button
            type="button"
            className="btn-danger ml-auto"
            onClick={() => setConfirmDelete(true)}
          >
            Delete queue
          </button>
        )}
      </header>

      <section className="card">
        {detail.loading && <div className="text-sm text-muted">Loading…</div>}
        {detail.error?.status === 404 && (
          <div className="text-sm text-muted">
            Either the queue does not exist or the SQS admin endpoints are not
            wired (no <code className="font-mono">--sqsAddress</code>).
          </div>
        )}
        {detail.error && detail.error.status !== 404 && (
          <div className="text-sm text-danger">{formatApiError(detail.error)}</div>
        )}
        {detail.data && (
          <dl className="grid grid-cols-2 gap-x-6 gap-y-2 text-sm">
            <dt className="text-muted">Generation</dt>
            <dd className="font-mono">{detail.data.generation}</dd>
            <dt className="text-muted">Created</dt>
            <dd className="font-mono">
              {detail.data.created_at ? new Date(detail.data.created_at).toLocaleString() : "—"}
            </dd>
          </dl>
        )}
      </section>

      {detail.data?.is_dlq && detail.data.dlq_sources && detail.data.dlq_sources.length > 0 && (
        <section className="card">
          <h2 className="text-sm font-semibold mb-3">DLQ for</h2>
          <div className="flex flex-wrap gap-2">
            {detail.data.dlq_sources.map((src) => (
              <Link
                key={src}
                to={`/sqs/${encodeURIComponent(src)}`}
                className="pill-muted hover:text-ink font-mono text-xs"
              >
                {src}
              </Link>
            ))}
          </div>
        </section>
      )}

      {detail.data && (
        <section className="card">
          <div className="flex items-center justify-between mb-3">
            <h2 className="text-sm font-semibold">Approximate message counts</h2>
          </div>
          <div className="grid grid-cols-3 gap-3">
            <CounterCard label="Visible (ready)" value={detail.data.counters.visible} />
            <CounterCard label="In flight" value={detail.data.counters.not_visible} />
            <CounterCard label="Delayed" value={detail.data.counters.delayed} />
          </div>
        </section>
      )}

      {detail.data && (
        <DLQSettingsSection
          queue={name}
          isFifo={detail.data.is_fifo}
          attributes={detail.data.attributes ?? {}}
          allQueues={queues.data?.queues ?? []}
          queuesLoading={queues.loading}
          queuesError={queues.error ? formatApiError(queues.error) : null}
          onSaved={() => detail.reload()}
        />
      )}

      {detail.data?.attributes && Object.keys(detail.data.attributes).length > 0 && (
        <section className="card">
          <h2 className="text-sm font-semibold mb-3">Configuration</h2>
          <dl className="grid grid-cols-2 gap-x-6 gap-y-2 text-sm">
            {Object.entries(detail.data.attributes).map(([k, v]) => (
              <div key={k} className="contents">
                <dt className="text-muted font-mono text-xs">{k}</dt>
                <dd className="font-mono break-all">{v}</dd>
              </div>
            ))}
          </dl>
        </section>
      )}

      {detail.data && (
        <MessagesSection
          queue={name}
          isFifo={detail.data.is_fifo}
          isDLQ={detail.data.is_dlq}
          inFlightCount={detail.data.counters.not_visible}
          onPurged={() => detail.reload()}
        />
      )}

      <Modal
        title="Delete queue"
        open={confirmDelete}
        onClose={() => !deleting && setConfirmDelete(false)}
        busy={deleting}
      >
        <p className="text-sm">
          Permanently delete <code className="font-mono">{name}</code>? All messages
          will be removed and the queue cannot be recovered.
        </p>
        {deleteError && <div className="mt-3 text-sm text-danger">{deleteError}</div>}
        <div className="flex justify-end gap-2 pt-4">
          <button
            type="button"
            className="btn-secondary"
            onClick={() => setConfirmDelete(false)}
            disabled={deleting}
          >
            Cancel
          </button>
          <button
            type="button"
            className="btn-danger"
            onClick={onDelete}
            disabled={deleting}
          >
            {deleting ? "Deleting…" : "Delete"}
          </button>
        </div>
      </Modal>
    </div>
  );
}

type RedrivePermission = "allowAll" | "denyAll" | "byQueue";

interface ParsedRedrivePolicy {
  targetName: string;
  maxReceiveCount: number;
}

interface ParsedRedriveAllowPolicy {
  permission: RedrivePermission;
  sourceQueueArns: string[];
}

interface DLQSettingsSectionProps {
  queue: string;
  isFifo: boolean;
  attributes: Record<string, string>;
  allQueues: string[];
  queuesLoading: boolean;
  queuesError: string | null;
  onSaved: () => void;
}

function DLQSettingsSection({
  queue,
  isFifo,
  attributes,
  allQueues,
  queuesLoading,
  queuesError,
  onSaved,
}: DLQSettingsSectionProps) {
  const arnPrefix = queueArnPrefix(attributes.QueueArn);
  const parsedRedrive = useMemo(
    () => parseRedrivePolicyAttribute(attributes.RedrivePolicy),
    [attributes.RedrivePolicy],
  );
  const parsedAllow = useMemo(
    () => parseRedriveAllowPolicyAttribute(attributes.RedriveAllowPolicy),
    [attributes.RedriveAllowPolicy],
  );
  const candidateQueues = useMemo(
    () => allQueues.filter((q) => q !== queue && isFifoQueueName(q) === isFifo),
    [allQueues, isFifo, queue],
  );
  const parsedAllowSourceText = parsedAllow.sourceQueueArns.join("\n");

  const [targetName, setTargetName] = useState(parsedRedrive.targetName);
  const [maxReceiveCount, setMaxReceiveCount] = useState(String(parsedRedrive.maxReceiveCount));
  const [redriveSaving, setRedriveSaving] = useState(false);
  const [redriveError, setRedriveError] = useState<string | null>(null);
  const [redriveSaved, setRedriveSaved] = useState<string | null>(null);

  const [permission, setPermission] = useState<RedrivePermission>(parsedAllow.permission);
  const [sourceQueueArnsText, setSourceQueueArnsText] = useState(parsedAllowSourceText);
  const [allowSaving, setAllowSaving] = useState(false);
  const [allowError, setAllowError] = useState<string | null>(null);
  const [allowSaved, setAllowSaved] = useState<string | null>(null);
  const redriveChoices = useMemo(() => {
    if (!targetName || candidateQueues.includes(targetName)) return candidateQueues;
    return [targetName, ...candidateQueues];
  }, [candidateQueues, targetName]);

  useEffect(() => {
    setTargetName(parsedRedrive.targetName);
    setMaxReceiveCount(String(parsedRedrive.maxReceiveCount));
    setRedriveError(null);
    setRedriveSaved(null);
  }, [queue, parsedRedrive.maxReceiveCount, parsedRedrive.targetName]);

  useEffect(() => {
    setPermission(parsedAllow.permission);
    setSourceQueueArnsText(parsedAllowSourceText);
    setAllowError(null);
    setAllowSaved(null);
  }, [queue, parsedAllow.permission, parsedAllowSourceText]);

  const onSaveRedrive = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    setRedriveError(null);
    setRedriveSaved(null);
    const dlq = targetName.trim();
    const maxReceive = Number(maxReceiveCount);
    if (!arnPrefix) {
      setRedriveError("QueueArn is not available for this queue.");
      return;
    }
    if (!dlq) {
      setRedriveError("Select a dead-letter queue.");
      return;
    }
    if (!Number.isInteger(maxReceive) || maxReceive < 1 || maxReceive > 1000) {
      setRedriveError("maxReceiveCount must be an integer from 1 to 1000.");
      return;
    }
    setRedriveSaving(true);
    try {
      await api.updateQueueAttributes(queue, {
        attributes: {
          RedrivePolicy: JSON.stringify({
            deadLetterTargetArn: queueArnForName(arnPrefix, dlq),
            maxReceiveCount: maxReceive,
          }),
        },
      });
      setRedriveSaved("Saved");
      onSaved();
    } catch (err) {
      setRedriveError(formatApiError(err));
    } finally {
      setRedriveSaving(false);
    }
  };

  const onSaveAllowPolicy = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    setAllowError(null);
    setAllowSaved(null);
    const policy: { redrivePermission: RedrivePermission; sourceQueueArns?: string[] } = {
      redrivePermission: permission,
    };
    if (permission === "byQueue") {
      const parsedSources = parseSourceQueueArnInput(sourceQueueArnsText, arnPrefix);
      if (parsedSources.needsArnPrefix) {
        setAllowError("QueueArn is not available to expand local queue names. Enter full source queue ARNs.");
        return;
      }
      if (parsedSources.sourceQueueArns.length === 0) {
        setAllowError("Enter at least one source queue ARN or local queue name.");
        return;
      }
      if (parsedSources.sourceQueueArns.length > 10) {
        setAllowError("sourceQueueArns can contain at most 10 queues.");
        return;
      }
      policy.sourceQueueArns = parsedSources.sourceQueueArns;
    }
    setAllowSaving(true);
    try {
      await api.updateQueueAttributes(queue, {
        attributes: { RedriveAllowPolicy: JSON.stringify(policy) },
      });
      setAllowSaved("Saved");
      onSaved();
    } catch (err) {
      setAllowError(formatApiError(err));
    } finally {
      setAllowSaving(false);
    }
  };

  return (
    <section className="card">
      <div className="flex items-center justify-between mb-3">
        <h2 className="text-sm font-semibold">DLQ settings</h2>
        {queuesLoading && <span className="text-xs text-muted">Loading queues…</span>}
      </div>
      {queuesError && <div className="text-sm text-danger mb-3">{queuesError}</div>}
      <div className="grid gap-4 lg:grid-cols-2">
        <form className="space-y-3" onSubmit={(event) => void onSaveRedrive(event)}>
          <div>
            <label className="label" htmlFor="redrive-dlq">Dead-letter queue</label>
            <select
              id="redrive-dlq"
              className="input font-mono"
              value={targetName}
              onChange={(e) => setTargetName(e.target.value)}
              disabled={redriveSaving}
            >
              <option value="">None selected</option>
              {redriveChoices.map((q) => (
                <option key={q} value={q}>{q}</option>
              ))}
            </select>
          </div>
          <div>
            <label className="label" htmlFor="redrive-max-receive">maxReceiveCount</label>
            <input
              id="redrive-max-receive"
              type="number"
              min={1}
              max={1000}
              step={1}
              className="input font-mono"
              value={maxReceiveCount}
              onChange={(e) => setMaxReceiveCount(e.target.value)}
              disabled={redriveSaving}
            />
          </div>
          {redriveError && <div className="text-sm text-danger">{redriveError}</div>}
          {redriveSaved && <div className="text-xs text-muted">{redriveSaved}</div>}
          <button type="submit" className="btn-primary" disabled={redriveSaving}>
            {redriveSaving ? "Saving…" : "Save redrive policy"}
          </button>
        </form>

        <form className="space-y-3" onSubmit={(event) => void onSaveAllowPolicy(event)}>
          <div>
            <label className="label" htmlFor="redrive-allow-permission">Redrive permission</label>
            <select
              id="redrive-allow-permission"
              className="input"
              value={permission}
              onChange={(e) => setPermission(e.target.value as RedrivePermission)}
              disabled={allowSaving}
            >
              <option value="allowAll">allowAll</option>
              <option value="denyAll">denyAll</option>
              <option value="byQueue">byQueue</option>
            </select>
          </div>
          {permission === "byQueue" && (
            <div>
              <label className="label" htmlFor="redrive-source-queues">Source queue ARNs</label>
              <textarea
                id="redrive-source-queues"
                className="input font-mono min-h-24"
                value={sourceQueueArnsText}
                onChange={(e) => setSourceQueueArnsText(e.target.value)}
                disabled={allowSaving}
                placeholder={candidateQueues
                  .map((name) => (arnPrefix ? queueArnForName(arnPrefix, name) : name))
                  .join("\n")}
              />
            </div>
          )}
          {allowError && <div className="text-sm text-danger">{allowError}</div>}
          {allowSaved && <div className="text-xs text-muted">{allowSaved}</div>}
          <button type="submit" className="btn-primary" disabled={allowSaving}>
            {allowSaving ? "Saving…" : "Save access policy"}
          </button>
        </form>
      </div>
    </section>
  );
}

function parseRedrivePolicyAttribute(raw?: string): ParsedRedrivePolicy {
  const fallback = { targetName: "", maxReceiveCount: 10 };
  if (!raw) return fallback;
  try {
    const obj = JSON.parse(raw) as Record<string, unknown>;
    const targetArn = typeof obj.deadLetterTargetArn === "string" ? obj.deadLetterTargetArn : "";
    const rawMax = obj.maxReceiveCount;
    let maxReceiveCount = fallback.maxReceiveCount;
    if (typeof rawMax === "number" && Number.isFinite(rawMax)) {
      maxReceiveCount = Math.trunc(rawMax);
    } else if (typeof rawMax === "string" && rawMax.trim() !== "") {
      const parsed = Number(rawMax);
      if (Number.isFinite(parsed)) maxReceiveCount = Math.trunc(parsed);
    }
    return {
      targetName: queueNameFromArn(targetArn),
      maxReceiveCount,
    };
  } catch {
    return fallback;
  }
}

function parseRedriveAllowPolicyAttribute(raw?: string): ParsedRedriveAllowPolicy {
  if (!raw) return { permission: "allowAll", sourceQueueArns: [] };
  try {
    const obj = JSON.parse(raw) as Record<string, unknown>;
    const permission = parseRedrivePermission(obj.redrivePermission);
    const sourceQueueArns = Array.isArray(obj.sourceQueueArns)
      ? obj.sourceQueueArns
        .filter((arn): arn is string => typeof arn === "string")
        .map((arn) => arn.trim())
        .filter((arn) => arn !== "")
      : [];
    return { permission, sourceQueueArns };
  } catch {
    return { permission: "allowAll", sourceQueueArns: [] };
  }
}

function parseRedrivePermission(value: unknown): RedrivePermission {
  if (value === "denyAll" || value === "byQueue") return value;
  return "allowAll";
}

function queueNameFromArn(arn: string): string {
  const idx = arn.lastIndexOf(":");
  if (idx < 0 || idx === arn.length - 1) return "";
  return arn.slice(idx + 1);
}

function queueArnPrefix(arn?: string): string {
  if (!arn) return "";
  const idx = arn.lastIndexOf(":");
  if (idx < 0) return "";
  return arn.slice(0, idx + 1);
}

function queueArnForName(prefix: string, name: string): string {
  return prefix + name;
}

function isFifoQueueName(name: string): boolean {
  return name.endsWith(".fifo");
}

function parseSourceQueueArnInput(value: string, arnPrefix: string): { sourceQueueArns: string[]; needsArnPrefix: boolean } {
  const seen = new Set<string>();
  const sourceQueueArns: string[] = [];
  let needsArnPrefix = false;
  for (const part of value.split(/[,\n]/u)) {
    const raw = part.trim();
    if (!raw) continue;
    const arn = raw.startsWith("arn:") ? raw : arnPrefix ? queueArnForName(arnPrefix, raw) : "";
    if (!arn) {
      needsArnPrefix = true;
      continue;
    }
    if (seen.has(arn)) continue;
    seen.add(arn);
    sourceQueueArns.push(arn);
  }
  return { sourceQueueArns, needsArnPrefix };
}

interface MessagesSectionProps {
  queue: string;
  isFifo: boolean;
  isDLQ: boolean;
  inFlightCount: number;
  onPurged: () => void;
}

// MessagesSection renders the design doc §3.5 Messages tab as a
// section beneath the queue's counters. Eager-fetches full bodies
// (256 KiB cap) so the detail modal renders directly from the row
// in memory, avoiding a re-peek race against concurrent purge /
// receive / visibility timer.
function MessagesSection({ queue, isFifo, isDLQ, inFlightCount, onPurged }: MessagesSectionProps) {
  const [result, setResult] = useState<SqsPeekResult | null>(null);
  const [cursor, setCursor] = useState<string | undefined>(undefined);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [selected, setSelected] = useState<SqsPeekedMessage | null>(null);
  const [confirmPurge, setConfirmPurge] = useState(false);
  const [purgeName, setPurgeName] = useState("");
  const [purging, setPurging] = useState(false);
  const [purgeError, setPurgeError] = useState<string | null>(null);
  // Cancels the prior peek so a slow response cannot overwrite newer state.
  const pendingAbortRef = useRef<AbortController | null>(null);

  const fetchPage = useCallback(
    async (pageCursor: string | undefined) => {
      pendingAbortRef.current?.abort();
      const ctl = new AbortController();
      pendingAbortRef.current = ctl;
      setLoading(true);
      setError(null);
      try {
        const res = await api.peekQueue(
          queue,
          { limit: kPeekPageSize, cursor: pageCursor, body_max_bytes: kPeekBodyMaxBytes },
          ctl.signal,
        );
        if (ctl.signal.aborted) return;
        setResult(res);
        setCursor(pageCursor);
        setLoading(false);
      } catch (err) {
        if (ctl.signal.aborted) return;
        if (err instanceof DOMException && err.name === "AbortError") return;
        // Clear stale rows so a failed fetch after a queue change does
        // not leave the prior queue's messages visible (Codex r3 P2).
        setResult(null);
        setCursor(undefined);
        setError(formatApiError(err));
        setLoading(false);
      }
    },
    [queue],
  );

  useEffect(() => {
    // Clear prior queue's table when the queue prop changes so the
    // brief loading window does not show cross-queue data.
    setResult(null);
    setCursor(undefined);
    void fetchPage(undefined);
    return () => pendingAbortRef.current?.abort();
  }, [fetchPage]);

  const onPurgeSubmit = async () => {
    if (purgeName !== queue) {
      setPurgeError(`Type "${queue}" exactly to confirm.`);
      return;
    }
    setPurging(true);
    setPurgeError(null);
    try {
      await api.purgeQueue(queue);
      setConfirmPurge(false);
      setPurgeName("");
      onPurged();
      void fetchPage(undefined);
    } catch (err) {
      // Server includes the remaining cooldown in both the
      // Retry-After header and the body's retry_after_seconds, but
      // ApiError only carries code+message. formatApiError already
      // includes the message ("only one PurgeQueue operation on each
      // queue is allowed every 60 seconds") so showing it verbatim
      // is sufficient; a future apiFetch enhancement could surface
      // the typed duration if it becomes worth it.
      setPurgeError(formatApiError(err));
    } finally {
      setPurging(false);
    }
  };

  const purgeLabel = isDLQ ? "Purge DLQ" : "Purge messages";
  const purgeConfirmCopy = isDLQ
    ? `This queue is the DLQ for one or more source queues. Purging deletes every failed message routed here. Type ${queue} to confirm.`
    : `This will permanently delete every message in ${queue}. The queue itself remains. Type ${queue} to confirm.`;

  return (
    <section className="card">
      <div className="flex items-center justify-between mb-3">
        <h2 className="text-sm font-semibold">Messages</h2>
        <button type="button" className="btn-danger text-xs" onClick={() => setConfirmPurge(true)}>
          {purgeLabel}
        </button>
      </div>
      <div className="text-xs text-muted mb-2">
        Showing {result?.messages.length ?? 0} visible message
        {(result?.messages.length ?? 0) === 1 ? "" : "s"}
        {inFlightCount > 0 && (
          <> ({inFlightCount} currently in-flight, not shown)</>
        )}
      </div>
      {error && <div className="text-sm text-danger mb-2">{error}</div>}
      {loading && <div className="text-sm text-muted">Loading…</div>}
      {!loading && result && result.messages.length === 0 && (
        <div className="text-sm text-muted">No visible messages.</div>
      )}
      {!loading && result && result.messages.length > 0 && (
        <MessagesTable
          messages={result.messages}
          showGroup={isFifo}
          onSelect={(m) => setSelected(m)}
        />
      )}
      <div className="flex items-center gap-2 mt-3 text-xs">
        <button
          type="button"
          className="btn-secondary text-xs"
          onClick={() => void fetchPage(undefined)}
          disabled={loading || cursor === undefined}
        >
          First page
        </button>
        <button
          type="button"
          className="btn-secondary text-xs"
          onClick={() => void fetchPage(result?.next_cursor)}
          disabled={loading || !result?.next_cursor}
        >
          Next page
        </button>
        <button
          type="button"
          className="btn-secondary text-xs ml-auto"
          onClick={() => void fetchPage(cursor)}
          disabled={loading}
        >
          Refresh
        </button>
      </div>

      <Modal title="Message detail" open={selected !== null} onClose={() => setSelected(null)}>
        {selected && <MessageDetail message={selected} queue={queue} />}
      </Modal>

      <Modal
        title={purgeLabel}
        open={confirmPurge}
        onClose={() => {
          if (purging) return;
          setConfirmPurge(false);
          setPurgeName("");
          setPurgeError(null);
        }}
        busy={purging}
      >
        <p className="text-sm">{purgeConfirmCopy}</p>
        <label className="block mt-3 text-xs text-muted">Type the queue name to confirm</label>
        <input
          type="text"
          className="input mt-1 font-mono"
          value={purgeName}
          onChange={(e) => setPurgeName(e.target.value)}
          disabled={purging}
          autoFocus
        />
        {purgeError && <div className="mt-3 text-sm text-danger">{purgeError}</div>}
        <div className="flex justify-end gap-2 pt-4">
          <button
            type="button"
            className="btn-secondary"
            onClick={() => {
              setConfirmPurge(false);
              setPurgeName("");
              setPurgeError(null);
            }}
            disabled={purging}
          >
            Cancel
          </button>
          <button
            type="button"
            className="btn-danger"
            onClick={() => void onPurgeSubmit()}
            disabled={purging || purgeName !== queue}
          >
            {purging ? "Purging…" : purgeLabel}
          </button>
        </div>
      </Modal>
    </section>
  );
}

interface MessagesTableProps {
  messages: SqsPeekedMessage[];
  showGroup: boolean;
  onSelect: (m: SqsPeekedMessage) => void;
}

function MessagesTable({ messages, showGroup, onSelect }: MessagesTableProps) {
  return (
    <table className="w-full text-xs">
      <thead className="text-muted text-left">
        <tr>
          <th className="py-1 pr-3">Message ID</th>
          <th className="py-1 pr-3">Sent</th>
          {showGroup && <th className="py-1 pr-3">Group</th>}
          <th className="py-1 pr-3">Recv</th>
          <th className="py-1 pr-3">Body</th>
          <th className="py-1 pr-3">Size</th>
        </tr>
      </thead>
      <tbody>
        {messages.map((m) => (
          <tr
            key={m.message_id}
            className="border-t border-border hover:bg-surface cursor-pointer"
            onClick={() => onSelect(m)}
          >
            <td className="py-1 pr-3 font-mono" title={m.message_id}>
              {m.message_id.slice(0, 8)}
            </td>
            <td className="py-1 pr-3 font-mono">{new Date(m.sent_timestamp).toLocaleString()}</td>
            {showGroup && <td className="py-1 pr-3 font-mono">{m.group_id ?? ""}</td>}
            <td className={`py-1 pr-3 font-mono ${m.receive_count === 0 ? "text-muted" : ""}`}>
              {m.receive_count}
            </td>
            <td className="py-1 pr-3 font-mono truncate max-w-md">{previewBody(m)}</td>
            <td className="py-1 pr-3 font-mono text-muted">{formatBytes(m.body_original_size)}</td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}

interface MessageDetailProps {
  message: SqsPeekedMessage;
  queue: string;
}

function MessageDetail({ message, queue }: MessageDetailProps) {
  const [copied, setCopied] = useState(false);
  const [copyError, setCopyError] = useState<string | null>(null);
  const onCopyJson = async () => {
    // Schema version per design doc §3.5; downstream tooling pins it.
    const payload = {
      schema_version: 1,
      queue,
      exported_at: new Date().toISOString(),
      message,
    };
    const json = JSON.stringify(payload, null, 2);
    // navigator.clipboard is secure-context-only. On insecure /
    // unavailable, point the operator at the body <pre> in the
    // modal rather than window.prompt (blocking modal pre-empts the
    // error message and some browsers truncate ~350KiB payloads).
    if (typeof navigator !== "undefined" && navigator.clipboard?.writeText) {
      try {
        await navigator.clipboard.writeText(json);
        setCopied(true);
        setCopyError(null);
        setTimeout(() => setCopied(false), 1500);
        return;
      } catch (err) {
        setCopyError(`Copy failed: ${String(err)}. Select the body text above to copy manually.`);
        return;
      }
    }
    setCopyError("Clipboard API unavailable (insecure context). Select the body text above to copy manually.");
  };

  return (
    <div className="space-y-3 text-sm">
      <dl className="grid grid-cols-3 gap-x-4 gap-y-1 text-xs">
        <dt className="text-muted">Message ID</dt>
        <dd className="col-span-2 font-mono">{message.message_id}</dd>
        <dt className="text-muted">Sent</dt>
        <dd className="col-span-2 font-mono">{new Date(message.sent_timestamp).toLocaleString()}</dd>
        <dt className="text-muted">Receive count</dt>
        <dd className="col-span-2 font-mono">{message.receive_count}</dd>
        {message.group_id && (
          <>
            <dt className="text-muted">Group ID</dt>
            <dd className="col-span-2 font-mono">{message.group_id}</dd>
          </>
        )}
        {message.deduplication_id && (
          <>
            <dt className="text-muted">Deduplication ID</dt>
            <dd className="col-span-2 font-mono">{message.deduplication_id}</dd>
          </>
        )}
        <dt className="text-muted">Original size</dt>
        <dd className="col-span-2 font-mono">{formatBytes(message.body_original_size)}</dd>
      </dl>
      <div>
        <div className="flex items-center justify-between mb-1">
          <span className="text-xs text-muted">Body</span>
          {message.body_truncated && (
            <span className="text-xs text-danger">
              Truncated to {formatBytes(utf8ByteLength(message.body))} of {formatBytes(message.body_original_size)}
            </span>
          )}
        </div>
        <pre className="text-xs font-mono bg-surface p-2 rounded overflow-x-auto max-h-64 whitespace-pre-wrap break-all">
          {message.body}
        </pre>
      </div>
      {message.attributes && Object.keys(message.attributes).length > 0 && (
        <div>
          <div className="text-xs text-muted mb-1">Attributes</div>
          <dl className="grid grid-cols-[max-content_max-content_1fr] gap-x-4 gap-y-1 text-xs">
            {Object.entries(message.attributes).map(([k, v]) => (
              <div key={k} className="contents">
                <dt className="font-mono">{k}</dt>
                <dd className="text-muted">{v.data_type}</dd>
                <dd className="font-mono break-all">
                  {v.string_value ?? (v.binary_value ? `<binary, ${base64DecodedByteLength(v.binary_value)} bytes>` : "")}
                </dd>
              </div>
            ))}
          </dl>
        </div>
      )}
      {copyError && <div className="text-xs text-danger">{copyError}</div>}
      <div className="flex justify-end">
        <button
          type="button"
          className="btn-secondary text-xs"
          onClick={() => void onCopyJson()}
        >
          {copied ? "Copied!" : "Copy as JSON"}
        </button>
      </div>
    </div>
  );
}

function CounterCard({ label, value }: { label: string; value: number }) {
  return (
    <div className="rounded-md border border-border bg-surface p-3">
      <div className="text-xs uppercase tracking-wide text-muted">{label}</div>
      <div className="text-2xl font-semibold mt-1 font-mono">{value}</div>
    </div>
  );
}

function previewBody(m: SqsPeekedMessage): string {
  const max = 96;
  if (m.body.length <= max) return m.body || "(empty)";
  return m.body.slice(0, max) + "…";
}

function formatBytes(n: number): string {
  if (n < 1024) return `${n} B`;
  if (n < 1024 * 1024) return `${(n / 1024).toFixed(1)} KiB`;
  return `${(n / (1024 * 1024)).toFixed(1)} MiB`;
}

// utf8ByteLength: server applies BodyMaxBytes to UTF-8 bytes, not
// UTF-16 code units; CJK/emoji bodies would otherwise under-report.
function utf8ByteLength(s: string): number {
  if (typeof TextEncoder !== "undefined") {
    return new TextEncoder().encode(s).byteLength;
  }
  return s.length;
}

// Decoded length without actually decoding: 4 chars → 3 bytes, minus padding.
function base64DecodedByteLength(b64: string): number {
  if (b64.length === 0) return 0;
  let padding = 0;
  if (b64.endsWith("==")) padding = 2;
  else if (b64.endsWith("=")) padding = 1;
  return Math.floor(b64.length * 3 / 4) - padding;
}
