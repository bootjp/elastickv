import { useState } from "react";
import { Link } from "react-router-dom";
import { api } from "../api/client";
import { useAuth } from "../auth";
import { Modal } from "../components/Modal";
import { formatApiError, useApiQuery } from "../lib/useApi";

export function SqsListPage() {
  const { session } = useAuth();
  const [open, setOpen] = useState(false);
  const [busy, setBusy] = useState(false);
  const queues = useApiQuery((signal) => api.listQueues(signal), []);

  return (
    <div className="space-y-4">
      <header className="flex items-center justify-between">
        <div>
          <h1 className="text-xl font-semibold">SQS queues</h1>
          <p className="text-xs text-muted">
            Create, describe, and delete SQS queues. Detail pages also surface
            the approximate visible / in-flight / delayed message counts.
          </p>
        </div>
        <div className="flex gap-2">
          <button type="button" className="btn-secondary" onClick={queues.reload}>
            Refresh
          </button>
          {session?.role === "full" && queues.error?.status !== 404 && (
            <button type="button" className="btn-primary" onClick={() => setOpen(true)}>
              Create queue
            </button>
          )}
        </div>
      </header>

      <section className="card">
        {queues.loading && <div className="text-sm text-muted">Loading…</div>}
        {queues.error?.status === 404 && (
          <div className="text-sm text-muted">
            SQS admin endpoints not wired on this build (the cluster was started
            without <code className="font-mono">--sqsAddress</code>, so the
            admin listener leaves <code className="font-mono">/admin/api/v1/sqs/*</code>
            off the wire).
          </div>
        )}
        {queues.error && queues.error.status !== 404 && (
          <div className="text-sm text-danger">{formatApiError(queues.error)}</div>
        )}
        {queues.data && queues.data.queues.length === 0 && (
          <div className="text-sm text-muted">No queues yet.</div>
        )}
        {queues.data && queues.data.queues.length > 0 && (
          <table className="table">
            <thead>
              <tr>
                <th>Queue</th>
                <th />
              </tr>
            </thead>
            <tbody>
              {queues.data.queues.map((name) => (
                <tr key={name}>
                  <td>
                    <Link className="font-mono text-accent hover:underline" to={`/sqs/${encodeURIComponent(name)}`}>
                      {name}
                    </Link>
                  </td>
                  <td className="text-right">
                    <Link className="text-xs text-muted hover:text-ink" to={`/sqs/${encodeURIComponent(name)}`}>
                      details →
                    </Link>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </section>
      <Modal title="Create SQS queue" open={open} busy={busy} onClose={() => setOpen(false)}>
        <CreateQueueForm busy={busy} setBusy={setBusy} onCancel={() => setOpen(false)} onCreated={() => {
          setOpen(false);
          queues.reload();
        }} />
      </Modal>
    </div>
  );
}

function CreateQueueForm({ busy, setBusy, onCancel, onCreated }: {
  busy: boolean;
  setBusy: (busy: boolean) => void;
  onCancel: () => void;
  onCreated: () => void;
}) {
  const [name, setName] = useState("");
  const [fifo, setFifo] = useState(false);
  const [deduplication, setDeduplication] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const onSubmit = async (event: React.FormEvent) => {
    event.preventDefault();
    if (busy) return;
    const queueName = name.trim();
    const valid = fifo
      ? /^[a-zA-Z0-9_-]+\.fifo$/.test(queueName)
      : /^[a-zA-Z0-9_-]+$/.test(queueName);
    if (!valid || queueName.length > 80) {
      setError("Use 1–80 letters, digits, hyphens or underscores. FIFO names must end in .fifo (included in the limit).");
      return;
    }
    setError(null);
    setBusy(true);
    try {
      await api.createQueue({
        name: queueName,
        attributes: fifo ? {
          FifoQueue: "true",
          ContentBasedDeduplication: String(deduplication),
        } : {},
      });
      onCreated();
    } catch (err) {
      setError(formatApiError(err));
    } finally {
      setBusy(false);
    }
  };

  return (
    <form onSubmit={onSubmit} className="space-y-3">
      <div>
        <label className="label" htmlFor="q-name">Queue name</label>
        <input id="q-name" className="input font-mono" value={name}
          onChange={(e) => setName(e.target.value)} required maxLength={80} disabled={busy}
          aria-describedby="q-name-help" />
        <p id="q-name-help" className="text-xs text-muted">
          1–80 letters, digits, hyphens or underscores. FIFO names must end in .fifo.
        </p>
      </div>
      <div>
        <label className="label" htmlFor="q-type">Queue type</label>
        <select id="q-type" className="input" value={fifo ? "fifo" : "standard"}
          onChange={(e) => setFifo(e.target.value === "fifo")} disabled={busy}>
          <option value="standard">Standard</option>
          <option value="fifo">FIFO</option>
        </select>
      </div>
      {fifo && (
        <label className="flex items-center gap-2 text-sm">
          <input type="checkbox" checked={deduplication} disabled={busy}
            onChange={(e) => setDeduplication(e.target.checked)} />
          Content-based deduplication
        </label>
      )}
      {error && <div role="alert" className="text-sm text-danger">{error}</div>}
      <div className="flex justify-end gap-2 pt-2">
        <button type="button" className="btn-secondary" onClick={onCancel} disabled={busy}>Cancel</button>
        <button type="submit" className="btn-primary" disabled={busy}>{busy ? "Creating…" : "Create"}</button>
      </div>
    </form>
  );
}
