import { useState } from "react";
import { Link, useNavigate, useParams } from "react-router-dom";
import { api } from "../api/client";
import { useAuth } from "../auth";
import { Modal } from "../components/Modal";
import { formatApiError, useApiQuery } from "../lib/useApi";

export function SqsDetailPage() {
  const { name = "" } = useParams<{ name: string }>();
  const { session } = useAuth();
  const detail = useApiQuery((signal) => api.describeQueue(name, signal), [name]);
  const [confirmDelete, setConfirmDelete] = useState(false);
  const [deleting, setDeleting] = useState(false);
  const [deleteError, setDeleteError] = useState<string | null>(null);
  const navigate = useNavigate();
  const writeAllowed = session?.role === "full";

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
        {writeAllowed && detail.data && (
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

      {detail.data && (
        <section className="card">
          <div className="flex items-center justify-between mb-3">
            <h2 className="text-sm font-semibold">Approximate message counts</h2>
            {detail.data.counters_truncated && (
              <span className="pill-muted text-xs" title="The visibility-index scan hit its per-call budget. The values shown are a lower bound (Phase 3.A).">
                truncated
              </span>
            )}
          </div>
          <div className="grid grid-cols-3 gap-3">
            <CounterCard label="Visible (ready)" value={detail.data.counters.visible} />
            <CounterCard label="In flight" value={detail.data.counters.not_visible} />
            <CounterCard label="Delayed" value={detail.data.counters.delayed} />
          </div>
        </section>
      )}

      {detail.data?.attributes && Object.keys(detail.data.attributes).length > 0 && (
        <section className="card">
          <h2 className="text-sm font-semibold mb-3">Configuration</h2>
          <dl className="grid grid-cols-2 gap-x-6 gap-y-2 text-sm">
            {Object.entries(detail.data.attributes).map(([k, v]) => (
              <div key={k} className="contents">
                <dt className="text-muted font-mono text-xs">{k}</dt>
                <dd className="font-mono">{v}</dd>
              </div>
            ))}
          </dl>
        </section>
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

function CounterCard({ label, value }: { label: string; value: number }) {
  return (
    <div className="rounded-md border border-border bg-surface p-3">
      <div className="text-xs uppercase tracking-wide text-muted">{label}</div>
      <div className="text-2xl font-semibold mt-1 font-mono">{value}</div>
    </div>
  );
}
