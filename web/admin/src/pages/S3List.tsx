import { useState } from "react";
import { Link } from "react-router-dom";
import { api, type CreateBucketRequest } from "../api/client";
import { useAuth } from "../auth";
import { Modal } from "../components/Modal";
import { formatApiError, useApiQuery } from "../lib/useApi";

export function S3ListPage() {
  const { session } = useAuth();
  const buckets = useApiQuery((_signal) => api.listBuckets(), []);
  const [open, setOpen] = useState(false);
  const writeAllowed = session?.role === "full";

  return (
    <div className="space-y-4">
      <header className="flex items-center justify-between">
        <div>
          <h1 className="text-xl font-semibold">S3 buckets</h1>
          <p className="text-xs text-muted">
            Backed by the existing handlers in
            <code className="font-mono ml-1">adapter/s3.go</code>;
            create + ACL flow lands as a single Raft commit (Section 4.2).
          </p>
        </div>
        <div className="flex gap-2">
          <button type="button" className="btn-secondary" onClick={buckets.reload}>
            Refresh
          </button>
          {writeAllowed && (
            <button type="button" className="btn-primary" onClick={() => setOpen(true)}>
              Create bucket
            </button>
          )}
        </div>
      </header>

      <section className="card">
        {buckets.loading && <div className="text-sm text-muted">Loading…</div>}
        {buckets.error?.status === 404 && (
          <div className="text-sm text-muted">
            S3 admin endpoints not yet wired (design phase P2).
          </div>
        )}
        {buckets.error && buckets.error.status !== 404 && (
          <div className="text-sm text-danger">{formatApiError(buckets.error)}</div>
        )}
        {buckets.data && buckets.data.buckets.length === 0 && (
          <div className="text-sm text-muted">No buckets yet.</div>
        )}
        {buckets.data && buckets.data.buckets.length > 0 && (
          <table className="table">
            <thead>
              <tr>
                <th>Bucket</th>
                <th>ACL</th>
                <th>Created</th>
                <th />
              </tr>
            </thead>
            <tbody>
              {buckets.data.buckets.map((b) => (
                <tr key={b.bucket_name}>
                  <td>
                    <Link className="font-mono text-accent hover:underline" to={`/s3/${encodeURIComponent(b.bucket_name)}`}>
                      {b.bucket_name}
                    </Link>
                  </td>
                  <td>
                    <span className="pill-muted font-mono">{b.acl ?? "private"}</span>
                  </td>
                  <td className="text-xs text-muted">
                    {b.created_at ? new Date(b.created_at).toLocaleString() : "—"}
                  </td>
                  <td className="text-right">
                    <Link className="text-xs text-muted hover:text-ink" to={`/s3/${encodeURIComponent(b.bucket_name)}`}>
                      details →
                    </Link>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </section>

      <Modal title="Create S3 bucket" open={open} onClose={() => setOpen(false)}>
        <CreateBucketForm
          onCancel={() => setOpen(false)}
          onCreated={() => {
            setOpen(false);
            buckets.reload();
          }}
        />
      </Modal>
    </div>
  );
}

function CreateBucketForm({
  onCancel,
  onCreated,
}: {
  onCancel: () => void;
  onCreated: () => void;
}) {
  const [name, setName] = useState("");
  const [acl, setAcl] = useState<"private" | "public-read">("private");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const onSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setError(null);
    setBusy(true);
    try {
      const req: CreateBucketRequest = { bucket_name: name.trim(), acl };
      await api.createBucket(req);
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
        <label className="label" htmlFor="b-name">Bucket name</label>
        <input
          id="b-name"
          className="input font-mono"
          value={name}
          onChange={(e) => setName(e.target.value)}
          required
          minLength={3}
          maxLength={63}
          pattern="[a-z0-9][a-z0-9.\-]+[a-z0-9]"
          title="3–63 chars; lowercase letters, digits, '.', '-'."
        />
      </div>
      <div>
        <label className="label" htmlFor="b-acl">Initial ACL</label>
        <select
          id="b-acl"
          className="input"
          value={acl}
          onChange={(e) => setAcl(e.target.value as "private" | "public-read")}
        >
          <option value="private">private</option>
          <option value="public-read">public-read</option>
        </select>
      </div>
      {error && <div className="text-sm text-danger">{error}</div>}
      <div className="flex justify-end gap-2 pt-2">
        <button type="button" className="btn-secondary" onClick={onCancel} disabled={busy}>
          Cancel
        </button>
        <button type="submit" className="btn-primary" disabled={busy}>
          {busy ? "Creating…" : "Create"}
        </button>
      </div>
    </form>
  );
}
