import { useState } from "react";
import { Link, useNavigate, useParams } from "react-router-dom";
import { api } from "../api/client";
import { useAuth } from "../auth";
import { Modal } from "../components/Modal";
import { formatApiError, useApiQuery } from "../lib/useApi";

export function S3DetailPage() {
  const { name = "" } = useParams<{ name: string }>();
  const { session } = useAuth();
  const detail = useApiQuery((signal) => api.describeBucket(name, signal), [name]);
  const [aclBusy, setAclBusy] = useState(false);
  const [aclError, setAclError] = useState<string | null>(null);
  const [confirmDelete, setConfirmDelete] = useState(false);
  const [deleting, setDeleting] = useState(false);
  const [deleteError, setDeleteError] = useState<string | null>(null);
  const navigate = useNavigate();
  const writeAllowed = session?.role === "full";

  const setAcl = async (acl: "private" | "public-read") => {
    setAclBusy(true);
    setAclError(null);
    try {
      await api.putBucketAcl(name, acl);
      detail.reload();
    } catch (err) {
      setAclError(formatApiError(err));
    } finally {
      setAclBusy(false);
    }
  };

  const onDelete = async () => {
    setDeleting(true);
    setDeleteError(null);
    try {
      await api.deleteBucket(name);
      navigate("/s3", { replace: true });
    } catch (err) {
      setDeleteError(formatApiError(err));
      setDeleting(false);
    }
  };

  const currentAcl = detail.data?.acl ?? "private";

  return (
    <div className="space-y-4">
      <header className="flex items-center gap-3">
        <Link to="/s3" className="text-sm text-muted hover:text-ink">← All buckets</Link>
        <h1 className="text-xl font-semibold font-mono ml-2">{name}</h1>
        {writeAllowed && (
          <button
            type="button"
            className="btn-danger ml-auto"
            onClick={() => setConfirmDelete(true)}
          >
            Delete bucket
          </button>
        )}
      </header>

      <section className="card">
        {detail.loading && <div className="text-sm text-muted">Loading…</div>}
        {detail.error?.status === 404 && (
          <div className="text-sm text-muted">
            Bucket missing or admin endpoint not yet wired (design phase P2).
          </div>
        )}
        {detail.error && detail.error.status !== 404 && (
          <div className="text-sm text-danger">{formatApiError(detail.error)}</div>
        )}
        {detail.data && (
          <dl className="grid grid-cols-2 gap-x-6 gap-y-2 text-sm">
            <dt className="text-muted">ACL</dt>
            <dd className="font-mono">{currentAcl}</dd>
            <dt className="text-muted">Created</dt>
            <dd className="font-mono">
              {detail.data.created_at ? new Date(detail.data.created_at).toLocaleString() : "—"}
            </dd>
          </dl>
        )}
      </section>

      {writeAllowed && detail.data && (
        <section className="card">
          <h2 className="text-sm font-semibold mb-3">ACL</h2>
          <div className="flex items-center gap-2">
            <button
              type="button"
              className={currentAcl === "private" ? "btn-primary" : "btn-secondary"}
              onClick={() => setAcl("private")}
              disabled={aclBusy || currentAcl === "private"}
            >
              private
            </button>
            <button
              type="button"
              className={currentAcl === "public-read" ? "btn-primary" : "btn-secondary"}
              onClick={() => setAcl("public-read")}
              disabled={aclBusy || currentAcl === "public-read"}
            >
              public-read
            </button>
            {aclBusy && <span className="text-xs text-muted ml-2">Updating…</span>}
          </div>
          {aclError && <div className="text-sm text-danger mt-2">{aclError}</div>}
        </section>
      )}

      <Modal
        title="Delete bucket"
        open={confirmDelete}
        onClose={() => !deleting && setConfirmDelete(false)}
        busy={deleting}
      >
        <p className="text-sm">
          Permanently delete <code className="font-mono">{name}</code>? The bucket must be empty;
          the request will fail otherwise.
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
