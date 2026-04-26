import { useState } from "react";
import { Link, useNavigate, useParams } from "react-router-dom";
import { api } from "../api/client";
import { useAuth } from "../auth";
import { Modal } from "../components/Modal";
import { formatApiError, useApiQuery } from "../lib/useApi";

export function DynamoDetailPage() {
  const { name = "" } = useParams<{ name: string }>();
  const { session } = useAuth();
  const detail = useApiQuery((signal) => api.describeTable(name, signal), [name]);
  const [confirmDelete, setConfirmDelete] = useState(false);
  const [deleting, setDeleting] = useState(false);
  const [deleteError, setDeleteError] = useState<string | null>(null);
  const navigate = useNavigate();
  const writeAllowed = session?.role === "full";

  const onDelete = async () => {
    setDeleting(true);
    setDeleteError(null);
    try {
      await api.deleteTable(name);
      navigate("/dynamo", { replace: true });
    } catch (err) {
      setDeleteError(formatApiError(err));
      setDeleting(false);
    }
  };

  return (
    <div className="space-y-4">
      <header className="flex items-center gap-3">
        <Link to="/dynamo" className="text-sm text-muted hover:text-ink">← All tables</Link>
        <h1 className="text-xl font-semibold font-mono ml-2">{name}</h1>
        {writeAllowed && (
          <button
            type="button"
            className="btn-danger ml-auto"
            onClick={() => setConfirmDelete(true)}
          >
            Delete table
          </button>
        )}
      </header>

      <section className="card">
        {detail.loading && <div className="text-sm text-muted">Loading…</div>}
        {detail.error?.status === 404 && (
          <div className="text-sm text-muted">
            Either the table does not exist or
            <code className="mx-1 font-mono">/admin/api/v1/dynamo/tables/{`{name}`}</code>
            is not wired yet.
          </div>
        )}
        {detail.error && detail.error.status !== 404 && (
          <div className="text-sm text-danger">{formatApiError(detail.error)}</div>
        )}
        {detail.data && (
          <dl className="grid grid-cols-2 gap-x-6 gap-y-2 text-sm">
            <dt className="text-muted">Partition key</dt>
            <dd className="font-mono">{detail.data.partition_key || "—"}</dd>
            <dt className="text-muted">Sort key</dt>
            <dd className="font-mono">{detail.data.sort_key || "—"}</dd>
            <dt className="text-muted">Generation</dt>
            <dd className="font-mono">{detail.data.generation}</dd>
          </dl>
        )}
      </section>
      {detail.data?.global_secondary_indexes && detail.data.global_secondary_indexes.length > 0 && (
        <section className="card">
          <h2 className="text-sm font-semibold mb-3">Global secondary indexes</h2>
          <table className="table">
            <thead>
              <tr>
                <th>Name</th>
                <th>Partition key</th>
                <th>Sort key</th>
                <th>Projection</th>
              </tr>
            </thead>
            <tbody>
              {detail.data.global_secondary_indexes.map((g) => (
                <tr key={g.name}>
                  <td className="font-mono">{g.name}</td>
                  <td className="font-mono">{g.partition_key}</td>
                  <td className="font-mono">{g.sort_key || "—"}</td>
                  <td className="font-mono text-xs">{g.projection_type}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </section>
      )}

      <Modal
        title="Delete table"
        open={confirmDelete}
        onClose={() => !deleting && setConfirmDelete(false)}
        busy={deleting}
      >
        <p className="text-sm">
          Permanently delete <code className="font-mono">{name}</code>? All items will be removed.
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
