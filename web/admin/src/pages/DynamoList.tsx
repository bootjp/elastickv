import { useState } from "react";
import { Link } from "react-router-dom";
import { api, type CreateTableRequest } from "../api/client";
import { useAuth } from "../auth";
import { Modal } from "../components/Modal";
import { formatApiError, useApiQuery } from "../lib/useApi";

export function DynamoListPage() {
  const { session } = useAuth();
  const tables = useApiQuery((_signal) => api.listTables(undefined), []);
  const [open, setOpen] = useState(false);
  const writeAllowed = session?.role === "full";

  return (
    <div className="space-y-4">
      <header className="flex items-center justify-between">
        <div>
          <h1 className="text-xl font-semibold">DynamoDB tables</h1>
          <p className="text-xs text-muted">
            Backed by the existing <code className="font-mono">CreateTable</code> /
            <code className="font-mono ml-1">ListTables</code> handlers in
            <code className="font-mono ml-1">adapter/dynamodb.go</code>.
          </p>
        </div>
        <div className="flex gap-2">
          <button type="button" className="btn-secondary" onClick={tables.reload}>
            Refresh
          </button>
          {writeAllowed && (
            <button type="button" className="btn-primary" onClick={() => setOpen(true)}>
              Create table
            </button>
          )}
        </div>
      </header>

      <section className="card">
        {tables.loading && <div className="text-sm text-muted">Loading…</div>}
        {tables.error?.status === 404 && (
          <PendingNotice phase="P1" />
        )}
        {tables.error && tables.error.status !== 404 && (
          <div className="text-sm text-danger">{formatApiError(tables.error)}</div>
        )}
        {tables.data && tables.data.tables.length === 0 && (
          <div className="text-sm text-muted">No tables yet.</div>
        )}
        {tables.data && tables.data.tables.length > 0 && (
          <table className="table">
            <thead>
              <tr>
                <th>Table</th>
                <th />
              </tr>
            </thead>
            <tbody>
              {tables.data.tables.map((name) => (
                <tr key={name}>
                  <td>
                    <Link className="font-mono text-accent hover:underline" to={`/dynamo/${encodeURIComponent(name)}`}>
                      {name}
                    </Link>
                  </td>
                  <td className="text-right">
                    <Link className="text-xs text-muted hover:text-ink" to={`/dynamo/${encodeURIComponent(name)}`}>
                      details →
                    </Link>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
        {tables.data?.next_token && (
          <div className="mt-3 text-xs text-muted">
            More tables exist. Pagination UI is pending (Section 4.3).
          </div>
        )}
      </section>

      <Modal title="Create DynamoDB table" open={open} onClose={() => setOpen(false)}>
        <CreateTableForm
          onCancel={() => setOpen(false)}
          onCreated={() => {
            setOpen(false);
            tables.reload();
          }}
        />
      </Modal>
    </div>
  );
}

interface CreateTableFormProps {
  onCancel: () => void;
  onCreated: () => void;
}

function CreateTableForm({ onCancel, onCreated }: CreateTableFormProps) {
  const [name, setName] = useState("");
  const [pkName, setPkName] = useState("id");
  const [pkType, setPkType] = useState<"S" | "N" | "B">("S");
  const [skName, setSkName] = useState("");
  const [skType, setSkType] = useState<"S" | "N" | "B">("S");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const onSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setError(null);
    setBusy(true);
    try {
      const req: CreateTableRequest = {
        table_name: name.trim(),
        partition_key: { name: pkName.trim(), type: pkType },
      };
      if (skName.trim()) {
        req.sort_key = { name: skName.trim(), type: skType };
      }
      await api.createTable(req);
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
        <label className="label" htmlFor="t-name">Table name</label>
        <input
          id="t-name"
          className="input font-mono"
          value={name}
          onChange={(e) => setName(e.target.value)}
          required
          minLength={3}
          pattern="[A-Za-z0-9_.\-]+"
        />
      </div>
      <fieldset className="space-y-2">
        <legend className="label">Partition key</legend>
        <div className="flex gap-2">
          <input
            className="input font-mono flex-1"
            placeholder="name"
            value={pkName}
            onChange={(e) => setPkName(e.target.value)}
            required
          />
          <select
            className="input w-24"
            value={pkType}
            onChange={(e) => setPkType(e.target.value as "S" | "N" | "B")}
          >
            <option value="S">S</option>
            <option value="N">N</option>
            <option value="B">B</option>
          </select>
        </div>
      </fieldset>
      <fieldset className="space-y-2">
        <legend className="label">Sort key (optional)</legend>
        <div className="flex gap-2">
          <input
            className="input font-mono flex-1"
            placeholder="name"
            value={skName}
            onChange={(e) => setSkName(e.target.value)}
          />
          <select
            className="input w-24"
            value={skType}
            onChange={(e) => setSkType(e.target.value as "S" | "N" | "B")}
          >
            <option value="S">S</option>
            <option value="N">N</option>
            <option value="B">B</option>
          </select>
        </div>
      </fieldset>
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

function PendingNotice({ phase }: { phase: string }) {
  return (
    <div className="text-sm text-muted">
      The DynamoDB admin endpoints are not yet wired on this build (design phase {phase}).
      Once <code className="font-mono">/admin/api/v1/dynamo/tables</code> ships, this page
      will populate without further frontend changes.
    </div>
  );
}
