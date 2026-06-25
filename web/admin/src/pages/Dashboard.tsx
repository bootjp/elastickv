import type { ApiError } from "../api/client";
import { api } from "../api/client";
import { formatApiError, useApiQuery } from "../lib/useApi";

export function DashboardPage() {
  const cluster = useApiQuery((signal) => api.cluster(signal), []);
  const tables = useApiQuery((signal) => api.listTables(undefined, signal), []);
  const buckets = useApiQuery((signal) => api.listBuckets(undefined, signal), []);

  return (
    <div className="space-y-6">
      <header className="flex items-center justify-between">
        <h1 className="text-xl font-semibold">Cluster overview</h1>
        <button type="button" className="btn-secondary" onClick={cluster.reload}>
          Refresh
        </button>
      </header>

      <section className="grid grid-cols-1 sm:grid-cols-3 gap-4">
        <SummaryCard
          label="Raft groups"
          value={cluster.data ? cluster.data.groups.length.toString() : null}
          loading={cluster.loading}
          error={cluster.error}
        />
        <SummaryCard
          label="DynamoDB tables"
          value={tables.data ? tables.data.tables.length.toString() : null}
          loading={tables.loading}
          error={tables.error}
          pendingMessage="Endpoint pending (Phase P1)"
        />
        <SummaryCard
          label="S3 buckets"
          value={buckets.data ? buckets.data.buckets.length.toString() : null}
          loading={buckets.loading}
          error={buckets.error}
          pendingMessage="Endpoint pending (Phase P2)"
        />
      </section>

      <section className="card">
        <div className="flex items-center justify-between mb-3">
          <h2 className="text-sm font-semibold">Local node</h2>
          {cluster.data && (
            <span className="text-xs text-muted">
              {new Date(cluster.data.timestamp).toLocaleString()}
            </span>
          )}
        </div>
        {cluster.loading && <div className="text-sm text-muted">Loading…</div>}
        {cluster.error && (
          <div className="text-sm text-danger">{formatApiError(cluster.error)}</div>
        )}
        {cluster.data && (
          <dl className="grid grid-cols-2 gap-x-4 gap-y-2 text-sm">
            <dt className="text-muted">Node ID</dt>
            <dd className="font-mono">{cluster.data.node_id || "—"}</dd>
            <dt className="text-muted">Build</dt>
            <dd className="font-mono">{cluster.data.version}</dd>
          </dl>
        )}
      </section>

      <section className="card">
        <h2 className="text-sm font-semibold mb-3">Raft groups</h2>
        {cluster.data && cluster.data.groups.length === 0 && (
          <div className="text-sm text-muted">No raft groups reported.</div>
        )}
        {cluster.data && cluster.data.groups.length > 0 && (
          <table className="table">
            <thead>
              <tr>
                <th>Group</th>
                <th>Leader</th>
                <th>Members</th>
                <th>Local role</th>
              </tr>
            </thead>
            <tbody>
              {cluster.data.groups.map((g) => (
                <tr key={g.group_id}>
                  <td className="font-mono">#{g.group_id}</td>
                  <td className="font-mono">{g.leader_id || <span className="text-muted">election…</span>}</td>
                  <td className="font-mono text-xs text-muted">{g.members.join(", ") || "—"}</td>
                  <td>
                    <span className={g.is_leader ? "pill-accent" : "pill-muted"}>
                      {g.is_leader ? "leader" : "follower"}
                    </span>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </section>
    </div>
  );
}

interface SummaryCardProps {
  label: string;
  value: string | null;
  loading: boolean;
  error: ApiError | null;
  pendingMessage?: string;
}

function SummaryCard({ label, value, loading, error, pendingMessage }: SummaryCardProps) {
  // 404 from a not-yet-wired backend handler is the "endpoint pending"
  // signal. Surface it as a soft notice instead of a red error so the
  // overview page reads correctly during P1/P2 rollout.
  const pending = error?.status === 404 && pendingMessage;
  return (
    <div className="card">
      <div className="text-xs uppercase tracking-wide text-muted">{label}</div>
      {loading && <div className="text-2xl font-semibold mt-1">…</div>}
      {!loading && value !== null && <div className="text-2xl font-semibold mt-1">{value}</div>}
      {!loading && pending && (
        <div className="text-xs text-muted mt-2">{pendingMessage}</div>
      )}
      {!loading && error && !pending && (
        <div className="text-xs text-danger mt-2">{formatApiError(error)}</div>
      )}
    </div>
  );
}

