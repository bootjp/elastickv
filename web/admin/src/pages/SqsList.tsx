import { Link } from "react-router-dom";
import { api } from "../api/client";
import { formatApiError, useApiQuery } from "../lib/useApi";

export function SqsListPage() {
  const queues = useApiQuery((signal) => api.listQueues(signal), []);

  return (
    <div className="space-y-4">
      <header className="flex items-center justify-between">
        <div>
          <h1 className="text-xl font-semibold">SQS queues</h1>
          <p className="text-xs text-muted">
            Backed by the SigV4-bypass admin entrypoints in
            <code className="font-mono ml-1">adapter/sqs_admin.go</code>.
            Detail pages show the new approximate counters from Phase 3.A.
          </p>
        </div>
        <button type="button" className="btn-secondary" onClick={queues.reload}>
          Refresh
        </button>
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
    </div>
  );
}
