import { useState, type FormEvent } from "react";
import { Navigate, useLocation, useNavigate } from "react-router-dom";
import { useAuth } from "../auth";
import { ApiError } from "../api/client";
import { formatApiError } from "../lib/useApi";

interface LocationState {
  from?: string;
}

export function LoginPage() {
  const { session, loading, login } = useAuth();
  const navigate = useNavigate();
  const location = useLocation();
  const [accessKey, setAccessKey] = useState("");
  const [secretKey, setSecretKey] = useState("");
  const [error, setError] = useState<string | null>(null);

  if (session) {
    const from = (location.state as LocationState | null)?.from ?? "/";
    return <Navigate to={from} replace />;
  }

  const onSubmit = async (e: FormEvent) => {
    e.preventDefault();
    setError(null);
    try {
      await login(accessKey.trim(), secretKey);
      const from = (location.state as LocationState | null)?.from ?? "/";
      navigate(from, { replace: true });
    } catch (err) {
      // Status-specific messaging beats raw codes here: 429 reads
      // very differently from 403 to a human typing credentials.
      if (err instanceof ApiError) {
        if (err.status === 429) {
          setError("Too many attempts. Wait a minute and try again.");
        } else if (err.status === 401) {
          setError("Invalid access key or secret key.");
        } else if (err.status === 403) {
          setError("This access key is not authorised for the admin dashboard.");
        } else {
          setError(formatApiError(err));
        }
      } else {
        setError(formatApiError(err));
      }
    }
  };

  return (
    <div className="min-h-screen flex items-center justify-center px-4">
      <form
        onSubmit={onSubmit}
        className="w-full max-w-sm rounded-lg border border-border bg-surface-2 p-6 space-y-4"
      >
        <div>
          <div className="text-lg font-semibold">elastickv admin</div>
          <p className="text-xs text-muted mt-1">
            Sign in with a SigV4 access key registered under
            <code className="mx-1 font-mono">admin.read_only_access_keys</code>
            or
            <code className="mx-1 font-mono">admin.full_access_keys</code>.
          </p>
        </div>
        <div>
          <label className="label" htmlFor="access_key">Access key</label>
          <input
            id="access_key"
            className="input font-mono"
            autoComplete="username"
            value={accessKey}
            onChange={(e) => setAccessKey(e.target.value)}
            required
          />
        </div>
        <div>
          <label className="label" htmlFor="secret_key">Secret key</label>
          <input
            id="secret_key"
            type="password"
            className="input font-mono"
            autoComplete="current-password"
            value={secretKey}
            onChange={(e) => setSecretKey(e.target.value)}
            required
          />
        </div>
        {error && (
          <div className="text-sm text-danger" role="alert">
            {error}
          </div>
        )}
        <button type="submit" className="btn-primary w-full" disabled={loading}>
          {loading ? "Signing in…" : "Sign in"}
        </button>
      </form>
    </div>
  );
}
