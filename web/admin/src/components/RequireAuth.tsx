import { Navigate, useLocation } from "react-router-dom";
import type { ReactNode } from "react";
import { useAuth } from "../auth";

// RequireAuth bounces unauthenticated visitors to /login and preserves
// the originally requested path on `state.from` so the login form can
// send them back where they intended to go.
export function RequireAuth({ children }: { children: ReactNode }) {
  const { session } = useAuth();
  const location = useLocation();
  if (!session) {
    return <Navigate to="/login" replace state={{ from: location.pathname + location.search }} />;
  }
  return <>{children}</>;
}

// RequireFullAccess gates write-only pages. Read-only sessions land
// here when they try to navigate to a /create or /edit route; the
// component renders an inline notice rather than a full redirect so
// the user still sees the surrounding nav.
export function RequireFullAccess({ children }: { children: ReactNode }) {
  const { session } = useAuth();
  if (!session) return null;
  if (session.role !== "full") {
    return (
      <div className="card text-sm">
        <div className="font-medium">Read-only session</div>
        <p className="text-muted mt-1">
          This action requires a full-access access key. Sign in with a key listed under
          <code className="mx-1 font-mono text-xs">admin.full_access_keys</code>
          to perform write operations.
        </p>
      </div>
    );
  }
  return <>{children}</>;
}
