import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useRef,
  useState,
} from "react";
import type { ReactNode } from "react";
import { ApiError, api, type Role } from "./api/client";

interface Session {
  role: Role;
  expiresAt: Date;
}

interface AuthContextValue {
  session: Session | null;
  loading: boolean;
  login: (accessKey: string, secretKey: string) => Promise<void>;
  logout: () => Promise<void>;
  // markUnauthorized clears the in-memory session when an API call
  // surfaces 401 mid-flow. The browser cookie may already be expired;
  // forcing a re-login keeps the UI from looping on stale state.
  markUnauthorized: () => void;
}

const AuthContext = createContext<AuthContextValue | undefined>(undefined);

const STORAGE_KEY = "elastickv-admin.session.v1";

interface PersistedSession {
  role: Role;
  expires_at: string;
}

function loadPersisted(): Session | null {
  try {
    const raw = sessionStorage.getItem(STORAGE_KEY);
    if (!raw) return null;
    const obj = JSON.parse(raw) as PersistedSession;
    const expiresAt = new Date(obj.expires_at);
    if (Number.isNaN(expiresAt.getTime()) || expiresAt.getTime() <= Date.now()) {
      sessionStorage.removeItem(STORAGE_KEY);
      return null;
    }
    return { role: obj.role, expiresAt };
  } catch {
    sessionStorage.removeItem(STORAGE_KEY);
    return null;
  }
}

function persistSession(s: Session | null) {
  if (!s) {
    sessionStorage.removeItem(STORAGE_KEY);
    return;
  }
  const payload: PersistedSession = {
    role: s.role,
    expires_at: s.expiresAt.toISOString(),
  };
  sessionStorage.setItem(STORAGE_KEY, JSON.stringify(payload));
}

export function AuthProvider({ children }: { children: ReactNode }) {
  const [session, setSession] = useState<Session | null>(() => loadPersisted());
  const [loading, setLoading] = useState(false);
  const expiryTimer = useRef<number | undefined>(undefined);

  const clearTimer = useCallback(() => {
    if (expiryTimer.current !== undefined) {
      window.clearTimeout(expiryTimer.current);
      expiryTimer.current = undefined;
    }
  }, []);

  const setSessionAndPersist = useCallback(
    (next: Session | null) => {
      setSession(next);
      persistSession(next);
      clearTimer();
      if (next) {
        const ms = next.expiresAt.getTime() - Date.now();
        if (ms > 0) {
          expiryTimer.current = window.setTimeout(() => {
            setSession(null);
            persistSession(null);
          }, ms);
        }
      }
    },
    [clearTimer],
  );

  useEffect(() => clearTimer, [clearTimer]);

  const login = useCallback(
    async (accessKey: string, secretKey: string) => {
      setLoading(true);
      try {
        const res = await api.login(accessKey, secretKey);
        setSessionAndPersist({
          role: res.role,
          expiresAt: new Date(res.expires_at),
        });
      } finally {
        setLoading(false);
      }
    },
    [setSessionAndPersist],
  );

  const logout = useCallback(async () => {
    try {
      await api.logout();
    } catch (err) {
      // 401 from logout means the cookie is already gone — that is the
      // desired end state, so we still clear local session below. Any
      // other error is logged but does not block sign-out from the UI.
      if (!(err instanceof ApiError) || err.status !== 401) {
        console.warn("admin logout failed", err);
      }
    } finally {
      setSessionAndPersist(null);
    }
  }, [setSessionAndPersist]);

  const markUnauthorized = useCallback(() => {
    setSessionAndPersist(null);
  }, [setSessionAndPersist]);

  const value = useMemo<AuthContextValue>(
    () => ({ session, loading, login, logout, markUnauthorized }),
    [session, loading, login, logout, markUnauthorized],
  );

  return <AuthContext.Provider value={value}>{children}</AuthContext.Provider>;
}

export function useAuth(): AuthContextValue {
  const ctx = useContext(AuthContext);
  if (!ctx) throw new Error("useAuth must be used within AuthProvider");
  return ctx;
}
