import { useCallback, useEffect, useRef, useState } from "react";
import { ApiError } from "../api/client";
import { useAuth } from "../auth";

interface UseApiState<T> {
  data: T | null;
  error: ApiError | null;
  loading: boolean;
  reload: () => void;
}

// useApiQuery wraps a one-shot loader with abort, error normalisation,
// and a global 401 → "session is gone" reaction. Keeping that reaction
// here means individual pages do not have to reimplement the redirect.
export function useApiQuery<T>(
  loader: (signal: AbortSignal) => Promise<T>,
  deps: ReadonlyArray<unknown>,
): UseApiState<T> {
  const { markUnauthorized } = useAuth();
  const [data, setData] = useState<T | null>(null);
  const [error, setError] = useState<ApiError | null>(null);
  const [loading, setLoading] = useState(true);
  const [tick, setTick] = useState(0);
  const loaderRef = useRef(loader);
  loaderRef.current = loader;
  // markUnauthorizedRef mirrors loaderRef for the same reason: keep the
  // 401 reaction out of the effect's dep list so a transient identity
  // change in AuthProvider (e.g. React Fast Refresh in dev, or a future
  // wrapping change) doesn't invalidate every active query and trigger
  // a fresh network round-trip on every page.
  const markUnauthorizedRef = useRef(markUnauthorized);
  markUnauthorizedRef.current = markUnauthorized;

  useEffect(() => {
    const ctrl = new AbortController();
    let cancelled = false;
    setLoading(true);
    setError(null);
    loaderRef
      .current(ctrl.signal)
      .then((value) => {
        if (cancelled) return;
        setData(value);
        setLoading(false);
      })
      .catch((err: unknown) => {
        if (cancelled) return;
        if (ctrl.signal.aborted) return;
        if (err instanceof ApiError) {
          if (err.status === 401) markUnauthorizedRef.current();
          setError(err);
        } else {
          setError(new ApiError(0, "network_error", String(err)));
        }
        setLoading(false);
      });
    return () => {
      cancelled = true;
      ctrl.abort();
    };
    // The loader itself is intentionally NOT in the dep list: callers
    // pass an inline arrow and the explicit deps array models the real
    // input set. Also include `tick` so reload() forces a refetch.
    // markUnauthorized is read through markUnauthorizedRef so it does
    // not need to be a dep either.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [...deps, tick]);

  const reload = useCallback(() => setTick((t) => t + 1), []);
  return { data, error, loading, reload };
}

// formatApiError flattens an ApiError or unknown into a string the UI
// can show without exposing stack traces. Code/message preserved when
// possible so operators can grep server logs by code.
export function formatApiError(err: unknown): string {
  if (err instanceof ApiError) {
    if (err.message && err.message !== err.code) {
      return `${err.code}: ${err.message}`;
    }
    return err.code;
  }
  return String(err);
}
