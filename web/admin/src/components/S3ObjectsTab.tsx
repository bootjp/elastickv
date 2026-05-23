import { useEffect, useRef, useState } from "react";
import type { AdminObject, AdminObjectListing } from "../api/client";
import { ApiError, api } from "../api/client";
import { useAuth } from "../auth";
import { Modal } from "./Modal";

// Phase 5 — Objects tab for /s3/buckets/{name}. Driven by the
// Phase-3b HTTP surface in internal/admin/s3_objects_handler.go.
//
// Capabilities (gated on session role):
//   read_only — list + download
//   full      — additionally upload + delete

// Server clamps max_keys to [1, 1000]; we surface 100 as the
// default page size (matches adminListObjectsDefaultMaxKeys).
const PAGE_SIZE = 100;

// Delimiter fixed to "/" — the listObjects pseudo-folder model
// only makes sense with one separator at a time. Operators who
// need flat listings can browse a prefix that has no slashes.
const DELIMITER = "/";

interface S3ObjectsTabProps {
  bucket: string;
}

export function S3ObjectsTab({ bucket }: S3ObjectsTabProps) {
  const { session } = useAuth();
  const writeAllowed = session?.role === "full";

  // prefix is the current folder address — empty string is the
  // bucket root. The breadcrumb derives from this; clicking a
  // segment trims the prefix. Clicking a common-prefix row pushes.
  const [prefix, setPrefix] = useState("");
  const [page, setPage] = useState<AdminObjectListing | null>(null);
  const [loading, setLoading] = useState(false);
  const [loadError, setLoadError] = useState<string | null>(null);
  // cursorStack tracks the continuation tokens that produced the
  // page-N>0 views so a Back-to-previous-page button can return
  // without scanning from the start.
  const [cursorStack, setCursorStack] = useState<string[]>([]);

  // listAbortRef aborts the in-flight list when a new one starts
  // (bucket change, prefix change, Next / Refresh, post-write
  // reload). Without this, an older slower request can overwrite
  // the current view with stale rows for a different prefix —
  // and the follow-up download / delete actions on those rows
  // would mutate the wrong resource (Codex P1, Gemini high on
  // PR #816).
  const listAbortRef = useRef<AbortController | null>(null);
  // Abort the last in-flight controller on unmount so a pending
  // network call doesn't update state after the component is
  // gone.
  useEffect(() => {
    return () => {
      listAbortRef.current?.abort();
    };
  }, []);

  // loadPage returns true on success, false on error or
  // cancellation. Callers (notably onNextPage) inspect the
  // return value to decide whether to advance the cursor stack.
  const loadPage = async (cursor: string | undefined, p: string): Promise<boolean> => {
    listAbortRef.current?.abort();
    const ctrl = new AbortController();
    listAbortRef.current = ctrl;
    setLoading(true);
    setLoadError(null);
    try {
      const result = await api.listObjects(
        bucket,
        {
          prefix: p,
          delimiter: DELIMITER,
          continuation_token: cursor,
          max_keys: PAGE_SIZE,
        },
        ctrl.signal,
      );
      if (listAbortRef.current !== ctrl) return false;
      setPage(result);
      return true;
    } catch (err) {
      if (listAbortRef.current !== ctrl) return false;
      if (err instanceof DOMException && err.name === "AbortError") return false;
      // Clear stale rows on a non-abort failure. Without this, a
      // route change from /s3/a to /s3/b followed by a transient
      // list failure would leave bucket-a's rows visible under
      // the bucket-b context — clicking Download/Delete would
      // then operate on the wrong bucket with an old key (Codex
      // P1 on PR #816 r2). The cursor stack is also cleared so
      // a subsequent Refresh doesn't try to use a stale
      // continuation token against the new context.
      setPage(null);
      setCursorStack([]);
      setLoadError(err instanceof ApiError ? `${err.code}: ${err.message || err.code}` : String(err));
      return false;
    } finally {
      if (listAbortRef.current === ctrl) {
        setLoading(false);
      }
    }
  };

  // Reset prefix + close the detail modal whenever the bucket
  // changes. React Router param changes reuse the same component
  // instance, so without this two pieces of state leak across
  // buckets:
  //
  //   1. prefix — bucket-b would open at bucket-a's subfolder
  //      until the operator manually clicked `(root)` (Codex P2
  //      on PR #816 r1, fixed in r2);
  //   2. detail — if a detail modal was open for an object in
  //      bucket-a when the operator navigated to /s3/bucket-b,
  //      the modal stayed open referencing the stale bucket-a
  //      key. Clicking Delete would then call
  //      api.deleteObject(bucketB, staleBucketAKey) (Claude
  //      review on PR #816 r3 caught this — same cross-context
  //      class as the prefix bug).
  //
  // This effect runs first, schedules the resets; the
  // [bucket, prefix] effect below fires on the *next* render
  // once prefix has flipped, so the user-visible network call
  // uses prefix="". A brief loadPage(undefined, oldPrefix) does
  // fire from this render's commit before the setPrefix
  // re-render settles; listAbortRef cancels it before it can
  // land, so no stale rows leak through.
  useEffect(() => {
    setPrefix("");
    setDetail(null);
    setConfirmDelete(false);
    setDetailError(null);
  }, [bucket]);

  // Initial load + bucket / prefix change refetches from the start.
  useEffect(() => {
    setCursorStack([]);
    void loadPage(undefined, prefix);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [bucket, prefix]);

  const onNextPage = async () => {
    const next = page?.next_continuation_token;
    if (!next) return;
    // Advance cursorStack only after the fetch succeeds. The
    // previous code pushed first; on a transient API failure the
    // UI still showed the old page while internal cursor state
    // had already moved forward — subsequent Refresh / post-
    // mutation reloads then used the wrong token and jumped to
    // a different page (Codex P2 on PR #816).
    const ok = await loadPage(next, prefix);
    if (ok) {
      setCursorStack((s) => [...s, next]);
    }
  };

  const onRefresh = () => {
    const top = cursorStack[cursorStack.length - 1];
    void loadPage(top, prefix);
  };

  const onEnterFolder = (commonPrefix: string) => {
    // commonPrefix already includes the trailing slash (the server
    // emits "photos/" when DELIMITER="/").
    setPrefix(commonPrefix);
  };

  const onCrumbClick = (target: string) => setPrefix(target);

  // Upload state.
  const fileInputRef = useRef<HTMLInputElement>(null);
  const [uploadBusy, setUploadBusy] = useState(false);
  const [uploadError, setUploadError] = useState<string | null>(null);

  const onUploadClick = () => fileInputRef.current?.click();

  const onFilePicked = async (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0];
    if (!file) return;
    // Clear the input so picking the same file twice in a row re-fires.
    e.target.value = "";
    setUploadBusy(true);
    setUploadError(null);
    try {
      // Object key is the prefix + the picked file's base name. The
      // server rejects keys whose URL segment is empty so a file
      // with an empty name (impossible via <input>, but defensive)
      // would 400 before reaching the storage layer.
      const key = prefix + file.name;
      await api.putObject(bucket, key, file, file.type || "application/octet-stream");
      void loadPage(cursorStack[cursorStack.length - 1], prefix);
    } catch (err) {
      setUploadError(err instanceof ApiError ? `${err.code}: ${err.message || err.code}` : String(err));
    } finally {
      setUploadBusy(false);
    }
  };

  // Detail modal state.
  const [detail, setDetail] = useState<AdminObject | null>(null);
  const [detailBusy, setDetailBusy] = useState(false);
  const [detailError, setDetailError] = useState<string | null>(null);
  const [confirmDelete, setConfirmDelete] = useState(false);

  const openDetail = (obj: AdminObject) => {
    setDetail(obj);
    setDetailError(null);
    setConfirmDelete(false);
  };

  const closeDetail = () => {
    if (detailBusy) return;
    setDetail(null);
    setDetailError(null);
    setConfirmDelete(false);
  };

  const onDelete = async () => {
    if (!detail) return;
    setDetailBusy(true);
    setDetailError(null);
    try {
      await api.deleteObject(bucket, detail.key);
      setDetail(null);
      setConfirmDelete(false);
      void loadPage(cursorStack[cursorStack.length - 1], prefix);
    } catch (err) {
      setDetailError(err instanceof ApiError ? `${err.code}: ${err.message || err.code}` : String(err));
    } finally {
      setDetailBusy(false);
    }
  };

  const hasNext = !!page?.next_continuation_token;
  const objectCount = page?.objects?.length ?? 0;
  const folderCount = page?.common_prefixes?.length ?? 0;

  return (
    <section className="card">
      <header className="flex items-center gap-3 mb-3">
        <h2 className="text-sm font-semibold">Objects</h2>
        <Breadcrumb prefix={prefix} onClick={onCrumbClick} />
        <div className="ml-auto flex items-center gap-2">
          <button type="button" className="btn-secondary" onClick={onRefresh} disabled={loading}>
            Refresh
          </button>
          {writeAllowed && (
            <>
              <input
                ref={fileInputRef}
                type="file"
                className="hidden"
                onChange={onFilePicked}
                disabled={uploadBusy}
              />
              <button
                type="button"
                className="btn-primary"
                onClick={onUploadClick}
                disabled={uploadBusy || loading}
              >
                {uploadBusy ? "Uploading…" : "Upload"}
              </button>
            </>
          )}
        </div>
      </header>

      {loadError && <div className="text-sm text-danger mb-3">{loadError}</div>}
      {uploadError && <div className="text-sm text-danger mb-3">{uploadError}</div>}
      {loading && <div className="text-sm text-muted">Loading…</div>}

      {!loading && objectCount === 0 && folderCount === 0 && !loadError && (
        <div className="text-sm text-muted py-4 text-center">
          {prefix ? "This folder is empty." : "This bucket is empty."}
          {writeAllowed && " Use \"Upload\" to add an object."}
        </div>
      )}

      {(objectCount > 0 || folderCount > 0) && (
        <table className="table">
          <thead>
            <tr>
              <th>Name</th>
              <th>Size</th>
              <th>Last modified</th>
              <th>Type</th>
            </tr>
          </thead>
          <tbody>
            {page?.common_prefixes?.map((cp) => (
              <tr key={"cp:" + cp} className="cursor-pointer hover:bg-surface-subtle" onClick={() => onEnterFolder(cp)}>
                <td className="font-mono text-xs">📁 {stripPrefix(cp, prefix)}</td>
                <td className="text-xs text-muted">—</td>
                <td className="text-xs text-muted">—</td>
                <td className="text-xs text-muted">folder</td>
              </tr>
            ))}
            {page?.objects.map((obj) => (
              <tr key={obj.key} className="cursor-pointer hover:bg-surface-subtle" onClick={() => openDetail(obj)}>
                <td className="font-mono text-xs">{stripPrefix(obj.key, prefix)}</td>
                <td className="text-xs">{formatSize(obj.size)}</td>
                <td className="text-xs text-muted">{formatDate(obj.last_modified)}</td>
                <td className="text-xs text-muted">{obj.content_type || "—"}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}

      <footer className="flex items-center pt-3 text-xs text-muted">
        <div>
          {objectCount > 0 ? `${objectCount} objects` : ""}
          {objectCount > 0 && folderCount > 0 ? " · " : ""}
          {folderCount > 0 ? `${folderCount} folders` : ""}
        </div>
        <div className="ml-auto flex gap-2">
          {hasNext ? (
            <button type="button" className="btn-secondary" onClick={onNextPage} disabled={loading}>
              Next page →
            </button>
          ) : (
            <span>{objectCount > 0 || folderCount > 0 ? "End of listing" : ""}</span>
          )}
        </div>
      </footer>

      {detail && (
        <ObjectDetailModal
          bucket={bucket}
          object={detail}
          busy={detailBusy}
          error={detailError}
          writeAllowed={writeAllowed}
          confirmDelete={confirmDelete}
          onClose={closeDetail}
          onConfirmDelete={() => setConfirmDelete(true)}
          onCancelDelete={() => setConfirmDelete(false)}
          onDelete={onDelete}
        />
      )}
    </section>
  );
}

interface BreadcrumbProps {
  prefix: string;
  onClick: (target: string) => void;
}

function Breadcrumb({ prefix, onClick }: BreadcrumbProps) {
  // prefix === "" is the bucket root; segments are slash-separated
  // folders, each retains its trailing slash so the listObjects
  // continuation contract stays intact.
  const segments = prefix === "" ? [] : prefix.replace(/\/$/u, "").split("/");
  return (
    <div className="text-xs text-muted flex items-center gap-1 font-mono">
      <button type="button" className="hover:text-ink underline" onClick={() => onClick("")}>
        (root)
      </button>
      {segments.map((seg, idx) => {
        const target = segments.slice(0, idx + 1).join("/") + "/";
        // key=target rather than idx so React reconciles by the
        // segment path itself — re-ordering or trimming the
        // breadcrumb (e.g., the operator drops to a different
        // depth) doesn't churn unrelated DOM nodes (Claude review
        // on PR #816 r3 — nit).
        return (
          <span key={target} className="flex items-center gap-1">
            <span>/</span>
            <button
              type="button"
              className="hover:text-ink underline"
              onClick={() => onClick(target)}
              disabled={idx === segments.length - 1}
            >
              {seg}
            </button>
          </span>
        );
      })}
    </div>
  );
}

interface ObjectDetailModalProps {
  bucket: string;
  object: AdminObject;
  busy: boolean;
  error: string | null;
  writeAllowed: boolean;
  confirmDelete: boolean;
  onClose: () => void;
  onConfirmDelete: () => void;
  onCancelDelete: () => void;
  onDelete: () => void;
}

function ObjectDetailModal({
  bucket,
  object,
  busy,
  error,
  writeAllowed,
  confirmDelete,
  onClose,
  onConfirmDelete,
  onCancelDelete,
  onDelete,
}: ObjectDetailModalProps) {
  const downloadHref = api.downloadObjectURL(bucket, object.key);
  return (
    <Modal title={object.key} open onClose={onClose} busy={busy}>
      <div className="space-y-3 text-sm">
        <dl className="grid grid-cols-2 gap-x-6 gap-y-1 text-xs">
          <dt className="text-muted">Size</dt>
          <dd className="font-mono">{formatSize(object.size)}</dd>
          <dt className="text-muted">Content type</dt>
          <dd className="font-mono">{object.content_type || "—"}</dd>
          <dt className="text-muted">ETag</dt>
          <dd className="font-mono break-all">{object.etag || "—"}</dd>
          <dt className="text-muted">Last modified</dt>
          <dd className="font-mono">{formatDate(object.last_modified)}</dd>
          <dt className="text-muted">Storage class</dt>
          <dd className="font-mono">{object.storage_class || "—"}</dd>
        </dl>
        {error && <div className="text-danger">{error}</div>}
        <div className="flex justify-end gap-2 pt-2">
          {confirmDelete ? (
            <>
              <span className="mr-auto text-xs text-danger">Delete this object?</span>
              <button type="button" className="btn-secondary" onClick={onCancelDelete} disabled={busy}>
                Cancel
              </button>
              <button type="button" className="btn-danger" onClick={onDelete} disabled={busy}>
                {busy ? "Deleting…" : "Delete"}
              </button>
            </>
          ) : (
            <>
              <button type="button" className="btn-secondary" onClick={onClose} disabled={busy}>
                Close
              </button>
              <a
                className="btn-secondary"
                href={downloadHref}
                // download attribute hints the browser to use the
                // server's Content-Disposition filename; passing an
                // empty value defers to the server.
                download
              >
                Download
              </a>
              {writeAllowed && (
                <button type="button" className="btn-danger" onClick={onConfirmDelete} disabled={busy}>
                  Delete
                </button>
              )}
            </>
          )}
        </div>
      </div>
    </Modal>
  );
}

// ---------- helpers ----------

// stripPrefix returns the trailing portion of `key` past `prefix`.
// Used to render `foo.png` in the table cell when the operator is
// browsing `bucket/photos/foo.png` under prefix `photos/`.
function stripPrefix(key: string, prefix: string): string {
  if (prefix && key.startsWith(prefix)) return key.slice(prefix.length);
  return key;
}

function formatSize(n: number): string {
  if (n < 1024) return `${n} B`;
  if (n < 1024 * 1024) return `${(n / 1024).toFixed(1)} KiB`;
  if (n < 1024 * 1024 * 1024) return `${(n / 1024 / 1024).toFixed(1)} MiB`;
  return `${(n / 1024 / 1024 / 1024).toFixed(2)} GiB`;
}

function formatDate(iso: string): string {
  if (!iso) return "—";
  try {
    return new Date(iso).toLocaleString();
  } catch {
    return iso;
  }
}
