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

  const loadPage = async (cursor: string | undefined, p: string) => {
    setLoading(true);
    setLoadError(null);
    try {
      const result = await api.listObjects(bucket, {
        prefix: p,
        delimiter: DELIMITER,
        continuation_token: cursor,
        max_keys: PAGE_SIZE,
      });
      setPage(result);
    } catch (err) {
      setLoadError(err instanceof ApiError ? `${err.code}: ${err.message || err.code}` : String(err));
    } finally {
      setLoading(false);
    }
  };

  // Initial load + bucket / prefix change refetches from the start.
  useEffect(() => {
    setCursorStack([]);
    void loadPage(undefined, prefix);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [bucket, prefix]);

  const onNextPage = () => {
    const next = page?.next_continuation_token;
    if (!next) return;
    setCursorStack((s) => [...s, next]);
    void loadPage(next, prefix);
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
        return (
          <span key={idx} className="flex items-center gap-1">
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
