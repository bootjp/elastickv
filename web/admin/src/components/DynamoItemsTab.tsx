import { useEffect, useMemo, useRef, useState } from "react";
import type {
  AdminAttributeValue,
  AdminItem,
  AdminScanItemsResult,
} from "../api/client";
import { ApiError, api, encodeAdminItemKey } from "../api/client";
import { useAuth } from "../auth";
import { Modal } from "./Modal";

// Phase 4 — Items tab for /dynamo/tables/{name}. Driven by the
// Phase-3a HTTP surface in internal/admin/dynamo_items_handler.go.
//
// Capabilities (gated on session role):
//   read_only — scan + view item JSON
//   full      — additionally add / edit / delete

// Scan page-size knobs. The server clamps to [1, 100]; we let the
// operator pick between the design default (25) and the server cap.
const PAGE_SIZES = [25, 100] as const;
type PageSize = (typeof PAGE_SIZES)[number];

interface DynamoItemsTabProps {
  table: string;
  // partitionKey / sortKey describe the table's primary-key shape so
  // the modal can pre-fill the URL-key form from the body or extract
  // the right attributes when uploading. Empty sortKey indicates a
  // hash-only table.
  partitionKey: string;
  sortKey?: string;
}

// EditorMode tracks the modal's intent: "view" is read-only, "edit"
// is overwriting an existing item, "add" is creating a new one. The
// distinction matters for two reasons: (1) the modal title and
// button labels change, and (2) "edit" keeps the URL key derived
// from the original load while "add" derives it fresh from the
// (operator-typed) body before each save.
type EditorMode = "view" | "edit" | "add";

export function DynamoItemsTab({ table, partitionKey, sortKey }: DynamoItemsTabProps) {
  const { session } = useAuth();
  const writeAllowed = session?.role === "full";

  const [pageSize, setPageSize] = useState<PageSize>(25);
  const [page, setPage] = useState<AdminScanItemsResult | null>(null);
  const [loading, setLoading] = useState(false);
  const [loadError, setLoadError] = useState<string | null>(null);
  // currentCursor is the continuation token that loaded the
  // currently-rendered page (undefined = page 0, no cursor sent).
  // Refresh and post-write reloads re-issue the scan against this
  // value; Next advances it to the page's last_evaluated_key after
  // the fetch succeeds. There is no Back button today — once Next
  // overwrites the cursor the prior cursor is gone — so a simple
  // single-value state is enough.
  const [currentCursor, setCurrentCursor] = useState<string | undefined>(undefined);

  // scanAbortRef aborts the in-flight scan when a new one starts
  // (table change, page-size change, Next / Refresh, post-write
  // reload). Without this, an older slower request can overwrite
  // the current view with stale items after the user has already
  // moved on — which is a confused-deputy class for the
  // follow-up edit / delete actions that key off the rendered
  // rows (Codex P1, Gemini high on PR #815).
  const scanAbortRef = useRef<AbortController | null>(null);
  // Ensure the in-flight controller is aborted on unmount. The
  // effect's cleanup function fires on every dep change AND on
  // unmount, so the unmount path is covered by the inline
  // controller it owns; the ref-based aborts cover the
  // user-driven reload cases.
  useEffect(() => {
    return () => {
      scanAbortRef.current?.abort();
    };
  }, []);

  // loadPage returns true on success, false on error / cancellation.
  // Callers (onNextPage) inspect the boolean to decide whether to
  // advance the cursor — matches the S3 ObjectsTab pattern from
  // PR #816 r1 so the two tabs share one cursor-advance contract.
  const loadPage = async (cursor: string | undefined): Promise<boolean> => {
    scanAbortRef.current?.abort();
    const ctrl = new AbortController();
    scanAbortRef.current = ctrl;
    setLoading(true);
    setLoadError(null);
    try {
      const result = await api.scanItems(table, { limit: pageSize, next_cursor: cursor }, ctrl.signal);
      // A second loadPage() may have replaced scanAbortRef before
      // we get here; ignore the resolved value of the obsolete
      // request to keep `page` aligned with the latest call.
      if (scanAbortRef.current !== ctrl) return false;
      setPage(result);
      return true;
    } catch (err) {
      if (scanAbortRef.current !== ctrl) return false;
      // AbortError is the cancellation signal — not a user-visible
      // failure; suppress it so the operator's manual Refresh
      // immediately after a Next page click doesn't show "aborted".
      if (err instanceof DOMException && err.name === "AbortError") return false;
      setLoadError(err instanceof ApiError ? `${err.code}: ${err.message || err.code}` : String(err));
      return false;
    } finally {
      if (scanAbortRef.current === ctrl) {
        setLoading(false);
      }
    }
  };

  // Initial load + page-size change refetches from the start AND
  // resets the modal. useApiQuery in the parent DynamoDetail page
  // does NOT clear data on dep change, so this component stays
  // mounted across /dynamo/tables/A → /dynamo/tables/B navigation.
  // Without the explicit modal reset, an open detail modal would
  // retain modalKey from table A while the table prop is now B —
  // onSave would then call api.putItem(B, keyFromA, body), silently
  // overwriting a colliding key in table B. Same cross-context
  // class as the S3 ObjectsTab detail-modal-on-bucket-change fix
  // (PR #816 r4). Inlined rather than calling closeModalForce
  // (declared later) to keep the effect self-contained.
  useEffect(() => {
    setCurrentCursor(undefined);
    setModalMode(null);
    setModalItem(null);
    setModalKey(null);
    setModalError(null);
    void loadPage(undefined);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [table, pageSize]);

  const onNextPage = async () => {
    const next = page?.last_evaluated_key;
    if (!next) return;
    // Advance currentCursor only after the fetch succeeds.
    // Matches the S3 ObjectsTab pattern: a transient failure must
    // not leave the UI on a cursor that no longer corresponds to
    // the rendered page (Refresh would then re-fetch the failed
    // page rather than the still-shown one). The comment on
    // currentCursor's useState above promises this contract.
    const cursor = encodeAdminItemKey(next);
    const ok = await loadPage(cursor);
    if (ok) {
      setCurrentCursor(cursor);
    }
  };

  const onRefresh = () => {
    void loadPage(currentCursor);
  };

  // Modal state.
  const [modalMode, setModalMode] = useState<EditorMode | null>(null);
  const [modalItem, setModalItem] = useState<AdminItem | null>(null);
  const [modalKey, setModalKey] = useState<Record<string, AdminAttributeValue> | null>(null);
  const [modalBusy, setModalBusy] = useState(false);
  const [modalError, setModalError] = useState<string | null>(null);

  const closeModal = () => {
    if (modalBusy) return;
    setModalMode(null);
    setModalItem(null);
    setModalKey(null);
    setModalError(null);
  };

  const openView = (item: AdminItem) => {
    setModalItem(item);
    setModalKey(extractPrimaryKey(item, partitionKey, sortKey));
    setModalMode("view");
    setModalError(null);
  };

  const openEdit = () => setModalMode("edit");

  const openAdd = () => {
    setModalItem({ attributes: {} });
    setModalKey(null);
    setModalMode("add");
    setModalError(null);
  };

  const onSave = async (next: AdminItem) => {
    setModalBusy(true);
    setModalError(null);
    try {
      // Edit mode MUST use the originally-opened modalKey rather
      // than re-deriving from the edited body — changing the
      // primary key in the body otherwise turns an "edit" into a
      // write to a different URL, leaving the original item
      // untouched and silently creating / overwriting a sibling
      // (Codex P1 on PR #815). Add mode does derive from body
      // (no original key exists). The server's body / URL-key
      // reconciliation enforces that the body's key attributes
      // match the URL key, so a primary-key edit attempt on the
      // edit path surfaces as 400 invalid_request — operators
      // who actually want to rename an item must delete + add.
      let key: Record<string, AdminAttributeValue> | null;
      if (modalMode === "edit" && modalKey) {
        key = modalKey;
      } else {
        key = extractPrimaryKey(next, partitionKey, sortKey);
        if (!key) {
          throw new Error(
            `body must contain the partition key "${partitionKey}"` +
              (sortKey ? ` and the sort key "${sortKey}"` : ""),
          );
        }
      }
      await api.putItem(table, key, next);
      closeModalForce();
      void loadPage(currentCursor);
    } catch (err) {
      setModalError(err instanceof ApiError ? `${err.code}: ${err.message || err.code}` : String(err));
    } finally {
      setModalBusy(false);
    }
  };

  const onDelete = async () => {
    if (!modalKey) return;
    setModalBusy(true);
    setModalError(null);
    try {
      await api.deleteItem(table, modalKey);
      closeModalForce();
      void loadPage(currentCursor);
    } catch (err) {
      setModalError(err instanceof ApiError ? `${err.code}: ${err.message || err.code}` : String(err));
    } finally {
      setModalBusy(false);
    }
  };

  const closeModalForce = () => {
    setModalMode(null);
    setModalItem(null);
    setModalKey(null);
    setModalError(null);
  };

  const hasNext = !!page?.last_evaluated_key;
  const itemCount = page?.items?.length ?? 0;

  return (
    <section className="card">
      <header className="flex items-center gap-3 mb-3">
        <h2 className="text-sm font-semibold">Items</h2>
        <div className="ml-auto flex items-center gap-2">
          <label className="text-xs text-muted">
            Page size
            <select
              className="ml-1 border border-border rounded px-1 py-0.5"
              value={pageSize}
              onChange={(e) => setPageSize(Number(e.target.value) as PageSize)}
              disabled={loading}
            >
              {PAGE_SIZES.map((n) => (
                <option key={n} value={n}>
                  {n}
                </option>
              ))}
            </select>
          </label>
          <button type="button" className="btn-secondary" onClick={onRefresh} disabled={loading}>
            Refresh
          </button>
          {writeAllowed && (
            <button type="button" className="btn-primary" onClick={openAdd} disabled={loading}>
              Add item
            </button>
          )}
        </div>
      </header>

      {loadError && <div className="text-sm text-danger mb-3">{loadError}</div>}
      {loading && <div className="text-sm text-muted">Loading…</div>}

      {!loading && itemCount === 0 && !loadError && (
        <div className="text-sm text-muted py-4 text-center">
          No items on this page. {writeAllowed && "Use \"Add item\" to seed one."}
        </div>
      )}

      {itemCount > 0 && (
        <table className="table">
          <thead>
            <tr>
              <th>Primary key</th>
              <th>Preview</th>
              <th />
            </tr>
          </thead>
          <tbody>
            {page!.items.map((item) => (
              // React key is the encoded primary key — guaranteed
              // unique within a DynamoDB scan page. describePrimaryKey
              // (which we used before) renders binary values as
              // "<binary ~NB>" and collides when two distinct binary
              // keys share the same byte length, so two different
              // rows would render with the same React key and
              // reconciliation could bind onOpen to the wrong item
              // (Claude review on PR #815 r2). encodeAdminItemKey
              // returns the full base64url-encoded JSON of the key
              // map, which encodes binary bytes verbatim and is
              // collision-free.
              <ItemRow
                key={encodeAdminItemKey(extractPrimaryKey(item, partitionKey, sortKey) ?? {})}
                item={item}
                partitionKey={partitionKey}
                sortKey={sortKey}
                onOpen={() => openView(item)}
              />
            ))}
          </tbody>
        </table>
      )}

      <footer className="flex items-center pt-3 text-xs text-muted">
        <div>{itemCount > 0 ? `${itemCount} items on this page` : ""}</div>
        <div className="ml-auto flex gap-2">
          {hasNext ? (
            <button type="button" className="btn-secondary" onClick={onNextPage} disabled={loading}>
              Next page →
            </button>
          ) : (
            <span>{itemCount > 0 ? "End of scan" : ""}</span>
          )}
        </div>
      </footer>

      {modalMode && modalItem && (
        <ItemEditorModal
          mode={modalMode}
          item={modalItem}
          busy={modalBusy}
          error={modalError}
          writeAllowed={writeAllowed}
          partitionKey={partitionKey}
          sortKey={sortKey}
          onClose={closeModal}
          onEdit={openEdit}
          onSave={onSave}
          onDelete={onDelete}
        />
      )}
    </section>
  );
}

interface ItemRowProps {
  item: AdminItem;
  partitionKey: string;
  sortKey?: string;
  onOpen: () => void;
}

function ItemRow({ item, partitionKey, sortKey, onOpen }: ItemRowProps) {
  const keyPreview = useMemo(
    () => describePrimaryKey(item, partitionKey, sortKey),
    [item, partitionKey, sortKey],
  );
  const valuePreview = useMemo(() => describeItemPreview(item, partitionKey, sortKey), [
    item,
    partitionKey,
    sortKey,
  ]);
  return (
    <tr>
      <td className="font-mono text-xs">{keyPreview}</td>
      <td className="font-mono text-xs text-muted">{valuePreview}</td>
      <td className="text-right">
        <button type="button" className="btn-secondary text-xs" onClick={onOpen}>
          Open
        </button>
      </td>
    </tr>
  );
}

interface ItemEditorModalProps {
  mode: EditorMode;
  item: AdminItem;
  busy: boolean;
  error: string | null;
  writeAllowed: boolean;
  partitionKey: string;
  sortKey?: string;
  onClose: () => void;
  onEdit: () => void;
  onSave: (next: AdminItem) => void;
  onDelete: () => void;
}

function ItemEditorModal({
  mode,
  item,
  busy,
  error,
  writeAllowed,
  partitionKey,
  sortKey,
  onClose,
  onEdit,
  onSave,
  onDelete,
}: ItemEditorModalProps) {
  const initial = useMemo(() => JSON.stringify(item, null, 2), [item]);
  const [draft, setDraft] = useState(initial);
  const [confirmDelete, setConfirmDelete] = useState(false);
  const [parseError, setParseError] = useState<string | null>(null);

  // Reset the draft when the item / mode flips. Using a key on the
  // modal would also work but this is one effect instead of a
  // remount + state-redaclare cycle.
  useEffect(() => {
    setDraft(initial);
    setParseError(null);
    setConfirmDelete(false);
  }, [initial, mode]);

  const title =
    mode === "add" ? "Add item" : mode === "edit" ? "Edit item" : "Item";

  const editable = mode === "add" || mode === "edit";

  const onSubmit = () => {
    let parsed: AdminItem;
    try {
      const obj = JSON.parse(draft);
      if (!obj || typeof obj !== "object" || !obj.attributes || typeof obj.attributes !== "object") {
        throw new Error('body must have an "attributes" object');
      }
      parsed = obj as AdminItem;
    } catch (err) {
      setParseError(err instanceof Error ? err.message : String(err));
      return;
    }
    setParseError(null);
    onSave(parsed);
  };

  return (
    <Modal title={title} open onClose={onClose} busy={busy}>
      <div className="space-y-3 text-sm">
        <div className="text-xs text-muted">
          Primary key: <code className="font-mono">{partitionKey}</code>
          {sortKey && (
            <>
              {" + "}
              <code className="font-mono">{sortKey}</code>
            </>
          )}
        </div>
        {editable ? (
          <textarea
            className="w-full h-72 font-mono text-xs border border-border rounded p-2"
            value={draft}
            onChange={(e) => setDraft(e.target.value)}
            disabled={busy}
            spellCheck={false}
          />
        ) : (
          <pre className="w-full h-72 font-mono text-xs border border-border rounded p-2 overflow-auto bg-surface-subtle">
            {initial}
          </pre>
        )}
        {parseError && <div className="text-danger">{parseError}</div>}
        {error && <div className="text-danger">{error}</div>}
        <div className="flex justify-end gap-2 pt-2">
          {confirmDelete ? (
            <>
              <span className="mr-auto text-xs text-danger">Delete this item?</span>
              <button type="button" className="btn-secondary" onClick={() => setConfirmDelete(false)} disabled={busy}>
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
              {mode === "view" && writeAllowed && (
                <>
                  <button type="button" className="btn-danger" onClick={() => setConfirmDelete(true)} disabled={busy}>
                    Delete
                  </button>
                  <button type="button" className="btn-primary" onClick={onEdit} disabled={busy}>
                    Edit
                  </button>
                </>
              )}
              {editable && (
                <button type="button" className="btn-primary" onClick={onSubmit} disabled={busy}>
                  {busy ? "Saving…" : "Save"}
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

// extractPrimaryKey pulls the table's primary-key attributes out of
// an item body, returning null when any required attribute is
// missing. Hash-only tables only require partitionKey; hash+range
// require both. The handler will reject mismatches at the body /
// URL-key reconciliation step, so this only protects against
// obvious operator typos before we even hit the network.
function extractPrimaryKey(
  item: AdminItem,
  partitionKey: string,
  sortKey?: string,
): Record<string, AdminAttributeValue> | null {
  const attrs = item.attributes ?? {};
  const pk = attrs[partitionKey];
  if (!pk) return null;
  const out: Record<string, AdminAttributeValue> = { [partitionKey]: pk };
  if (sortKey) {
    const sk = attrs[sortKey];
    if (!sk) return null;
    out[sortKey] = sk;
  }
  return out;
}

// describePrimaryKey renders the partition key (and sort key, if
// any) as a compact `pk=value` or `pk=value | sk=value` string for
// the table row. We render scalar types as the typed value; complex
// types fall back to JSON.
function describePrimaryKey(
  item: AdminItem,
  partitionKey: string,
  sortKey?: string,
): string {
  const parts = [`${partitionKey}=${formatAttributeShort(item.attributes?.[partitionKey])}`];
  if (sortKey) {
    parts.push(`${sortKey}=${formatAttributeShort(item.attributes?.[sortKey])}`);
  }
  return parts.join(" | ");
}

// describeItemPreview is a teaser for the row's value column: count
// of non-key attributes, plus the first two key=value pairs to
// help disambiguate items. Long values truncate at 32 chars.
function describeItemPreview(
  item: AdminItem,
  partitionKey: string,
  sortKey?: string,
): string {
  const attrs = item.attributes ?? {};
  const skip = new Set([partitionKey]);
  if (sortKey) skip.add(sortKey);
  const entries = Object.entries(attrs).filter(([k]) => !skip.has(k));
  if (entries.length === 0) return "(no other attributes)";
  const head = entries
    .slice(0, 2)
    .map(([k, v]) => {
      const val = formatAttributeShort(v);
      return `${k}=${val.length > 32 ? val.slice(0, 32) + "…" : val}`;
    })
    .join(", ");
  const more = entries.length > 2 ? ` (+${entries.length - 2} more)` : "";
  return head + more;
}

function formatAttributeShort(v: AdminAttributeValue | undefined): string {
  if (!v) return "—";
  if (v.S !== undefined) return v.S;
  if (v.N !== undefined) return v.N;
  if (v.BOOL !== undefined) return String(v.BOOL);
  if (v.NULL !== undefined) return "null";
  // v.B is the base64 string, not the raw bytes. Operators read
  // the size as bytes, so approximate it: every 4 base64 chars
  // encode 3 bytes; padding-aware floor is close enough for a
  // teaser preview (off by at most 2 from the true byte count).
  if (v.B !== undefined) return `<binary ~${approxB64Bytes(v.B)}B>`;
  if (v.SS) return `[${v.SS.join(",")}]`;
  if (v.NS) return `[${v.NS.join(",")}]`;
  if (v.BS) return `<${v.BS.length} bin>`;
  if (v.L) return `[${v.L.length} list]`;
  if (v.M) return `{${Object.keys(v.M).length} map}`;
  return "?";
}

function approxB64Bytes(b64: string): number {
  const trimmed = b64.replace(/=+$/u, "");
  return Math.floor((trimmed.length * 3) / 4);
}
