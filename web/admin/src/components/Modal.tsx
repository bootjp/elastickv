import { useEffect, useId, useRef } from "react";
import type { ReactNode } from "react";

interface ModalProps {
  title: string;
  open: boolean;
  onClose: () => void;
  children: ReactNode;
  // Keeps the close affordances (Esc, backdrop) wired off when a
  // background save is in flight so the user cannot accidentally
  // dismiss a half-applied operation.
  busy?: boolean;
}

// Lightweight modal: no portal, no animation library. shadcn/ui's
// Dialog primitive would pull in @radix-ui/react-dialog (~10KB gzip)
// and we only need a single confirmation/edit surface per page.
//
// Accessibility (per the WAI-ARIA Authoring Practices for dialogs):
//   - role="dialog" + aria-modal="true" so AT announce the dialog
//     and treat the rest of the page as inert.
//   - aria-labelledby on the title <div> so the dialog is named.
//   - Focus is moved into the dialog on open and restored to the
//     previously-focused element on close.
//   - Tab and Shift+Tab are wrapped to keep focus inside the dialog
//     until it closes, so keyboard users cannot accidentally tab to
//     the page underneath.
export function Modal({ title, open, onClose, children, busy }: ModalProps) {
  const dialogRef = useRef<HTMLDivElement>(null);
  const previouslyFocusedRef = useRef<HTMLElement | null>(null);
  const titleId = useId();

  useEffect(() => {
    if (!open) return;

    previouslyFocusedRef.current = (document.activeElement as HTMLElement | null) ?? null;
    // Focus the first focusable element so keyboard users land
    // inside the dialog instead of the page underneath. Run on the
    // next tick so the dialog DOM exists and any autofocus has had
    // a chance to settle.
    queueMicrotask(() => focusFirstFocusable(dialogRef.current));

    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape" && !busy) {
        onClose();
        return;
      }
      if (e.key !== "Tab") return;
      const root = dialogRef.current;
      if (!root) return;
      const focusables = focusableElements(root);
      if (focusables.length === 0) {
        // Empty dialog: keep Tab from leaving via the page.
        e.preventDefault();
        return;
      }
      const first = focusables[0];
      const last = focusables[focusables.length - 1];
      const active = document.activeElement;
      if (e.shiftKey && active === first) {
        e.preventDefault();
        last.focus();
      } else if (!e.shiftKey && active === last) {
        e.preventDefault();
        first.focus();
      }
    };

    window.addEventListener("keydown", onKey);
    return () => {
      window.removeEventListener("keydown", onKey);
      // Restore focus to whoever opened the dialog. Guard against
      // the trigger being unmounted (e.g. the dialog deleted the
      // row whose button opened it) by checking isConnected.
      const restore = previouslyFocusedRef.current;
      previouslyFocusedRef.current = null;
      if (restore && restore.isConnected) {
        restore.focus();
      }
    };
  }, [open, busy, onClose]);

  if (!open) return null;
  return (
    <div
      className="fixed inset-0 z-50 flex items-center justify-center bg-black/40 p-4"
      onMouseDown={(e) => {
        if (e.target === e.currentTarget && !busy) onClose();
      }}
    >
      <div
        ref={dialogRef}
        role="dialog"
        aria-modal="true"
        aria-labelledby={titleId}
        className="w-full max-w-md rounded-lg border border-border bg-surface shadow-xl"
      >
        <div className="border-b border-border px-4 py-3 flex items-center">
          <div id={titleId} className="font-semibold text-sm">{title}</div>
          <button
            type="button"
            onClick={onClose}
            disabled={busy}
            className="ml-auto text-muted hover:text-ink disabled:opacity-50"
            aria-label="Close"
          >
            ×
          </button>
        </div>
        <div className="px-4 py-4">{children}</div>
      </div>
    </div>
  );
}

// focusableSelector targets the elements an end user can tab to.
// Excludes [tabindex="-1"] (programmatic-only focus targets) and
// disabled / hidden inputs. Kept narrow on purpose: the modal only
// hosts buttons / form fields / links, not embedded media or
// content-editable surfaces.
const focusableSelector = [
  "a[href]",
  "button:not([disabled])",
  "input:not([disabled]):not([type='hidden'])",
  "select:not([disabled])",
  "textarea:not([disabled])",
  '[tabindex]:not([tabindex="-1"])',
].join(",");

function focusableElements(root: HTMLElement): HTMLElement[] {
  return Array.from(root.querySelectorAll<HTMLElement>(focusableSelector));
}

function focusFirstFocusable(root: HTMLElement | null): void {
  if (!root) return;
  const focusables = focusableElements(root);
  if (focusables.length > 0) {
    focusables[0].focus();
    return;
  }
  // Fallback: focus the dialog container itself so screen readers
  // still announce it. Add tabindex=-1 dynamically so the browser
  // accepts the focus call without making the dialog tab-stop bait.
  root.setAttribute("tabindex", "-1");
  root.focus();
}
