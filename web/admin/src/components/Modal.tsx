import { useEffect } from "react";
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
export function Modal({ title, open, onClose, children, busy }: ModalProps) {
  useEffect(() => {
    if (!open) return;
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape" && !busy) onClose();
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [open, busy, onClose]);

  if (!open) return null;
  return (
    <div
      className="fixed inset-0 z-50 flex items-center justify-center bg-black/40 p-4"
      onMouseDown={(e) => {
        if (e.target === e.currentTarget && !busy) onClose();
      }}
    >
      <div className="w-full max-w-md rounded-lg border border-border bg-surface shadow-xl">
        <div className="border-b border-border px-4 py-3 flex items-center">
          <div className="font-semibold text-sm">{title}</div>
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
