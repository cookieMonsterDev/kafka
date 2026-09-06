import type { ReactNode } from 'react';
import { Skeleton } from '../ui/skeleton';

/** The scrollable, labelled region every settings table sits in — matches the pattern group-detail.tsx uses for its own tables. */
export function TableShell({ label, children }: { readonly label: string; readonly children: ReactNode }) {
  return (
    <div tabIndex={0} role="region" aria-label={label} className="overflow-x-auto rounded-xl border border-border">
      <table className="w-full text-sm">{children}</table>
    </div>
  );
}

export function LoadingRows() {
  return (
    <div
      className="flex flex-col gap-1 overflow-hidden rounded-xl border border-border p-2"
      role="status"
      aria-busy="true"
    >
      <span className="sr-only">Loading…</span>
      {[0, 1, 2, 3].map((row) => (
        <Skeleton key={row} className="h-10 w-full" />
      ))}
    </div>
  );
}
