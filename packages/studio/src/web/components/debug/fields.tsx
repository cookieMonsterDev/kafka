import type { ReactNode } from 'react';
export function Field({ label, children }: { readonly label: string; readonly children: ReactNode }) {
  return (
    <label className="flex min-w-0 flex-col gap-2 text-sm">
      <span className="font-medium">{label}</span>
      {children}
    </label>
  );
}
export function Output({ value }: { readonly value: unknown }) {
  return (
    <pre
      tabIndex={0}
      className="max-h-96 overflow-auto rounded-lg border border-border bg-muted/30 p-3 text-xs focus-visible:ring-2 focus-visible:ring-ring"
    >
      {JSON.stringify(value, null, 2)}
    </pre>
  );
}

export interface OperationProps {
  readonly running: boolean;
  readonly canWrite: boolean;
  readonly operate: (action: (signal: AbortSignal) => Promise<void> | void) => Promise<void>;
  readonly setOutput: (value: unknown) => void;
}
