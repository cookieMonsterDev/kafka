import type { ReactNode } from 'react';

import { ErrorState } from './error-state';

interface QueryLike<TData> {
  readonly data: TData | undefined;
  readonly isPending: boolean;
  readonly isError: boolean;
  readonly error: unknown;
  readonly refetch: () => unknown;
}

interface QueryBoundaryProps<TData> {
  readonly query: QueryLike<TData>;
  /** Layout-preserving placeholder shaped like the loaded content. */
  readonly skeleton: ReactNode;
  /** What went wrong, in the caller's own words — the server's message is appended below it. */
  readonly errorTitle: string;
  readonly children: (data: TData) => ReactNode;
}

/**
 * Pending / error / loaded, decided in one place. Routes used to hand-roll this three ways with
 * a bare `<p>` per branch and no way to recover from a failure.
 */
export function QueryBoundary<TData>({ query, skeleton, errorTitle, children }: QueryBoundaryProps<TData>) {
  if (query.isPending) {
    return (
      <div role="status" aria-busy="true">
        <span className="sr-only">Loading…</span>
        {skeleton}
      </div>
    );
  }

  if (query.isError) {
    return <ErrorState title={errorTitle} error={query.error} onRetry={() => void query.refetch()} />;
  }

  return query.data === undefined ? null : <>{children(query.data)}</>;
}
