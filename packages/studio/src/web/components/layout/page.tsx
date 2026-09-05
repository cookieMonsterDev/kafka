import type { ReactNode } from 'react';

import { cn } from '../../lib/utils';

interface PageLayoutProps {
  /** Control row directly under the top bar — filters, view switchers, page-level actions. */
  readonly toolbar?: ReactNode;
  /**
   * Page-local detail rail. Beside the content on `xl`, stacked underneath below it — the rail is
   * never the only place a piece of information lives.
   */
  readonly rail?: ReactNode;
  readonly railLabel?: string;
  readonly children: ReactNode;
}

/**
 * The work canvas: optional toolbar, scrolling content, optional detail rail. Routes compose
 * this instead of re-deriving the grid, so the three-column shape stays consistent.
 */
export function PageLayout({ toolbar, rail, railLabel, children }: PageLayoutProps) {
  return (
    <div className="flex min-h-0 min-w-0 flex-1 flex-col">
      {toolbar !== undefined && (
        // `min-h`, not `h` — a toolbar dense enough to wrap must grow the row, not overflow it.
        <div className="flex min-h-14 shrink-0 flex-wrap items-center justify-between gap-3 px-4 py-2 lg:px-6">
          {toolbar}
        </div>
      )}
      <div className="flex min-h-0 min-w-0 flex-1 xl:overflow-hidden">
        <div
          className={cn(
            'min-w-0 flex-1 overflow-y-auto p-4 lg:p-6',
            // The toolbar row already leaves breathing room under its controls; a full top pad
            // on top of that reads as a hole between the two.
            toolbar !== undefined && 'pt-0 lg:pt-0',
            rail !== undefined && 'xl:pr-6',
          )}
        >
          <div className="mx-auto w-full max-w-[110rem]">{children}</div>
          {rail !== undefined && (
            <div className="mt-6 flex flex-col gap-4 xl:hidden">
              <h2 className="sr-only">{railLabel ?? 'Details'}</h2>
              {rail}
            </div>
          )}
        </div>
        {rail !== undefined && (
          <aside
            aria-label={railLabel ?? 'Details'}
            className="hidden w-80 shrink-0 flex-col gap-4 overflow-y-auto border-l border-border bg-sidebar p-4 xl:flex"
          >
            {rail}
          </aside>
        )}
      </div>
    </div>
  );
}
