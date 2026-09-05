import * as React from 'react';
import { TriangleAlert } from 'lucide-react';

import { cn } from '../../lib/utils';
import { Button } from './button';

interface ErrorStateProps extends React.ComponentProps<'div'> {
  readonly title: string;
  /** The underlying failure. Server messages come through verbatim — they are the useful part. */
  readonly error?: unknown;
  readonly onRetry?: () => void;
  readonly retryLabel?: string;
}

/** Reads a displayable message off whatever a query or mutation rejected with. */
function errorMessage(error: unknown): string | undefined {
  if (error instanceof Error && error.message !== '') return error.message;
  if (typeof error === 'string' && error !== '') return error;
  return undefined;
}

/**
 * Every failed read renders this: what broke, why, and a way out. An error with no recovery
 * action is a dead end, so `onRetry` should be wired wherever the caller can actually refetch.
 */
function ErrorState({ className, title, error, onRetry, retryLabel = 'Try again', ...props }: ErrorStateProps) {
  const message = errorMessage(error);

  return (
    <div
      data-slot="error-state"
      role="alert"
      className={cn('flex flex-col items-center gap-2 px-4 py-12 text-center', className)}
      {...props}
    >
      <span className="mb-1 flex size-10 items-center justify-center rounded-xl border border-destructive/30 bg-destructive/15">
        <TriangleAlert className="size-5 text-destructive" aria-hidden="true" />
      </span>
      <p className="text-sm font-medium">{title}</p>
      {message !== undefined && <p className="max-w-sm text-sm wrap-break-word text-muted-foreground">{message}</p>}
      {onRetry !== undefined && (
        <Button type="button" variant="outline" size="sm" className="mt-2" onClick={onRetry}>
          {retryLabel}
        </Button>
      )}
    </div>
  );
}

export { ErrorState, errorMessage };
