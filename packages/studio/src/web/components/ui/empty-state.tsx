import * as React from 'react';
import type { LucideIcon } from 'lucide-react';

import { cn } from '../../lib/utils';

interface EmptyStateProps extends React.ComponentProps<'div'> {
  readonly icon?: LucideIcon;
  readonly title: string;
  readonly description?: string;
  /** A single next step — creating the missing thing, or clearing the filter that hid it. */
  readonly action?: React.ReactNode;
}

function EmptyState({ className, icon: Icon, title, description, action, ...props }: EmptyStateProps) {
  return (
    <div
      data-slot="empty-state"
      className={cn('flex flex-col items-center gap-2 px-4 py-12 text-center', className)}
      {...props}
    >
      {Icon !== undefined && (
        <span className="mb-1 flex size-10 items-center justify-center rounded-xl border border-border bg-muted/40">
          <Icon className="size-5 text-muted-foreground" aria-hidden="true" />
        </span>
      )}
      <p className="text-sm font-medium">{title}</p>
      {description !== undefined && <p className="max-w-sm text-sm text-muted-foreground">{description}</p>}
      {action !== undefined && <div className="mt-2">{action}</div>}
    </div>
  );
}

export { EmptyState };
