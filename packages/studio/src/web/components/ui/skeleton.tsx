import * as React from 'react';

import { cn } from '../../lib/utils';

/**
 * A shimmer placeholder. Always size it to the real content it stands in for — a skeleton that
 * doesn't reserve the right box makes the page jump when data lands, which is worse than a
 * spinner. `aria-hidden` because the surrounding region already announces its busy state.
 */
function Skeleton({ className, ...props }: React.ComponentProps<'div'>) {
  return (
    <div
      data-slot="skeleton"
      aria-hidden="true"
      className={cn('animate-pulse rounded-md bg-muted', className)}
      {...props}
    />
  );
}

export { Skeleton };
