import * as React from 'react';
import { LoaderCircle } from 'lucide-react';

import { cn } from '../../lib/utils';

interface SpinnerProps extends React.ComponentProps<'span'> {
  /** Announced to assistive tech. Say what is loading, not just "Loading". */
  readonly label: string;
}

/**
 * The only spinner in the app. `animate-spin` is disabled under `prefers-reduced-motion` by the
 * base layer, so the glyph still communicates "busy" without the rotation.
 */
function Spinner({ className, label, ...props }: SpinnerProps) {
  return (
    <span data-slot="spinner" role="status" className={cn('inline-flex items-center', className)} {...props}>
      <LoaderCircle className="size-4 animate-spin text-muted-foreground" aria-hidden="true" />
      <span className="sr-only">{label}</span>
    </span>
  );
}

export { Spinner };
