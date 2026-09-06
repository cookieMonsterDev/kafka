import * as React from 'react';
import { cva, type VariantProps } from 'class-variance-authority';

import { cn } from '../../lib/utils';

const badgeVariants = cva(
  'inline-flex shrink-0 items-center gap-1 rounded-full border px-1.5 py-0.5 text-[0.6875rem] leading-none font-medium tabular-nums',
  {
    variants: {
      variant: {
        default: 'border-transparent bg-muted text-muted-foreground',
        accent: 'border-primary/25 bg-primary/12 text-primary',
        outline: 'border-border text-muted-foreground',
        destructive: 'border-destructive/30 bg-destructive/15 text-destructive',
      },
    },
    defaultVariants: { variant: 'default' },
  },
);

function Badge({ className, variant, ...props }: React.ComponentProps<'span'> & VariantProps<typeof badgeVariants>) {
  return <span data-slot="badge" className={cn(badgeVariants({ variant }), className)} {...props} />;
}

export { Badge, badgeVariants };
