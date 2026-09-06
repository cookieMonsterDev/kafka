import * as React from 'react';
import { Dialog as SheetPrimitive } from 'radix-ui';

import { cn } from '../../lib/utils';

const Sheet = SheetPrimitive.Root;
const SheetTrigger = SheetPrimitive.Trigger;
const SheetClose = SheetPrimitive.Close;
const SheetPortal = SheetPrimitive.Portal;

function SheetOverlay({ className, ...props }: React.ComponentProps<typeof SheetPrimitive.Overlay>) {
  return (
    <SheetPrimitive.Overlay
      data-slot="sheet-overlay"
      className={cn('fixed inset-0 z-50 bg-black/50', className)}
      {...props}
    />
  );
}

const SHEET_SIDE_CLASSNAME = {
  left: 'inset-y-0 left-0 h-full w-full max-w-xs border-r',
  right: 'inset-y-0 right-0 h-full w-full max-w-xs border-l',
  top: 'inset-x-0 top-0 h-auto max-h-[80vh] border-b',
  bottom: 'inset-x-0 bottom-0 h-auto max-h-[80vh] border-t',
} as const;

type SheetSide = keyof typeof SHEET_SIDE_CLASSNAME;

function SheetContent({
  className,
  children,
  side = 'right',
  showClose = true,
  ...props
}: React.ComponentProps<typeof SheetPrimitive.Content> & { side?: SheetSide; showClose?: boolean }) {
  return (
    <SheetPortal>
      <SheetOverlay />
      <SheetPrimitive.Content
        data-slot="sheet-content"
        className={cn(
          'fixed z-50 flex flex-col gap-4 border-border bg-card p-4 text-card-foreground shadow-lg outline-none',
          SHEET_SIDE_CLASSNAME[side],
          className,
        )}
        {...props}
      >
        {children}
        {showClose && (
          <SheetPrimitive.Close
            className="absolute top-4 right-4 rounded-lg text-muted-foreground outline-none hover:text-foreground focus-visible:ring-3 focus-visible:ring-ring/50"
            aria-label="Close"
          >
            <svg
              className="size-4"
              viewBox="0 0 24 24"
              fill="none"
              stroke="currentColor"
              strokeWidth="2"
              aria-hidden="true"
            >
              <path d="M18 6 6 18" />
              <path d="m6 6 12 12" />
            </svg>
          </SheetPrimitive.Close>
        )}
      </SheetPrimitive.Content>
    </SheetPortal>
  );
}

function SheetHeader({ className, ...props }: React.ComponentProps<'div'>) {
  return <div data-slot="sheet-header" className={cn('flex flex-col gap-1.5', className)} {...props} />;
}

function SheetTitle({ className, ...props }: React.ComponentProps<typeof SheetPrimitive.Title>) {
  return <SheetPrimitive.Title data-slot="sheet-title" className={cn('text-sm font-semibold', className)} {...props} />;
}

function SheetDescription({ className, ...props }: React.ComponentProps<typeof SheetPrimitive.Description>) {
  return (
    <SheetPrimitive.Description
      data-slot="sheet-description"
      className={cn('text-sm text-muted-foreground', className)}
      {...props}
    />
  );
}

export { Sheet, SheetClose, SheetContent, SheetDescription, SheetHeader, SheetOverlay, SheetTitle, SheetTrigger };
