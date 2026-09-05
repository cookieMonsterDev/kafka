import * as React from 'react';
import { X } from 'lucide-react';
import { Toast as ToastPrimitive } from 'radix-ui';

import { cn } from '../../lib/utils';

interface ToastMessage {
  readonly id: number;
  readonly title: string;
  readonly description?: string | undefined;
  readonly variant: 'default' | 'destructive';
}

type ToastInput = Omit<ToastMessage, 'id'>;

/**
 * A module-level queue rather than a context, so non-React callers can raise a toast — the
 * QueryClient's `MutationCache.onError` in `main.tsx` is the reason this exists.
 */
const listeners = new Set<(message: ToastMessage) => void>();
let nextId = 0;

export function toast(input: ToastInput): void {
  nextId += 1;
  const message: ToastMessage = { ...input, id: nextId };
  for (const listener of listeners) listener(message);
}

/** Convenience for the common case: something failed and the user should know why. */
export function toastError(title: string, error: unknown): void {
  const description = error instanceof Error && error.message !== '' ? error.message : undefined;
  toast({ title, description, variant: 'destructive' });
}

/**
 * Mount once, at the app root. Failures raised while a dialog was closing would otherwise
 * vanish with the dialog.
 */
export function Toaster() {
  const [messages, setMessages] = React.useState<readonly ToastMessage[]>([]);

  React.useEffect(() => {
    function onMessage(message: ToastMessage) {
      setMessages((current) => [...current, message]);
    }
    listeners.add(onMessage);
    return () => {
      listeners.delete(onMessage);
    };
  }, []);

  function dismiss(id: number) {
    setMessages((current) => current.filter((message) => message.id !== id));
  }

  return (
    <ToastPrimitive.Provider swipeDirection="right" duration={8000}>
      {messages.map((message) => (
        <ToastPrimitive.Root
          key={message.id}
          open
          onOpenChange={(open) => {
            if (!open) dismiss(message.id);
          }}
          className={cn(
            'flex items-start gap-3 rounded-xl border bg-popover p-3 text-popover-foreground shadow-lg',
            'data-[swipe=end]:translate-x-(--radix-toast-swipe-end-x)',
            'data-[swipe=move]:translate-x-(--radix-toast-swipe-move-x) data-[swipe=move]:transition-none',
            message.variant === 'destructive' ? 'border-destructive/40' : 'border-border',
          )}
        >
          <div className="min-w-0 flex-1">
            <ToastPrimitive.Title
              className={cn('text-sm font-medium', message.variant === 'destructive' && 'text-destructive')}
            >
              {message.title}
            </ToastPrimitive.Title>
            {message.description !== undefined && (
              <ToastPrimitive.Description className="mt-0.5 text-sm wrap-break-word text-muted-foreground">
                {message.description}
              </ToastPrimitive.Description>
            )}
          </div>
          <ToastPrimitive.Close
            aria-label="Dismiss"
            className="shrink-0 rounded-md p-1 text-muted-foreground outline-none hover:text-foreground focus-visible:ring-3 focus-visible:ring-ring/50"
          >
            <X className="size-4" aria-hidden="true" />
          </ToastPrimitive.Close>
        </ToastPrimitive.Root>
      ))}
      <ToastPrimitive.Viewport className="fixed right-0 bottom-0 z-100 flex w-full max-w-sm flex-col gap-2 p-4 outline-none" />
    </ToastPrimitive.Provider>
  );
}
