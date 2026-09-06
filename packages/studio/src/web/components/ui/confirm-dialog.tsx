import { useId, useState } from 'react';
import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
  AlertDialogTrigger,
} from './alert-dialog';
import { Button } from './button';
import { Input } from './input';

export interface ConfirmDialogProps {
  readonly trigger: React.ReactNode;
  readonly title: string;
  readonly description: React.ReactNode;
  /** The exact text the operator must type to enable the confirm action — usually the resource's own name. */
  readonly confirmValue: string;
  readonly confirmLabel?: string;
  readonly onConfirm: () => void;
  readonly pending?: boolean;
}

/**
 * A destructive action stays behind a modal that requires typing the resource's own name before
 * the confirm button enables — a misclick or a stray Enter key can't trigger it. The dialog closes
 * itself as soon as the confirm button is clicked (Radix's default `Action` behavior); a caller
 * whose mutation can fail is responsible for surfacing that failure itself (a toast, an inline
 * banner on the page behind the dialog), not this component.
 */
export function ConfirmDialog({
  trigger,
  title,
  description,
  confirmValue,
  confirmLabel = 'Delete',
  onConfirm,
  pending = false,
}: ConfirmDialogProps) {
  const [typed, setTyped] = useState('');
  const [open, setOpen] = useState(false);
  const inputId = useId();
  const matches = typed === confirmValue;

  return (
    <AlertDialog
      open={open}
      onOpenChange={(next) => {
        setOpen(next);
        if (!next) setTyped('');
      }}
    >
      <AlertDialogTrigger asChild>{trigger}</AlertDialogTrigger>
      <AlertDialogContent>
        <AlertDialogHeader>
          <AlertDialogTitle>{title}</AlertDialogTitle>
          <AlertDialogDescription>{description}</AlertDialogDescription>
        </AlertDialogHeader>
        <div className="flex flex-col gap-1.5">
          <label htmlFor={inputId} className="text-sm text-muted-foreground">
            Type <span className="font-mono font-medium text-foreground">{confirmValue}</span> to confirm
          </label>
          <Input
            id={inputId}
            value={typed}
            onChange={(event) => setTyped(event.target.value)}
            autoComplete="off"
            spellCheck={false}
          />
        </div>
        <AlertDialogFooter>
          <AlertDialogCancel asChild>
            <Button type="button" variant="outline">
              Cancel
            </Button>
          </AlertDialogCancel>
          <AlertDialogAction asChild>
            <Button
              type="button"
              variant="destructive"
              disabled={!matches || pending}
              onClick={(event) => {
                if (!matches) {
                  event.preventDefault();
                  return;
                }
                onConfirm();
              }}
            >
              {pending ? 'Working…' : confirmLabel}
            </Button>
          </AlertDialogAction>
        </AlertDialogFooter>
      </AlertDialogContent>
    </AlertDialog>
  );
}
