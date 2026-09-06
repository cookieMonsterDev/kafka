import { useId, useState } from 'react';
import { Button } from '../ui/button';
import { Dialog, DialogContent, DialogDescription, DialogFooter, DialogHeader, DialogTitle } from '../ui/dialog';
import { Input } from '../ui/input';

export interface TemplateNameDialogProps {
  readonly onOpenChange: (open: boolean) => void;
  readonly title: string;
  readonly description: string;
  readonly confirmLabel: string;
  /** Prefills the field — used for renaming an existing template; empty for a new one. */
  readonly initialLabel?: string;
  readonly onSubmit: (label: string) => void;
}

/**
 * Prompts for a template name, prefilled when renaming — shared by the "save as template" and
 * "rename template" actions. The caller mounts one instance per request (keyed so a rename of a
 * different template gets a fresh instance rather than this one's state lingering) and unmounts
 * it on close, so `initialLabel` only ever needs to seed `useState` once.
 */
export function TemplateNameDialog({
  onOpenChange,
  title,
  description,
  confirmLabel,
  initialLabel = '',
  onSubmit,
}: TemplateNameDialogProps) {
  const [label, setLabel] = useState(initialLabel);
  const labelId = useId();
  const trimmed = label.trim();

  function handleSubmit(): void {
    if (trimmed === '') return;
    onSubmit(trimmed);
    onOpenChange(false);
  }

  return (
    <Dialog open onOpenChange={onOpenChange}>
      <DialogContent>
        <DialogHeader>
          <DialogTitle>{title}</DialogTitle>
          <DialogDescription>{description}</DialogDescription>
        </DialogHeader>
        <label htmlFor={labelId} className="flex flex-col gap-1.5">
          <span className="text-sm font-medium">Name</span>
          <Input
            id={labelId}
            // A modal the user just opened is supposed to move focus onto its primary control (WAI-ARIA dialog pattern).
            autoFocus
            value={label}
            onChange={(event) => setLabel(event.target.value)}
            onKeyDown={(event) => {
              if (event.key === 'Enter') {
                event.preventDefault();
                handleSubmit();
              }
            }}
            placeholder="e.g. Cancelled order"
          />
        </label>
        <DialogFooter>
          <Button type="button" variant="outline" onClick={() => onOpenChange(false)}>
            Cancel
          </Button>
          <Button type="button" disabled={trimmed === ''} onClick={handleSubmit}>
            {confirmLabel}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}
