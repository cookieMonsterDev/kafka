import { useState } from 'react';
import { Check, Copy, Trash2 } from 'lucide-react';
import { useMutation, useQueryClient } from '@tanstack/react-query';
import type { MessageRecord } from '../../../shared/contracts/message';
import { decodeMessageField } from '../../lib/decode';
import { errorMessage } from '../../lib/error-message';
import { deleteRecords, messagesQueryKeys } from '../../lib/messages-api';
import { formatTimestamp } from '../../lib/utils';
import { Badge } from '../ui/badge';
import { Button } from '../ui/button';
import { ConfirmDialog } from '../ui/confirm-dialog';
import { toast } from '../ui/toast';
import type { MessageFiltersValue } from './filters';

const COPY_CONFIRMATION_MS = 1500;

/** Copies decoded text to the clipboard, with a brief inline confirmation instead of only a toast — the toast can be missed while looking at the rail. */
function CopyButton({ text, label }: { readonly text: string; readonly label: string }) {
  const [copied, setCopied] = useState(false);

  async function handleCopy(): Promise<void> {
    try {
      await navigator.clipboard.writeText(text);
      setCopied(true);
      setTimeout(() => setCopied(false), COPY_CONFIRMATION_MS);
    } catch (error) {
      toast({ title: 'Could not copy to clipboard', description: errorMessage(error), variant: 'destructive' });
    }
  }

  return (
    <Button type="button" variant="ghost" size="icon-xs" aria-label={label} onClick={() => void handleCopy()}>
      {copied ? (
        <Check className="size-3.5 text-primary" aria-hidden="true" />
      ) : (
        <Copy className="size-3.5" aria-hidden="true" />
      )}
    </Button>
  );
}

export interface MessageDetailRailProps {
  readonly topic: string;
  readonly message: MessageRecord;
  readonly decoder: MessageFiltersValue['decoder'];
}

export function MessageDetailRail({ topic, message, decoder }: MessageDetailRailProps) {
  const queryClient = useQueryClient();
  const key = message.key === null ? null : decodeMessageField(message.key, decoder);
  const value = message.value === null ? null : decodeMessageField(message.value, decoder);
  const headerEntries = Object.entries(message.headers);

  const deleteMutation = useMutation({
    mutationFn: () =>
      deleteRecords(topic, {
        partitions: [{ partition: message.partition, beforeOffset: String(BigInt(message.offset) + 1n) }],
      }),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: messagesQueryKeys.all }),
  });

  return (
    <div className="flex flex-col gap-4">
      <div className="flex flex-wrap items-center gap-2">
        <Badge variant="outline">Partition {message.partition}</Badge>
        <Badge variant="outline">Offset {message.offset}</Badge>
      </div>
      <dl className="grid grid-cols-[auto_1fr] gap-x-3 gap-y-1 text-sm">
        <dt className="text-muted-foreground">Timestamp</dt>
        <dd>{formatTimestamp(message.timestamp)}</dd>
        <dt className="text-muted-foreground">Size</dt>
        <dd>{message.size} B</dd>
      </dl>

      <div className="flex flex-col gap-1.5">
        <div className="flex items-center justify-between">
          <span className="text-sm font-medium">Key</span>
          <CopyButton text={key === null ? 'null' : key.text} label="Copy key" />
        </div>
        <pre className="max-h-32 overflow-auto rounded-lg border border-border bg-muted/30 p-2 font-mono text-xs break-all whitespace-pre-wrap">
          {key === null ? 'null' : key.text}
        </pre>
        {key?.error !== undefined && <p className="text-xs text-muted-foreground">{key.error}</p>}
      </div>

      <div className="flex flex-col gap-1.5">
        <div className="flex items-center justify-between">
          <span className="text-sm font-medium">Value</span>
          <CopyButton text={value === null ? 'null' : value.text} label="Copy value" />
        </div>
        <pre className="max-h-64 overflow-auto rounded-lg border border-border bg-muted/30 p-2 font-mono text-xs break-all whitespace-pre-wrap">
          {value === null ? 'null' : value.text}
        </pre>
        {value?.error !== undefined && <p className="text-xs text-muted-foreground">{value.error}</p>}
      </div>

      {headerEntries.length > 0 && (
        <div className="flex flex-col gap-1.5">
          <span className="text-sm font-medium">Headers</span>
          <dl className="flex flex-col gap-1 text-xs">
            {headerEntries.map(([headerKey, headerValue]) => (
              <div key={headerKey} className="flex gap-2">
                <dt className="shrink-0 font-medium text-muted-foreground">{headerKey}</dt>
                <dd className="min-w-0 break-all">
                  {headerValue === null ? 'null' : decodeMessageField(headerValue, decoder).text}
                </dd>
              </div>
            ))}
          </dl>
        </div>
      )}

      <div className="mt-2 border-t border-border pt-4">
        <ConfirmDialog
          trigger={
            <Button type="button" variant="destructive" size="sm">
              <Trash2 className="size-4" aria-hidden="true" />
              Delete this record and everything before it
            </Button>
          }
          title="Delete records?"
          description={`Permanently deletes every record on partition ${String(message.partition)} up to and including offset ${message.offset}. This cannot be undone.`}
          confirmValue={topic}
          confirmLabel="Delete records"
          pending={deleteMutation.isPending}
          onConfirm={() => deleteMutation.mutate()}
        />
        {deleteMutation.isError && (
          <p className="mt-2 text-sm text-destructive" role="alert">
            {errorMessage(deleteMutation.error) ?? 'delete failed'}
          </p>
        )}
      </div>
    </div>
  );
}
