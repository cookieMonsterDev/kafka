import { useMemo, useState } from 'react';
import type { GroupOffsetResetTarget, GroupPartitionLag } from '../../../shared/contracts/group';
import { Button } from '../ui/button';
import { Dialog, DialogContent, DialogFooter, DialogHeader, DialogTitle } from '../ui/dialog';
import { Input } from '../ui/input';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '../ui/select';

type ResetKind = GroupOffsetResetTarget['to'];

const RESET_KIND_LABEL: Record<ResetKind, string> = {
  earliest: 'Earliest available offset',
  latest: 'Latest offset',
  offset: 'A specific offset',
  timestamp: 'The offset as of a timestamp',
};

export interface ResetOffsetsDialogProps {
  readonly open: boolean;
  readonly onOpenChange: (open: boolean) => void;
  readonly partitionLag: readonly GroupPartitionLag[];
  readonly pending?: boolean;
  readonly onConfirm: (topic: string, partitions: readonly GroupOffsetResetTarget[]) => void;
}

/**
 * One reset target applied to every partition of the chosen topic — the same shape
 * `kafka-consumer-groups.sh --reset-offsets` exposes (per-topic, not per-partition), which covers
 * the common case without a custom editor for every partition. The current committed offset and
 * lag for that topic is shown as the "before" state so the operator can see what this touches
 * before confirming.
 */
export function ResetOffsetsDialog({ open, onOpenChange, partitionLag, pending, onConfirm }: ResetOffsetsDialogProps) {
  const topics = useMemo(() => [...new Set(partitionLag.map((entry) => entry.topic))], [partitionLag]);
  const [topic, setTopic] = useState<string | null>(topics[0] ?? null);
  const [kind, setKind] = useState<ResetKind>('latest');
  const [offset, setOffset] = useState('0');
  const [timestamp, setTimestamp] = useState('');

  const rows = topic === null ? [] : partitionLag.filter((entry) => entry.topic === topic);
  const timestampMs = Date.parse(timestamp);
  const canConfirm =
    topic !== null &&
    rows.length > 0 &&
    (kind !== 'offset' || /^\d+$/.test(offset.trim())) &&
    (kind !== 'timestamp' || !Number.isNaN(timestampMs));

  function handleConfirm(): void {
    if (!canConfirm || topic === null) return;
    const partitions: GroupOffsetResetTarget[] = rows.map((row) => {
      if (kind === 'offset') return { partition: row.partition, to: 'offset', offset: offset.trim() };
      if (kind === 'timestamp') return { partition: row.partition, to: 'timestamp', timestamp: timestampMs };
      return { partition: row.partition, to: kind };
    });
    onConfirm(topic, partitions);
  }

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent>
        <DialogHeader>
          <DialogTitle>Reset offsets</DialogTitle>
        </DialogHeader>

        <div className="flex flex-col gap-4">
          <div className="flex flex-col gap-1.5">
            <label className="text-sm font-medium" htmlFor="reset-topic">
              Topic
            </label>
            <Select value={topic ?? undefined} onValueChange={setTopic}>
              <SelectTrigger id="reset-topic" aria-label="Topic">
                <SelectValue placeholder="Select a topic" />
              </SelectTrigger>
              <SelectContent>
                {topics.map((name) => (
                  <SelectItem key={name} value={name}>
                    {name}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>

          <div className="flex flex-col gap-1.5">
            <label className="text-sm font-medium" htmlFor="reset-kind">
              Reset to
            </label>
            <Select value={kind} onValueChange={(value) => setKind(value as ResetKind)}>
              <SelectTrigger id="reset-kind" aria-label="Reset to">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                {(Object.keys(RESET_KIND_LABEL) as ResetKind[]).map((value) => (
                  <SelectItem key={value} value={value}>
                    {RESET_KIND_LABEL[value]}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>

          {kind === 'offset' && (
            <div className="flex flex-col gap-1.5">
              <label className="text-sm font-medium" htmlFor="reset-offset">
                Offset
              </label>
              <Input
                id="reset-offset"
                inputMode="numeric"
                value={offset}
                onChange={(event) => setOffset(event.target.value)}
              />
            </div>
          )}

          {kind === 'timestamp' && (
            <div className="flex flex-col gap-1.5">
              <label className="text-sm font-medium" htmlFor="reset-timestamp">
                Timestamp
              </label>
              <Input
                id="reset-timestamp"
                type="datetime-local"
                value={timestamp}
                onChange={(event) => setTimestamp(event.target.value)}
              />
            </div>
          )}

          {rows.length > 0 && (
            <div className="overflow-hidden rounded-lg border border-border">
              <table className="w-full text-sm">
                <thead>
                  <tr className="border-b border-border text-left text-xs text-muted-foreground">
                    <th scope="col" className="px-3 py-1.5 font-medium">
                      Partition
                    </th>
                    <th scope="col" className="px-3 py-1.5 font-medium">
                      Committed
                    </th>
                    <th scope="col" className="px-3 py-1.5 font-medium">
                      Lag
                    </th>
                  </tr>
                </thead>
                <tbody>
                  {rows.map((row) => (
                    <tr key={row.partition} className="border-b border-border last:border-0">
                      <td className="px-3 py-1.5">{row.partition}</td>
                      <td className="px-3 py-1.5">{row.committedOffset ?? '—'}</td>
                      <td className="px-3 py-1.5">{row.lag ?? '—'}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </div>

        <DialogFooter>
          <Button type="button" variant="outline" onClick={() => onOpenChange(false)}>
            Cancel
          </Button>
          <Button type="button" disabled={!canConfirm || pending === true} onClick={handleConfirm}>
            {pending === true ? 'Resetting…' : 'Reset offsets'}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}
