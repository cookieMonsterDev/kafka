import { Redo2, Trash2 } from 'lucide-react';
import { EmptyState } from '../ui/empty-state';
import { Button } from '../ui/button';

export interface ProduceHistoryEntry {
  readonly id: string;
  readonly topic: string;
  readonly key: string;
  readonly value: string;
  readonly sentAt: string;
  readonly outcome: { readonly ok: true; readonly detail: string } | { readonly ok: false; readonly detail: string };
}

export interface ProduceHistoryProps {
  readonly entries: readonly ProduceHistoryEntry[];
  readonly onReplay: (entry: ProduceHistoryEntry) => void;
  readonly onClear: () => void;
}

function truncate(text: string, max: number): string {
  return text.length > max ? `${text.slice(0, max)}…` : text;
}

/**
 * A session-local log of what this producer session has sent — not persisted, since it exists only
 * to let an operator re-fire the last few messages, not to be a durable audit trail (Kafka itself
 * is that).
 */
export function ProduceHistory({ entries, onReplay, onClear }: ProduceHistoryProps) {
  if (entries.length === 0) {
    return (
      <EmptyState
        icon={Redo2}
        title="Nothing sent yet"
        description="Messages you send in this session show up here for a quick replay."
      />
    );
  }

  return (
    <div className="flex flex-col gap-3">
      <div className="flex items-center justify-between">
        <h2 className="text-sm font-semibold">Recent sends</h2>
        <Button type="button" variant="ghost" size="sm" onClick={onClear}>
          <Trash2 className="size-4" aria-hidden="true" />
          Clear
        </Button>
      </div>
      <ul className="flex flex-col gap-2">
        {entries.map((entry) => (
          <li key={entry.id} className="rounded-lg border border-border p-3 text-sm">
            <div className="flex items-start justify-between gap-2">
              <div className="min-w-0">
                <p className="truncate font-medium">{entry.key === '' ? '(no key)' : entry.key}</p>
                <p className="mt-0.5 truncate font-mono text-xs text-muted-foreground">{truncate(entry.value, 80)}</p>
              </div>
              <Button type="button" variant="outline" size="sm" onClick={() => onReplay(entry)}>
                <Redo2 className="size-4" aria-hidden="true" />
                <span className="sr-only sm:not-sr-only">Replay</span>
              </Button>
            </div>
            <p className={`mt-1.5 text-xs ${entry.outcome.ok ? 'text-muted-foreground' : 'text-destructive'}`}>
              {entry.topic} · {entry.outcome.detail}
            </p>
          </li>
        ))}
      </ul>
    </div>
  );
}
