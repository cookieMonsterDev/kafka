import { Activity, ArrowDownToLine, ArrowUpFromLine } from 'lucide-react';
import type { StudioEvent } from '../../../shared/contracts/event';
import { Badge } from '../ui/badge';
import { Card, CardContent, CardHeader, CardTitle } from '../ui/card';
import { EmptyState } from '../ui/empty-state';
import { formatBytes } from '../../lib/utils';

const RECENT_ACTIVITY_LIMIT = 25;

function Stat({ label, value }: { readonly label: string; readonly value: number }) {
  return (
    <div className="flex flex-col gap-0.5">
      <span className="text-xs text-muted-foreground">{label}</span>
      <span className="text-lg font-semibold tabular-nums">{value}</span>
    </div>
  );
}

function describeActivity(event: StudioEvent): string {
  const verb = event.kind === 'produce' ? 'Produced' : 'Tailed';
  const noun = event.count === 1 ? 'message' : 'messages';
  return `${verb} ${String(event.count)} ${noun} · ${event.topic}`;
}

function ActivityRow({ event }: { readonly event: StudioEvent }) {
  return (
    <li className="flex items-start gap-2 py-1.5 text-sm">
      {event.kind === 'produce' ? (
        <ArrowUpFromLine className="mt-0.5 size-3.5 shrink-0 text-primary" aria-hidden="true" />
      ) : (
        <ArrowDownToLine className="mt-0.5 size-3.5 shrink-0 text-muted-foreground" aria-hidden="true" />
      )}
      <span className="min-w-0 flex-1">
        <span className="block truncate">{describeActivity(event)}</span>
        <span className="text-xs text-muted-foreground">
          {new Date(event.timestamp).toLocaleTimeString()} · {formatBytes(String(event.bytes))}
        </span>
      </span>
    </li>
  );
}

export interface BoardMetricsProps {
  readonly events: readonly StudioEvent[];
  readonly topicCount: number;
  readonly groupCount: number;
  readonly live: boolean;
}

/**
 * The board's right-hand rail: cluster-wide counts (already-cached queries, not re-fetched here)
 * and a feed of recent activity read straight from the same firehose the particle layer animates —
 * nothing here is invented for the sake of looking alive.
 */
export function BoardMetrics({ events, topicCount, groupCount, live }: BoardMetricsProps) {
  const recent = events.slice(-RECENT_ACTIVITY_LIMIT).toReversed();

  return (
    <div className="flex h-full min-h-0 flex-col gap-4">
      <Card className="shrink-0">
        <CardHeader>
          <CardTitle>Cluster stats</CardTitle>
        </CardHeader>
        <CardContent className="grid grid-cols-2 gap-4">
          <Stat label="Topics" value={topicCount} />
          <Stat label="Consumer groups" value={groupCount} />
        </CardContent>
      </Card>

      {/* Fills whatever height the rail has left — its own border reaches the bottom of the rail
          instead of stopping partway and leaving a stretch of plain background under it. */}
      <Card className="flex min-h-0 flex-1 flex-col">
        <CardHeader className="items-center">
          <CardTitle>Recent activity</CardTitle>
          <Badge variant={live ? 'accent' : 'outline'} className="gap-1">
            <Activity className="size-3" aria-hidden="true" />
            {live ? 'Live' : 'Disconnected'}
          </Badge>
        </CardHeader>
        <CardContent className="min-h-0 flex-1 overflow-y-auto [scrollbar-gutter:stable]">
          {recent.length === 0 ? (
            <EmptyState
              icon={Activity}
              title="No activity yet"
              description="Produce or tail a topic to see it flow through the board."
            />
          ) : (
            <ul className="flex flex-col divide-y divide-border">
              {recent.map((event) => (
                <ActivityRow key={event.id} event={event} />
              ))}
            </ul>
          )}
        </CardContent>
      </Card>
    </div>
  );
}
