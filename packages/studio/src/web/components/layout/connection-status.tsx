import { useQuery } from '@tanstack/react-query';
import { Tooltip, TooltipContent, TooltipTrigger } from '../ui/tooltip';
import { fetchHealth, healthQueryKey } from '../../lib/api';
import { cn } from '../../lib/utils';

const HEALTH_POLL_INTERVAL_MS = 15_000;

type ConnectionState = 'connecting' | 'connected' | 'unreachable';

const STATE_LABEL: Record<ConnectionState, string> = {
  connecting: 'Connecting…',
  connected: 'Connected',
  unreachable: 'Unreachable',
};

const STATE_DOT: Record<ConnectionState, string> = {
  connecting: 'bg-muted-foreground',
  connected: 'bg-primary',
  unreachable: 'bg-destructive',
};

export interface ConnectionStatusProps {
  readonly collapsed: boolean;
}

/**
 * Sidebar-footer reachability card — a full "cluster health / uptime" card would just duplicate
 * the cluster-overview page, so this only answers one question: is the studio server itself still
 * there. `role="status"` carries the text so it announces on change without a separate live
 * region, and the state is never conveyed by color alone.
 */
export function ConnectionStatus({ collapsed }: ConnectionStatusProps) {
  const { data, isPending, isError } = useQuery({
    queryKey: healthQueryKey,
    queryFn: fetchHealth,
    refetchInterval: HEALTH_POLL_INTERVAL_MS,
    retry: false,
  });

  const state: ConnectionState = isPending ? 'connecting' : isError ? 'unreachable' : 'connected';
  const label = STATE_LABEL[state];

  if (collapsed) {
    return (
      <Tooltip>
        <TooltipTrigger asChild>
          {/*
            Focusable only in the collapsed state, where it's the Tooltip's trigger — a keyboard
            user needs a stop to focus before the tooltip can show; expanded, the text is already
            visible, so a tab stop there would just be a dead end.
          */}
          <div
            tabIndex={0}
            role="status"
            className="flex h-10 items-center justify-center rounded-lg outline-none focus-visible:ring-3 focus-visible:ring-ring/50"
          >
            <span aria-hidden="true" className={cn('size-2 shrink-0 rounded-full', STATE_DOT[state])} />
            <span className="sr-only">{label}</span>
          </div>
        </TooltipTrigger>
        <TooltipContent side="right">{label}</TooltipContent>
      </Tooltip>
    );
  }

  return (
    <div role="status" className="rounded-xl border border-border bg-card p-3">
      <div className="flex items-center justify-between gap-2">
        <span className="text-[0.6875rem] font-medium tracking-widest text-muted-foreground uppercase">Connection</span>
        <span aria-hidden="true" className={cn('size-2 shrink-0 rounded-full', STATE_DOT[state])} />
      </div>
      <p className="mt-1.5 truncate text-sm font-medium">{label}</p>
      <p className="truncate text-xs text-muted-foreground">
        {data?.readOnly === true ? 'Studio server · read-only' : 'Studio server'}
      </p>
    </div>
  );
}
