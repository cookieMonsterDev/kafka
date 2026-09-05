import { useQuery } from '@tanstack/react-query';
import { Tooltip, TooltipContent, TooltipTrigger } from '../ui/tooltip';
import { cn } from '../../lib/utils';

interface HealthResponse {
  readonly status: string;
}

const HEALTH_POLL_INTERVAL_MS = 15_000;

async function fetchHealth(): Promise<HealthResponse> {
  const res = await fetch('/api/health');
  if (!res.ok) throw new Error(`GET /api/health failed with ${String(res.status)}`);
  return (await res.json()) as HealthResponse;
}

export interface ConnectionStatusProps {
  readonly collapsed: boolean;
}

/**
 * Sidebar-footer reachability pill — the mock's full "cluster health / uptime" card would just
 * duplicate the cluster-overview page (§1.5), so this only answers one question: is the studio
 * server itself still there. `role="status"` carries the text so it announces on change without a
 * separate live region, and the label is never conveyed by color alone.
 */
export function ConnectionStatus({ collapsed }: ConnectionStatusProps) {
  const { data, isError } = useQuery({
    queryKey: ['health'],
    queryFn: fetchHealth,
    refetchInterval: HEALTH_POLL_INTERVAL_MS,
    retry: false,
  });

  const connected = data !== undefined && !isError;
  const label = connected ? 'Connected' : 'Unreachable';

  const pill = (
    <div
      // Focusable only in the collapsed state, where it's the Tooltip's trigger — a keyboard user
      // needs a stop to focus before the tooltip can show; in the expanded state the text is
      // already visible, so adding a tab stop here would just be a dead end.
      tabIndex={collapsed ? 0 : undefined}
      className={cn(
        'flex items-center gap-2 rounded-md px-1 py-1 text-xs text-muted-foreground outline-none',
        collapsed && 'focus-visible:ring-3 focus-visible:ring-ring/50',
      )}
      role="status"
    >
      <span
        aria-hidden="true"
        className={cn('size-2 shrink-0 rounded-full', connected ? 'bg-emerald-500' : 'bg-destructive')}
      />
      <span className={collapsed ? 'sr-only' : 'truncate'}>{label}</span>
    </div>
  );

  if (!collapsed) return pill;

  return (
    <Tooltip>
      <TooltipTrigger asChild>{pill}</TooltipTrigger>
      <TooltipContent side="right">{label}</TooltipContent>
    </Tooltip>
  );
}
