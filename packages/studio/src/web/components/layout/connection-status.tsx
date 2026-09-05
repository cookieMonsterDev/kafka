import { Lock } from 'lucide-react';
import { useQuery } from '@tanstack/react-query';
import { Tooltip, TooltipContent, TooltipTrigger } from '../ui/tooltip';
import { cn } from '../../lib/utils';

interface HealthResponse {
  readonly status: string;
  readonly readOnly: boolean;
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
  const readOnly = data?.readOnly === true;
  const label = connected ? 'Connected' : 'Unreachable';
  const fullLabel = readOnly ? `${label} · Read-only mode` : label;

  const pill = (
    <div className="flex items-center gap-2 px-1 py-1 text-xs text-muted-foreground" role="status">
      <span
        aria-hidden="true"
        className={cn('size-2 shrink-0 rounded-full', connected ? 'bg-emerald-500' : 'bg-destructive')}
      />
      <span className={collapsed ? 'sr-only' : 'truncate'}>{label}</span>
      {readOnly && <Lock className={cn('size-3.5 shrink-0', !collapsed && 'ml-auto')} aria-hidden="true" />}
      {readOnly && <span className="sr-only">Read-only mode</span>}
    </div>
  );

  if (!collapsed) return pill;

  return (
    <Tooltip>
      <TooltipTrigger asChild>{pill}</TooltipTrigger>
      <TooltipContent side="right">{fullLabel}</TooltipContent>
    </Tooltip>
  );
}
