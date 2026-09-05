import { Server } from 'lucide-react';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '../ui/select';
import { Tooltip, TooltipContent, TooltipTrigger } from '../ui/tooltip';
import { apiGet, apiSend, clusterQueryKey } from '../../lib/api';
import { cn } from '../../lib/utils';

interface ProfilesResponse {
  readonly active: string | null;
  readonly profiles: Readonly<Record<string, unknown>>;
}

const profilesQueryKey = ['profiles'] as const;

const fetchProfiles = (): Promise<ProfilesResponse> => apiGet<ProfilesResponse>('/api/profiles');

const setActiveProfile = (profile: string | null): Promise<ProfilesResponse> =>
  apiSend<ProfilesResponse>('POST', '/api/profiles/active', { profile });

/** `Select` values must be non-empty strings, so "no profile" needs a sentinel of its own. */
const DIRECT_CONNECTION = '__direct__';

export interface ProfileSwitcherProps {
  /** Icon-only sidebar rail: the trigger doesn't fit, so this shows a static hint instead. */
  readonly collapsed?: boolean;
  readonly className?: string;
}

/**
 * Lists the profiles configured under `cli.profiles` and lets the operator switch which one the
 * server connects with. Renders nothing when no profiles are configured — there's nothing to
 * switch between yet.
 */
export function ProfileSwitcher({ collapsed = false, className }: ProfileSwitcherProps) {
  const queryClient = useQueryClient();
  const { data } = useQuery({ queryKey: profilesQueryKey, queryFn: fetchProfiles });
  const mutation = useMutation({
    mutationFn: setActiveProfile,
    onSuccess: (result) => {
      queryClient.setQueryData(profilesQueryKey, result);
      void queryClient.invalidateQueries({ queryKey: clusterQueryKey });
    },
  });

  const names = data ? Object.keys(data.profiles) : [];
  if (names.length === 0) return null;

  const activeLabel = data?.active ?? 'Direct connection';

  if (collapsed) {
    return (
      <Tooltip>
        <TooltipTrigger asChild>
          <span
            tabIndex={0}
            className={cn(
              'flex h-10 items-center justify-center rounded-lg text-muted-foreground outline-none focus-visible:ring-3 focus-visible:ring-ring/50',
              className,
            )}
          >
            <Server className="size-[18px]" aria-hidden="true" />
            <span className="sr-only">Active profile: {activeLabel}. Expand the sidebar to switch.</span>
          </span>
        </TooltipTrigger>
        <TooltipContent side="right">{activeLabel} — expand to switch</TooltipContent>
      </Tooltip>
    );
  }

  return (
    <div className={className}>
      <p className="px-3 pb-1.5 text-[0.6875rem] font-medium tracking-[0.1em] text-muted-foreground uppercase">
        Environment
      </p>
      <Select
        value={data?.active ?? DIRECT_CONNECTION}
        disabled={mutation.isPending}
        onValueChange={(value) => {
          mutation.mutate(value === DIRECT_CONNECTION ? null : value);
        }}
      >
        <SelectTrigger aria-label="Active connection profile" className="h-10 w-full gap-2 rounded-lg">
          <span aria-hidden="true" className="size-2 shrink-0 rounded-full bg-primary" />
          <SelectValue />
        </SelectTrigger>
        <SelectContent>
          <SelectItem value={DIRECT_CONNECTION}>Direct connection</SelectItem>
          {names.map((name) => (
            <SelectItem key={name} value={name}>
              {name}
            </SelectItem>
          ))}
        </SelectContent>
      </Select>
    </div>
  );
}
