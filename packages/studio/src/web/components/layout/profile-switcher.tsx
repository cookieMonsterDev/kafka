import { UserRound } from 'lucide-react';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { Tooltip, TooltipContent, TooltipTrigger } from '../ui/tooltip';

interface ProfilesResponse {
  readonly active: string | null;
  readonly profiles: Readonly<Record<string, unknown>>;
}

async function fetchProfiles(): Promise<ProfilesResponse> {
  const res = await fetch('/api/profiles');
  if (!res.ok) throw new Error(`GET /api/profiles failed with ${String(res.status)}`);
  return (await res.json()) as ProfilesResponse;
}

async function setActiveProfile(profile: string | null): Promise<ProfilesResponse> {
  const res = await fetch('/api/profiles/active', {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ profile }),
  });
  if (!res.ok) throw new Error(`POST /api/profiles/active failed with ${String(res.status)}`);
  return (await res.json()) as ProfilesResponse;
}

/** The empty string stands in for "no profile" in the `<select>` — `null` isn't a valid option value. */
const DIRECT_CONNECTION = '';

export interface ProfileSwitcherProps {
  /** Icon-only sidebar rail: the native `<select>` doesn't fit, so this shows a static hint instead. */
  readonly collapsed?: boolean;
}

/**
 * Lists the profiles configured under `cli.profiles` and lets the operator switch which one the
 * server connects with. Renders nothing when no profiles are configured — there's nothing to
 * switch between yet.
 */
export function ProfileSwitcher({ collapsed = false }: ProfileSwitcherProps) {
  const queryClient = useQueryClient();
  const { data } = useQuery({ queryKey: ['profiles'], queryFn: fetchProfiles });
  const mutation = useMutation({
    mutationFn: setActiveProfile,
    onSuccess: (result) => {
      queryClient.setQueryData(['profiles'], result);
      void queryClient.invalidateQueries({ queryKey: ['cluster'] });
    },
  });

  const names = data ? Object.keys(data.profiles) : [];
  if (names.length === 0) return null;

  const activeLabel = data?.active ?? 'Direct connection';

  if (collapsed) {
    return (
      <Tooltip>
        <TooltipTrigger asChild>
          <span className="flex items-center justify-center py-1 text-muted-foreground">
            <UserRound className="size-4" aria-hidden="true" />
            <span className="sr-only">Active profile: {activeLabel}. Expand the sidebar to switch.</span>
          </span>
        </TooltipTrigger>
        <TooltipContent side="right">{activeLabel} — expand to switch</TooltipContent>
      </Tooltip>
    );
  }

  return (
    <div className="flex flex-col gap-1.5">
      <label htmlFor="profile-switcher" className="text-xs font-medium text-muted-foreground">
        Profile
      </label>
      <select
        id="profile-switcher"
        className="h-8 w-full rounded-lg border border-border bg-background px-2 text-sm outline-none focus-visible:border-ring focus-visible:ring-3 focus-visible:ring-ring/50"
        value={data?.active ?? DIRECT_CONNECTION}
        disabled={mutation.isPending}
        onChange={(event) => {
          const value = event.target.value;
          mutation.mutate(value === DIRECT_CONNECTION ? null : value);
        }}
      >
        <option value={DIRECT_CONNECTION}>Direct connection</option>
        {names.map((name) => (
          <option key={name} value={name}>
            {name}
          </option>
        ))}
      </select>
      {mutation.isError && (
        <p className="text-sm text-destructive" role="alert">
          Could not switch profile.
        </p>
      )}
    </div>
  );
}
