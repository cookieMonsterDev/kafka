import { useMemo, useState } from 'react';
import { SearchX, Users } from 'lucide-react';
import { useQuery } from '@tanstack/react-query';
import { Link, createRoute } from '@tanstack/react-router';
import { PageLayout } from '../components/layout/page';
import { Button } from '../components/ui/button';
import { EmptyState } from '../components/ui/empty-state';
import { ErrorState } from '../components/ui/error-state';
import { Input } from '../components/ui/input';
import { Skeleton } from '../components/ui/skeleton';
import { cn } from '../lib/utils';
import { groupQueryKeys, listGroups, listShareGroups, shareGroupQueryKeys } from '../lib/groups-api';
import { rootRoute } from './root';

export const groupsRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: '/groups',
  component: GroupsPage,
});

type GroupKind = 'consumer' | 'share';

const KIND_LABEL: Record<GroupKind, string> = { consumer: 'Consumer groups', share: 'Share groups' };
const KIND_ROUTE: Record<GroupKind, '/groups/$groupId' | '/share-groups/$groupId'> = {
  consumer: '/groups/$groupId',
  share: '/share-groups/$groupId',
};

function KindToggle({ kind, onChange }: { readonly kind: GroupKind; readonly onChange: (kind: GroupKind) => void }) {
  return (
    <div role="radiogroup" aria-label="Group protocol" className="flex gap-1 rounded-lg border border-border p-1">
      {(Object.keys(KIND_LABEL) as GroupKind[]).map((value) => (
        <button
          key={value}
          type="button"
          role="radio"
          aria-checked={kind === value}
          onClick={() => onChange(value)}
          className={cn(
            'rounded-md px-3 py-1 text-sm outline-none transition-colors focus-visible:ring-3 focus-visible:ring-ring/50',
            kind === value ? 'bg-accent font-medium text-foreground' : 'text-muted-foreground hover:text-foreground',
          )}
        >
          {KIND_LABEL[value]}
        </button>
      ))}
    </div>
  );
}

function GroupsPage() {
  const [kind, setKind] = useState<GroupKind>('consumer');
  const [search, setSearch] = useState('');

  const consumerQuery = useQuery({
    queryKey: groupQueryKeys.list(),
    queryFn: listGroups,
    enabled: kind === 'consumer',
  });
  const shareQuery = useQuery({
    queryKey: shareGroupQueryKeys.list(),
    queryFn: listShareGroups,
    enabled: kind === 'share',
  });
  const { data, isPending, isError, error, refetch } = kind === 'consumer' ? consumerQuery : shareQuery;

  const filtered = useMemo(() => {
    const groups = data?.groups ?? [];
    const query = search.trim().toLowerCase();
    return query === '' ? groups : groups.filter((group) => group.groupId.toLowerCase().includes(query));
  }, [data, search]);

  const toolbar = (
    <>
      <KindToggle kind={kind} onChange={setKind} />
      <label className="flex min-w-0 flex-1 items-center sm:max-w-xs">
        <span className="sr-only">Filter {KIND_LABEL[kind].toLowerCase()}</span>
        <Input
          type="search"
          placeholder="Filter groups…"
          value={search}
          onChange={(event) => setSearch(event.target.value)}
        />
      </label>
    </>
  );

  return (
    <PageLayout toolbar={toolbar}>
      <section aria-label={KIND_LABEL[kind]} className="flex flex-col gap-4">
        {isPending && (
          <div
            className="flex flex-col gap-1 overflow-hidden rounded-xl border border-border p-2"
            role="status"
            aria-busy="true"
          >
            <span className="sr-only">Loading {KIND_LABEL[kind].toLowerCase()}…</span>
            {[0, 1, 2, 3].map((row) => (
              <Skeleton key={row} className="h-10 w-full" />
            ))}
          </div>
        )}
        {isError && (
          <div className="rounded-xl border border-border">
            <ErrorState
              title={`Could not load ${KIND_LABEL[kind].toLowerCase()}`}
              error={error}
              onRetry={() => void refetch()}
            />
          </div>
        )}

        {data &&
          (filtered.length === 0 ? (
            <div className="rounded-xl border border-border">
              {search.trim() === '' ? (
                <EmptyState
                  icon={Users}
                  title={`No ${KIND_LABEL[kind].toLowerCase()} yet`}
                  description="Nothing is consuming from this cluster right now."
                />
              ) : (
                <EmptyState
                  icon={SearchX}
                  title={`No groups match “${search}”`}
                  action={
                    <Button type="button" variant="outline" size="sm" onClick={() => setSearch('')}>
                      Clear filter
                    </Button>
                  }
                />
              )}
            </div>
          ) : (
            <div className="overflow-hidden rounded-xl border border-border">
              <ul className="divide-y divide-border">
                {filtered.map((group) => (
                  <li key={group.groupId}>
                    <Link
                      to={KIND_ROUTE[kind]}
                      params={{ groupId: group.groupId }}
                      className="flex items-center justify-between gap-3 px-4 py-3 text-sm outline-none hover:bg-muted/40 focus-visible:bg-muted/40"
                    >
                      <span className="min-w-0 truncate font-medium">{group.groupId}</span>
                      <span className="shrink-0 text-xs text-muted-foreground">{group.protocolType}</span>
                    </Link>
                  </li>
                ))}
              </ul>
            </div>
          ))}
      </section>
    </PageLayout>
  );
}
