import { useQuery } from '@tanstack/react-query';
import { createRoute } from '@tanstack/react-router';
import { PageLayout } from '../components/layout/page';
import { ErrorState } from '../components/ui/error-state';
import { Skeleton } from '../components/ui/skeleton';
import { getShareGroup, shareGroupQueryKeys } from '../lib/groups-api';
import { rootRoute } from './root';

export const shareGroupDetailRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: '/share-groups/$groupId',
  component: ShareGroupDetailPage,
});

/** Read-only, matching the API (§4): share groups (KIP-932) have no reset/delete route in this studio yet. */
function ShareGroupDetailPage() {
  const { groupId } = shareGroupDetailRoute.useParams();

  const { data, isPending, isError, error, refetch } = useQuery({
    queryKey: shareGroupQueryKeys.detail(groupId),
    queryFn: () => getShareGroup(groupId),
  });

  return (
    <PageLayout>
      <section aria-label={`Share group ${groupId}`} className="flex flex-col gap-6">
        {isPending && (
          <div className="flex flex-col gap-6" role="status" aria-busy="true">
            <span className="sr-only">Loading share group…</span>
            <Skeleton className="h-40 w-full rounded-xl" />
            <Skeleton className="h-64 w-full rounded-xl" />
          </div>
        )}
        {isError && (
          <div className="rounded-xl border border-border">
            <ErrorState title="Could not load this group" error={error} onRetry={() => void refetch()} />
          </div>
        )}

        {data && (
          <>
            <div className="rounded-xl border border-border bg-card p-4 text-card-foreground">
              <h2 className="text-sm font-semibold">State: {data.state}</h2>
              <p className="mt-1 text-sm text-muted-foreground">{data.members.length} member(s)</p>
            </div>

            <div className="rounded-xl border border-border bg-card p-4 text-card-foreground">
              <h2 className="text-sm font-semibold">Members</h2>
              <div tabIndex={0} role="region" aria-label="Members table" className="mt-3 overflow-x-auto">
                <table className="w-full text-sm">
                  <thead>
                    <tr className="border-b border-border text-left text-xs text-muted-foreground">
                      <th scope="col" className="px-3 py-2 font-medium">
                        Member
                      </th>
                      <th scope="col" className="px-3 py-2 font-medium">
                        Client
                      </th>
                      <th scope="col" className="px-3 py-2 font-medium">
                        Host
                      </th>
                    </tr>
                  </thead>
                  <tbody>
                    {data.members.map((member) => (
                      <tr key={member.memberId} className="border-b border-border last:border-0">
                        <td className="px-3 py-2 font-mono text-xs">{member.memberId}</td>
                        <td className="px-3 py-2">{member.clientId}</td>
                        <td className="px-3 py-2">{member.clientHost}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>

            <div className="rounded-xl border border-border bg-card p-4 text-card-foreground">
              <h2 className="text-sm font-semibold">Offsets</h2>
              <div tabIndex={0} role="region" aria-label="Offsets table" className="mt-3 overflow-x-auto">
                <table className="w-full text-sm">
                  <thead>
                    <tr className="border-b border-border text-left text-xs text-muted-foreground">
                      <th scope="col" className="px-3 py-2 font-medium">
                        Topic
                      </th>
                      <th scope="col" className="px-3 py-2 font-medium">
                        Partition
                      </th>
                      <th scope="col" className="px-3 py-2 font-medium">
                        Start offset
                      </th>
                      <th scope="col" className="px-3 py-2 font-medium">
                        Lag
                      </th>
                    </tr>
                  </thead>
                  <tbody>
                    {data.offsets.flatMap((topic) =>
                      topic.partitions.map((partition) => (
                        <tr
                          key={`${topic.topic}:${String(partition.partition)}`}
                          className="border-b border-border last:border-0"
                        >
                          <td className="px-3 py-2">{topic.topic}</td>
                          <td className="px-3 py-2">{partition.partition}</td>
                          <td className="px-3 py-2">{partition.startOffset}</td>
                          <td className="px-3 py-2">{partition.lag}</td>
                        </tr>
                      )),
                    )}
                  </tbody>
                </table>
              </div>
            </div>
          </>
        )}
      </section>
    </PageLayout>
  );
}
