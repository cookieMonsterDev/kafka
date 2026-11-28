import { useQuery } from '@tanstack/react-query';
import { createLazyRoute } from '@tanstack/react-router';
import type { StreamsGroupTasks } from '../../shared/contracts/group';
import { PageLayout } from '../components/layout/page';
import { ErrorState } from '../components/ui/error-state';
import { Skeleton } from '../components/ui/skeleton';
import { getStreamsGroup, streamsGroupQueryKeys } from '../lib/groups-api';

export const Route = createLazyRoute('/streams-groups/$groupId')({
  component: StreamsGroupDetailPage,
});

/** Tasks in Kafka Streams' own `<subtopology>_<partition>` notation, such as `0_1`. */
function formatTasks(tasks: readonly StreamsGroupTasks[]): string {
  const taskIds = tasks.flatMap((task) => task.partitions.map((partition) => `${task.subtopologyId}_${partition}`));
  return taskIds.length === 0 ? '—' : taskIds.join(', ');
}

function formatTopics(topics: readonly string[]): string {
  return topics.length === 0 ? '—' : topics.join(', ');
}

function HeaderCell({ children }: { readonly children: string }) {
  return (
    <th scope="col" className="px-3 py-2 font-medium">
      {children}
    </th>
  );
}

/** Read-only, like share groups: the studio only inspects streams groups (KIP-1071), which a Kafka Streams runtime joins. */
function StreamsGroupDetailPage() {
  const { groupId } = Route.useParams();

  const { data, isPending, isError, error, refetch } = useQuery({
    queryKey: streamsGroupQueryKeys.detail(groupId),
    queryFn: () => getStreamsGroup(groupId),
  });

  return (
    <PageLayout>
      <section aria-label={`Streams group ${groupId}`} className="flex flex-col gap-6">
        {isPending && (
          <div className="flex flex-col gap-6" role="status" aria-busy="true">
            <span className="sr-only">Loading streams group…</span>
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
              <dl className="mt-3 grid grid-cols-2 gap-3 text-sm sm:grid-cols-4">
                <div>
                  <dt className="text-xs text-muted-foreground">Group epoch</dt>
                  <dd>{data.groupEpoch}</dd>
                </div>
                <div>
                  <dt className="text-xs text-muted-foreground">Topology epoch</dt>
                  <dd>{data.topologyEpoch ?? '—'}</dd>
                </div>
                <div>
                  <dt className="text-xs text-muted-foreground">Assignor</dt>
                  <dd>{data.assignorName ?? '—'}</dd>
                </div>
                <div>
                  <dt className="text-xs text-muted-foreground">Members</dt>
                  <dd>{data.members.length}</dd>
                </div>
              </dl>
            </div>

            <div className="rounded-xl border border-border bg-card p-4 text-card-foreground">
              <h2 className="text-sm font-semibold">Topology</h2>
              {data.subtopologies === null ? (
                <p className="mt-3 text-sm text-muted-foreground">
                  Not ready: the group has no topology yet, or its source topics are missing or partitioned differently
                  than the topology needs.
                </p>
              ) : (
                <div tabIndex={0} role="region" aria-label="Subtopologies table" className="mt-3 overflow-x-auto">
                  <table className="w-full text-sm">
                    <thead>
                      <tr className="border-b border-border text-left text-xs text-muted-foreground">
                        <HeaderCell>Subtopology</HeaderCell>
                        <HeaderCell>Source topics</HeaderCell>
                        <HeaderCell>Repartition topics</HeaderCell>
                        <HeaderCell>Changelog topics</HeaderCell>
                      </tr>
                    </thead>
                    <tbody>
                      {data.subtopologies.map((subtopology) => (
                        <tr key={subtopology.subtopologyId} className="border-b border-border last:border-0">
                          <td className="px-3 py-2 font-mono text-xs">{subtopology.subtopologyId}</td>
                          <td className="px-3 py-2">
                            {formatTopics([...subtopology.sourceTopics, ...subtopology.repartitionSourceTopics])}
                          </td>
                          <td className="px-3 py-2">{formatTopics(subtopology.repartitionSinkTopics)}</td>
                          <td className="px-3 py-2">{formatTopics(subtopology.stateChangelogTopics)}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              )}
            </div>

            <div className="rounded-xl border border-border bg-card p-4 text-card-foreground">
              <h2 className="text-sm font-semibold">Members</h2>
              <div tabIndex={0} role="region" aria-label="Members table" className="mt-3 overflow-x-auto">
                <table className="w-full text-sm">
                  <thead>
                    <tr className="border-b border-border text-left text-xs text-muted-foreground">
                      <HeaderCell>Member</HeaderCell>
                      <HeaderCell>Process</HeaderCell>
                      <HeaderCell>Client</HeaderCell>
                      <HeaderCell>Host</HeaderCell>
                      <HeaderCell>Active tasks</HeaderCell>
                      <HeaderCell>Standby tasks</HeaderCell>
                      <HeaderCell>Warm-up tasks</HeaderCell>
                    </tr>
                  </thead>
                  <tbody>
                    {data.members.map((member) => (
                      <tr key={member.memberId} className="border-b border-border last:border-0">
                        <td className="px-3 py-2 font-mono text-xs">{member.memberId}</td>
                        <td className="px-3 py-2 font-mono text-xs">{member.processId}</td>
                        <td className="px-3 py-2">{member.clientId}</td>
                        <td className="px-3 py-2">{member.clientHost}</td>
                        <td className="px-3 py-2 font-mono text-xs">{formatTasks(member.activeTasks)}</td>
                        <td className="px-3 py-2 font-mono text-xs">{formatTasks(member.standbyTasks)}</td>
                        <td className="px-3 py-2 font-mono text-xs">{formatTasks(member.warmupTasks)}</td>
                      </tr>
                    ))}
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
