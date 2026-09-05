import { useState } from 'react';
import { Trash2 } from 'lucide-react';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { createRoute, useNavigate } from '@tanstack/react-router';
import type { GroupOffsetResetTarget } from '../../shared/contracts/group';
import { PageLayout } from '../components/layout/page';
import { ResetOffsetsDialog } from '../components/groups/reset-dialog';
import { Button } from '../components/ui/button';
import { ConfirmDialog } from '../components/ui/confirm-dialog';
import { ErrorState } from '../components/ui/error-state';
import { Skeleton } from '../components/ui/skeleton';
import { deleteGroup, getGroup, groupQueryKeys, resetGroupOffsets } from '../lib/groups-api';
import { rootRoute } from './root';

export const groupDetailRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: '/groups/$groupId',
  component: GroupDetailPage,
});

function GroupDetailPage() {
  const { groupId } = groupDetailRoute.useParams();
  const navigate = useNavigate();
  const queryClient = useQueryClient();
  const [resetOpen, setResetOpen] = useState(false);

  const { data, isPending, isError, error, refetch } = useQuery({
    queryKey: groupQueryKeys.detail(groupId),
    queryFn: () => getGroup(groupId),
  });

  const resetMutation = useMutation({
    mutationFn: ({ topic, partitions }: { topic: string; partitions: readonly GroupOffsetResetTarget[] }) =>
      resetGroupOffsets(groupId, { topic, partitions: [...partitions] }),
    onSuccess: async () => {
      setResetOpen(false);
      await queryClient.invalidateQueries({ queryKey: groupQueryKeys.detail(groupId) });
    },
  });

  const deleteMutation = useMutation({
    mutationFn: () => deleteGroup(groupId),
    onSuccess: async () => {
      await queryClient.invalidateQueries({ queryKey: groupQueryKeys.list() });
      void navigate({ to: '/groups' });
    },
  });

  return (
    <PageLayout>
      <section aria-label={`Consumer group ${groupId}`} className="flex flex-col gap-6">
        {isPending && (
          <div className="flex flex-col gap-6" role="status" aria-busy="true">
            <span className="sr-only">Loading consumer group…</span>
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
              <div className="flex flex-wrap items-center justify-between gap-3">
                <h2 className="text-sm font-semibold">State: {data.state}</h2>
                <Button type="button" variant="outline" size="sm" onClick={() => setResetOpen(true)}>
                  Reset offsets
                </Button>
              </div>
              <dl className="mt-3 grid grid-cols-2 gap-3 text-sm sm:grid-cols-4">
                <div>
                  <dt className="text-xs text-muted-foreground">Protocol</dt>
                  <dd>{data.protocolType}</dd>
                </div>
                <div>
                  <dt className="text-xs text-muted-foreground">Assignor</dt>
                  <dd>{data.assignorName || '—'}</dd>
                </div>
                <div>
                  <dt className="text-xs text-muted-foreground">Members</dt>
                  <dd>{data.members.length}</dd>
                </div>
              </dl>
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
                      <th scope="col" className="px-3 py-2 font-medium">
                        Assigned partitions
                      </th>
                    </tr>
                  </thead>
                  <tbody>
                    {data.members.map((member) => (
                      <tr key={member.memberId} className="border-b border-border last:border-0">
                        <td className="px-3 py-2 font-mono text-xs">{member.memberId}</td>
                        <td className="px-3 py-2">{member.clientId}</td>
                        <td className="px-3 py-2">{member.clientHost}</td>
                        <td className="px-3 py-2">
                          {member.assignedTopicPartitions
                            .map((entry) => `${entry.topic} [${entry.partitions.join(', ')}]`)
                            .join('; ') || '—'}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>

            <div className="rounded-xl border border-border bg-card p-4 text-card-foreground">
              <h2 className="text-sm font-semibold">Partition lag</h2>
              <div tabIndex={0} role="region" aria-label="Partition lag table" className="mt-3 overflow-x-auto">
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
                        Committed
                      </th>
                      <th scope="col" className="px-3 py-2 font-medium">
                        Log end
                      </th>
                      <th scope="col" className="px-3 py-2 font-medium">
                        Lag
                      </th>
                    </tr>
                  </thead>
                  <tbody>
                    {data.partitionLag.map((row) => (
                      <tr
                        key={`${row.topic}:${String(row.partition)}`}
                        className="border-b border-border last:border-0"
                      >
                        <td className="px-3 py-2">{row.topic}</td>
                        <td className="px-3 py-2">{row.partition}</td>
                        <td className="px-3 py-2">{row.committedOffset ?? '—'}</td>
                        <td className="px-3 py-2">{row.logEndOffset}</td>
                        <td className="px-3 py-2">{row.lag ?? '—'}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>

            <div className="rounded-xl border border-destructive/30 bg-card p-4 text-card-foreground">
              <h2 className="text-sm font-semibold">Delete this group</h2>
              <p className="mt-1 text-sm text-muted-foreground">
                Permanently deletes <span className="font-medium text-foreground">{groupId}</span> and its committed
                offsets. This cannot be undone.
              </p>
              <div className="mt-3">
                <ConfirmDialog
                  trigger={
                    <Button type="button" variant="destructive">
                      <Trash2 className="size-4" aria-hidden="true" />
                      Delete group
                    </Button>
                  }
                  title={`Delete "${groupId}"?`}
                  description="This permanently deletes the group and its committed offsets. This cannot be undone."
                  confirmValue={groupId}
                  confirmLabel="Delete group"
                  pending={deleteMutation.isPending}
                  onConfirm={() => deleteMutation.mutate()}
                />
              </div>
              {deleteMutation.isError && (
                <p className="mt-2 text-sm text-destructive" role="alert">
                  {deleteMutation.error.message}
                </p>
              )}
            </div>

            <ResetOffsetsDialog
              open={resetOpen}
              onOpenChange={setResetOpen}
              partitionLag={data.partitionLag}
              pending={resetMutation.isPending}
              onConfirm={(topic, partitions) => resetMutation.mutate({ topic, partitions })}
            />
            {resetMutation.isError && (
              <p className="text-sm text-destructive" role="alert">
                {resetMutation.error.message}
              </p>
            )}
          </>
        )}
      </section>
    </PageLayout>
  );
}
