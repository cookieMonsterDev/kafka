import { useId, useState } from 'react';
import { Trash2 } from 'lucide-react';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { createRoute, useNavigate } from '@tanstack/react-router';
import { PageLayout } from '../components/layout/page';
import { ConfigEditor } from '../components/topics/config-editor';
import { Button } from '../components/ui/button';
import { ConfirmDialog } from '../components/ui/confirm-dialog';
import { ErrorState } from '../components/ui/error-state';
import { Input } from '../components/ui/input';
import { Skeleton } from '../components/ui/skeleton';
import { addPartitions, alterTopicConfigs, deleteTopic, getTopic, topicQueryKeys } from '../lib/topics-api';
import { formatBytes } from '../lib/utils';
import { rootRoute } from './root';

export const topicDetailRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: '/topics/$name',
  component: TopicDetailPage,
});

function AddPartitionsControl({ topic, currentCount }: { readonly topic: string; readonly currentCount: number }) {
  const queryClient = useQueryClient();
  const inputId = useId();
  const [count, setCount] = useState(String(currentCount));

  const mutation = useMutation({
    mutationFn: (nextCount: number) => addPartitions(topic, { count: nextCount }),
    onSuccess: async () => {
      await queryClient.invalidateQueries({ queryKey: topicQueryKeys.detail(topic) });
      await queryClient.invalidateQueries({ queryKey: topicQueryKeys.list() });
    },
  });

  const parsed = Number(count);
  const isValid = Number.isInteger(parsed) && parsed > currentCount;

  return (
    <form
      className="flex items-end gap-2"
      onSubmit={(event) => {
        event.preventDefault();
        if (isValid) mutation.mutate(parsed);
      }}
    >
      <div className="flex flex-col gap-1.5">
        <label htmlFor={inputId} className="text-sm font-medium">
          New partition count
        </label>
        <Input
          id={inputId}
          type="number"
          min={currentCount + 1}
          step={1}
          value={count}
          onChange={(event) => setCount(event.target.value)}
          className="w-32"
        />
      </div>
      <Button type="submit" variant="outline" disabled={!isValid || mutation.isPending}>
        {mutation.isPending ? 'Raising…' : 'Add partitions'}
      </Button>
      {mutation.isError && (
        <p className="text-sm text-destructive" role="alert">
          {mutation.error.message}
        </p>
      )}
    </form>
  );
}

function TopicDetailPage() {
  const { name } = topicDetailRoute.useParams();
  const navigate = useNavigate();
  const queryClient = useQueryClient();

  const { data, isPending, isError, error, refetch } = useQuery({
    queryKey: topicQueryKeys.detail(name),
    queryFn: () => getTopic(name),
  });

  const configMutation = useMutation({
    mutationFn: (input: Parameters<typeof alterTopicConfigs>[1]) => alterTopicConfigs(name, input),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: topicQueryKeys.detail(name) }),
  });

  const deleteMutation = useMutation({
    mutationFn: () => deleteTopic(name),
    onSuccess: async () => {
      await queryClient.invalidateQueries({ queryKey: topicQueryKeys.list() });
      void navigate({ to: '/topics' });
    },
  });

  return (
    <PageLayout>
      <section aria-label={`Topic ${name}`} className="flex flex-col gap-6">
        {isPending && (
          <div className="flex flex-col gap-6" role="status" aria-busy="true">
            <span className="sr-only">Loading topic…</span>
            <Skeleton className="h-56 w-full rounded-xl" />
            <Skeleton className="h-72 w-full rounded-xl" />
          </div>
        )}
        {isError && (
          <div className="rounded-xl border border-border">
            <ErrorState title="Could not load this topic" error={error} onRetry={() => void refetch()} />
          </div>
        )}

        {data && (
          <>
            <div className="rounded-xl border border-border bg-card p-4 text-card-foreground">
              <h2 className="text-sm font-semibold">Partitions</h2>
              <div
                tabIndex={0}
                role="region"
                aria-label="Partitions table, scroll horizontally for more columns"
                className="mt-3 overflow-x-auto"
              >
                <table className="w-full text-sm">
                  <thead>
                    <tr className="border-b border-border text-left text-xs text-muted-foreground">
                      <th scope="col" className="px-3 py-2 font-medium">
                        Partition
                      </th>
                      <th scope="col" className="px-3 py-2 font-medium">
                        Leader
                      </th>
                      <th scope="col" className="px-3 py-2 font-medium">
                        Replicas
                      </th>
                      <th scope="col" className="px-3 py-2 font-medium">
                        ISR
                      </th>
                      <th scope="col" className="px-3 py-2 font-medium">
                        Earliest
                      </th>
                      <th scope="col" className="px-3 py-2 font-medium">
                        Latest
                      </th>
                      <th scope="col" className="px-3 py-2 font-medium">
                        Size
                      </th>
                    </tr>
                  </thead>
                  <tbody>
                    {data.partitions.map((partition) => (
                      <tr key={partition.partitionIndex} className="border-b border-border last:border-0">
                        <td className="px-3 py-2">{partition.partitionIndex}</td>
                        <td className="px-3 py-2">{partition.leader}</td>
                        <td className="px-3 py-2">{partition.replicas.join(', ')}</td>
                        <td className="px-3 py-2">{partition.isr.join(', ')}</td>
                        <td className="px-3 py-2">{partition.earliestOffset ?? '—'}</td>
                        <td className="px-3 py-2">{partition.latestOffset ?? '—'}</td>
                        <td className="px-3 py-2">{formatBytes(partition.sizeBytes)}</td>
                      </tr>
                    ))}
                  </tbody>
                  {data.partitions.some((partition) => partition.sizeBytes !== null) && (
                    <tfoot>
                      <tr className="border-t border-border text-xs text-muted-foreground">
                        <td colSpan={6} className="px-3 py-2 text-right font-medium">
                          Total
                        </td>
                        <td className="px-3 py-2 font-medium text-foreground">
                          {formatBytes(
                            String(
                              data.partitions.reduce(
                                (total, partition) => total + BigInt(partition.sizeBytes ?? '0'),
                                0n,
                              ),
                            ),
                          )}
                        </td>
                      </tr>
                    </tfoot>
                  )}
                </table>
              </div>
              <div className="mt-4">
                <AddPartitionsControl topic={name} currentCount={data.partitions.length} />
              </div>
            </div>

            <div className="rounded-xl border border-border bg-card p-4 text-card-foreground">
              <h2 className="text-sm font-semibold">Configuration</h2>
              <div className="mt-3">
                <ConfigEditor
                  configs={data.configs}
                  pending={configMutation.isPending}
                  onSave={(input) => configMutation.mutate(input)}
                />
              </div>
              {configMutation.isError && (
                <p className="mt-2 text-sm text-destructive" role="alert">
                  {configMutation.error.message}
                </p>
              )}
            </div>

            {/*
              Destroying the topic belongs with the topic, at the end of it — not in the page
              toolbar, where it sat next to page-level controls and read as an ambient action.
            */}
            <div className="rounded-xl border border-destructive/30 bg-card p-4 text-card-foreground">
              <h2 className="text-sm font-semibold">Delete this topic</h2>
              <p className="mt-1 text-sm text-muted-foreground">
                Permanently deletes <span className="font-medium text-foreground">{name}</span> and every message in it.
                This cannot be undone.
              </p>
              <div className="mt-3">
                <ConfirmDialog
                  trigger={
                    <Button type="button" variant="destructive">
                      <Trash2 className="size-4" aria-hidden="true" />
                      Delete topic
                    </Button>
                  }
                  title={`Delete "${name}"?`}
                  description="This permanently deletes the topic and every message in it. This cannot be undone."
                  confirmValue={name}
                  confirmLabel="Delete topic"
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
          </>
        )}
      </section>
    </PageLayout>
  );
}
