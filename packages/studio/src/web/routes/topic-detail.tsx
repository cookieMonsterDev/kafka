import { useId, useState } from 'react';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { createRoute, useNavigate } from '@tanstack/react-router';
import { ConfigEditor } from '../components/topics/config-editor';
import { Button } from '../components/ui/button';
import { ConfirmDialog } from '../components/ui/confirm-dialog';
import { Input } from '../components/ui/input';
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

  const { data, isPending, isError } = useQuery({
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
    <section aria-labelledby="topic-detail-heading" className="flex flex-col gap-6">
      <div className="flex items-center justify-between gap-3">
        <h1 id="topic-detail-heading" className="text-lg font-semibold break-all">
          {name}
        </h1>
        <ConfirmDialog
          trigger={
            <Button type="button" variant="destructive">
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

      {isPending && (
        <p className="text-sm text-muted-foreground" role="status">
          Loading topic…
        </p>
      )}
      {isError && (
        <p className="text-sm text-destructive" role="alert">
          Could not load this topic. It may not exist.
        </p>
      )}
      {deleteMutation.isError && (
        <p className="text-sm text-destructive" role="alert">
          {deleteMutation.error.message}
        </p>
      )}

      {data && (
        <>
          <div className="rounded-xl border border-border bg-card p-4 text-card-foreground">
            <h2 className="text-sm font-semibold">Partitions</h2>
            <div className="mt-3 overflow-x-auto">
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
                    <tr className="text-xs text-muted-foreground">
                      <td colSpan={5} className="px-3 py-2 text-right font-medium">
                        Total
                      </td>
                      <td className="px-3 py-2 font-medium">
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
        </>
      )}
    </section>
  );
}
