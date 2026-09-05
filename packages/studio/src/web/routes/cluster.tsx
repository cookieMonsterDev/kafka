import { CircleAlert, Layers, Plug } from 'lucide-react';
import { useQuery } from '@tanstack/react-query';
import { Link, createRoute } from '@tanstack/react-router';
import { PageLayout } from '../components/layout/page';
import { Badge } from '../components/ui/badge';
import { Card, CardContent, CardHeader, CardTitle } from '../components/ui/card';
import { EmptyState } from '../components/ui/empty-state';
import { ErrorState } from '../components/ui/error-state';
import { Skeleton } from '../components/ui/skeleton';
import { clusterQueryKey, fetchClusterStatus, fetchHealth, healthQueryKey } from '../lib/api';
import { listTopics, topicQueryKeys } from '../lib/topics-api';
import { topicAccentClass } from '../lib/topic-accent';
import { rootRoute } from './root';

export const clusterRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: '/',
  component: ClusterOverview,
});

function formatUptime(seconds: number): string {
  if (seconds < 60) return `${String(seconds)}s`;
  const minutes = Math.floor(seconds / 60);
  if (minutes < 60) return `${String(minutes)}m`;
  const hours = Math.floor(minutes / 60);
  return hours < 24 ? `${String(hours)}h ${String(minutes % 60)}m` : `${String(Math.floor(hours / 24))}d`;
}

/** A label/value tile. Renders a shimmer of the same height while the value is loading. */
function Stat({
  label,
  value,
  pending,
}: {
  readonly label: string;
  readonly value: string;
  readonly pending: boolean;
}) {
  return (
    <div className="rounded-xl border border-border bg-card p-3">
      <p className="text-[0.6875rem] font-medium tracking-[0.1em] text-muted-foreground uppercase">{label}</p>
      {pending ? (
        <Skeleton className="mt-1.5 h-6 w-20" />
      ) : (
        <p className="mt-1 truncate text-xl leading-tight font-semibold tabular-nums">{value}</p>
      )}
    </div>
  );
}

function TopicsRail() {
  const topics = useQuery({ queryKey: topicQueryKeys.list(), queryFn: listTopics });

  return (
    <Card>
      <CardHeader className="pb-3">
        <CardTitle>Topics</CardTitle>
        {topics.data !== undefined && <Badge variant="outline">{topics.data.topics.length}</Badge>}
      </CardHeader>
      <CardContent className="px-2 pb-2">
        {topics.isPending && (
          <div className="flex flex-col gap-1 p-2" role="status" aria-busy="true">
            <span className="sr-only">Loading topics…</span>
            {[0, 1, 2, 3].map((row) => (
              <Skeleton key={row} className="h-9 w-full" />
            ))}
          </div>
        )}
        {topics.isError && (
          <ErrorState
            className="py-6"
            title="Could not load topics"
            error={topics.error}
            onRetry={() => void topics.refetch()}
          />
        )}
        {topics.data?.topics.length === 0 && <EmptyState className="py-6" icon={Layers} title="No topics yet" />}
        {topics.data !== undefined && topics.data.topics.length > 0 && (
          <ul className="flex flex-col">
            {topics.data.topics.slice(0, 12).map((topic) => (
              <li key={topic.name}>
                <Link
                  to="/topics/$name"
                  params={{ name: topic.name }}
                  className="flex items-center gap-2.5 rounded-lg px-2 py-2 outline-none hover:bg-muted/50 focus-visible:ring-3 focus-visible:ring-ring/50"
                >
                  <span aria-hidden="true" className={`size-2 shrink-0 rounded-full ${topicAccentClass(topic.name)}`} />
                  <span className="min-w-0 flex-1">
                    <span className="block truncate text-sm">{topic.name}</span>
                    <span className="block text-xs text-muted-foreground">
                      {topic.partitionCount} partition{topic.partitionCount === 1 ? '' : 's'}
                    </span>
                  </span>
                </Link>
              </li>
            ))}
          </ul>
        )}
      </CardContent>
    </Card>
  );
}

function ClusterOverview() {
  const cluster = useQuery({ queryKey: clusterQueryKey, queryFn: fetchClusterStatus });
  const health = useQuery({ queryKey: healthQueryKey, queryFn: fetchHealth });
  const topics = useQuery({ queryKey: topicQueryKeys.list(), queryFn: listTopics });

  const partitionTotal = topics.data?.topics.reduce((total, topic) => total + topic.partitionCount, 0);

  return (
    <PageLayout railLabel="Cluster details" rail={<TopicsRail />}>
      <div className="flex flex-col gap-4">
        <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-3">
          <Stat
            label="Topics"
            value={topics.data === undefined ? '—' : String(topics.data.topics.length)}
            pending={topics.isPending}
          />
          <Stat
            label="Partitions"
            value={partitionTotal === undefined ? '—' : String(partitionTotal)}
            pending={topics.isPending}
          />
          <Stat
            label="Uptime"
            value={health.data === undefined ? '—' : formatUptime(health.data.uptimeSeconds)}
            pending={health.isPending}
          />
        </div>

        <Card>
          <CardHeader>
            <CardTitle>Cluster</CardTitle>
            {health.data?.readOnly === true && <Badge variant="accent">Read-only</Badge>}
          </CardHeader>
          <CardContent>
            {cluster.isError ? (
              <ErrorState
                className="py-6"
                title="Could not reach the studio server"
                error={cluster.error}
                onRetry={() => void cluster.refetch()}
              />
            ) : cluster.isPending ? (
              <div role="status" aria-busy="true">
                <span className="sr-only">Checking connection…</span>
                <Skeleton className="h-16 w-full" />
              </div>
            ) : cluster.data.connected ? (
              <p className="text-sm text-muted-foreground">Connected to a Kafka cluster.</p>
            ) : (
              <EmptyState
                className="py-6"
                icon={Plug}
                title="Not connected to a Kafka cluster"
                description="The studio server is running, but no broker connection is configured yet. Connection profiles arrive in a later update."
              />
            )}
          </CardContent>
        </Card>

        {/* A topics failure is reported once, by the rail that owns the topic list — repeating it
            here would put the same message on screen twice. */}

        {health.isError && (
          <p className="flex items-center gap-2 text-sm text-destructive" role="alert">
            <CircleAlert className="size-4 shrink-0" aria-hidden="true" />
            Studio health is unavailable — the server may have stopped.
          </p>
        )}
      </div>
    </PageLayout>
  );
}
