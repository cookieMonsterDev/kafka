import { useQuery } from '@tanstack/react-query';
import { createRoute } from '@tanstack/react-router';
import { rootRoute } from './root';

interface ClusterStatus {
  readonly connected: boolean;
}

async function fetchClusterStatus(): Promise<ClusterStatus> {
  const res = await fetch('/api/cluster');
  if (!res.ok) throw new Error(`GET /api/cluster failed with ${String(res.status)}`);
  return (await res.json()) as ClusterStatus;
}

export const clusterRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: '/',
  component: ClusterOverview,
});

function ClusterOverview() {
  const { data, isPending, isError } = useQuery({ queryKey: ['cluster'], queryFn: fetchClusterStatus });

  return (
    <section
      aria-labelledby="cluster-overview-heading"
      className="rounded-xl border border-border bg-card p-6 text-card-foreground"
    >
      <h1 id="cluster-overview-heading" className="text-lg font-semibold">
        Cluster overview
      </h1>
      {isPending && (
        <p className="mt-2 text-sm text-muted-foreground" role="status">
          Checking connection…
        </p>
      )}
      {isError && (
        <p className="mt-2 text-sm text-destructive" role="alert">
          Could not reach the studio server.
        </p>
      )}
      {data && !data.connected && (
        <p className="mt-2 text-sm text-muted-foreground">
          Not connected to a Kafka cluster yet. Connection profiles are coming in a later update.
        </p>
      )}
    </section>
  );
}
