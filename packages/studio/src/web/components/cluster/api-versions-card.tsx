import { useState } from 'react';
import { Server } from 'lucide-react';
import { useQuery } from '@tanstack/react-query';
import { Badge } from '../ui/badge';
import { SelectInput } from '../ui/base-select';
import { Card, CardContent, CardHeader, CardTitle } from '../ui/card';
import { EmptyState } from '../ui/empty-state';
import { QueryBoundary } from '../ui/query-boundary';
import { Skeleton } from '../ui/skeleton';
import { apiVersionsQueryKey, fetchApiVersions } from '../../lib/api';
import { apiVersionStatus, countApiVersionStatuses } from '../../lib/api-version-status';
import type { ApiVersionView, BrokerApiVersionsView } from '../../../shared/contracts/cluster';

function BrokerRange({ versions }: { readonly versions: ApiVersionView['brokerVersions'] }) {
  if (versions === null) return <span className="text-muted-foreground">Not supported</span>;
  if (versions.minVersion === versions.maxVersion) return <>{versions.minVersion}</>;
  return (
    <>
      {versions.minVersion}–{versions.maxVersion}
    </>
  );
}

/** The version Studio uses, or why it uses none; each state is spelled out, not only colored. */
function VersionUsed({ api }: { readonly api: ApiVersionView }) {
  switch (apiVersionStatus(api)) {
    case 'usable':
      return <>{api.version}</>;
    case 'not-implemented':
      return <Badge variant="outline">Not implemented</Badge>;
    case 'unsupported':
      return <span className="text-muted-foreground">None</span>;
    case 'no-common-version':
      return <Badge variant="destructive">No common version</Badge>;
  }
}

function ApiVersionsTable({ broker }: { readonly broker: BrokerApiVersionsView }) {
  return (
    <div
      tabIndex={0}
      role="region"
      aria-label={`API versions of broker ${String(broker.nodeId)}, scroll for more rows and columns`}
      className="max-h-112 overflow-auto rounded-lg border border-border"
    >
      <table className="w-full text-sm">
        <caption className="sr-only">
          Kafka APIs of broker {broker.nodeId}: the versions it supports and the version Studio uses
        </caption>
        <thead className="sticky top-0 z-10 bg-card">
          <tr className="border-b border-border text-left text-xs text-muted-foreground">
            <th scope="col" className="px-3 py-2 font-medium">
              API
            </th>
            <th scope="col" className="px-3 py-2 font-medium">
              Key
            </th>
            <th scope="col" className="px-3 py-2 font-medium">
              Broker versions
            </th>
            <th scope="col" className="px-3 py-2 font-medium">
              Version used
            </th>
          </tr>
        </thead>
        <tbody>
          {broker.apis.map((api) => (
            <tr key={api.apiKey} className="border-b border-border last:border-0">
              <th scope="row" className="px-3 py-2 text-left font-normal whitespace-nowrap" translate="no">
                {api.apiName ?? <span className="text-muted-foreground">Unknown</span>}
              </th>
              <td className="px-3 py-2 tabular-nums">{api.apiKey}</td>
              <td className="px-3 py-2 tabular-nums whitespace-nowrap">
                <BrokerRange versions={api.brokerVersions} />
              </td>
              <td className="px-3 py-2 tabular-nums">
                <VersionUsed api={api} />
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function BrokerApiVersions({ brokers }: { readonly brokers: readonly BrokerApiVersionsView[] }) {
  const [selectedNodeId, setSelectedNodeId] = useState('');
  const broker = brokers.find(({ nodeId }) => String(nodeId) === selectedNodeId) ?? brokers[0];
  if (broker === undefined) return <EmptyState className="py-6" icon={Server} title="No brokers reported" />;

  const counts = countApiVersionStatuses(broker.apis);
  const address = `${broker.host}:${String(broker.port)}`;

  return (
    <div className="flex flex-col gap-3">
      <div className="flex flex-wrap items-center gap-x-4 gap-y-2">
        {brokers.length > 1 && (
          <label className="flex items-center gap-2 text-sm">
            <span className="font-medium">Broker</span>
            <SelectInput
              aria-label="Broker"
              className="w-auto min-w-52"
              value={String(broker.nodeId)}
              onValueChange={setSelectedNodeId}
              items={brokers.map(({ nodeId, host, port }) => ({
                value: String(nodeId),
                label: `${String(nodeId)} · ${host}:${String(port)}`,
              }))}
            />
          </label>
        )}
        <p className="text-sm text-muted-foreground">
          {brokers.length === 1 && (
            <>
              Broker {broker.nodeId} · <span translate="no">{address}</span> ·{' '}
            </>
          )}
          {broker.rack !== null && <>Rack {broker.rack} · </>}
          {counts.usable} usable · {counts['not-implemented']} not implemented by Studio · {counts.unsupported} not
          supported by the broker
          {counts['no-common-version'] > 0 && <> · {counts['no-common-version']} with no common version</>}
        </p>
      </div>
      <ApiVersionsTable key={broker.nodeId} broker={broker} />
    </div>
  );
}

/**
 * Each broker's Kafka APIs with the versions it advertises and the version Studio's client uses
 * (KIP-1220), one broker at a time.
 */
export function ApiVersionsCard() {
  const apiVersions = useQuery({ queryKey: apiVersionsQueryKey, queryFn: fetchApiVersions });

  return (
    <Card>
      <CardHeader>
        <CardTitle>API versions</CardTitle>
      </CardHeader>
      <CardContent>
        <QueryBoundary
          query={apiVersions}
          skeleton={<Skeleton className="h-16 w-full" />}
          errorTitle="Could not load API versions"
        >
          {(data) => <BrokerApiVersions brokers={data.brokers} />}
        </QueryBoundary>
      </CardContent>
    </Card>
  );
}
