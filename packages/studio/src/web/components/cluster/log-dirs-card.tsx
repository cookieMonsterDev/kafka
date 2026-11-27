import { HardDrive } from 'lucide-react';
import { useQuery } from '@tanstack/react-query';
import { Badge } from '../ui/badge';
import { Card, CardContent, CardHeader, CardTitle } from '../ui/card';
import { EmptyState } from '../ui/empty-state';
import { QueryBoundary } from '../ui/query-boundary';
import { Skeleton } from '../ui/skeleton';
import { fetchLogDirs, logDirsQueryKey } from '../../lib/api';
import { logDirUsedPercent } from '../../lib/log-dir-usage';
import { formatBytes } from '../../lib/utils';
import type { LogDirView } from '../../../shared/contracts/cluster';

/** Used space as text, with a bar that repeats it visually; the text alone carries the value. */
function UsedSpace({ logDir }: { readonly logDir: LogDirView }) {
  const percent = logDirUsedPercent(logDir);
  if (percent === null) return <span className="text-muted-foreground">Not reported</span>;

  return (
    <div className="flex min-w-40 flex-col gap-1">
      <span className="tabular-nums">
        {formatBytes(logDir.usedBytes)} of {formatBytes(logDir.totalBytes)} ({percent}%)
      </span>
      <span aria-hidden="true" className="h-1.5 w-full overflow-hidden rounded-full bg-muted">
        <span className="block h-full rounded-full bg-primary" style={{ width: `${String(percent)}%` }} />
      </span>
    </div>
  );
}

function CordonedCell({ isCordoned }: { readonly isCordoned: boolean | null }) {
  if (isCordoned === null) return <span className="text-muted-foreground">Not reported</span>;
  return isCordoned ? <Badge variant="accent">Cordoned</Badge> : <span className="text-muted-foreground">No</span>;
}

function LogDirsTable({ logDirs }: { readonly logDirs: readonly LogDirView[] }) {
  if (logDirs.length === 0) {
    return <EmptyState className="py-6" icon={HardDrive} title="No log directories reported" />;
  }

  return (
    <div
      tabIndex={0}
      role="region"
      aria-label="Log directories table, scroll horizontally for more columns"
      className="overflow-x-auto"
    >
      <table className="w-full text-sm">
        <thead>
          <tr className="border-b border-border text-left text-xs text-muted-foreground">
            <th scope="col" className="px-3 py-2 font-medium">
              Broker
            </th>
            <th scope="col" className="px-3 py-2 font-medium">
              Path
            </th>
            <th scope="col" className="px-3 py-2 font-medium">
              Used
            </th>
            <th scope="col" className="px-3 py-2 font-medium">
              Usable
            </th>
            <th scope="col" className="px-3 py-2 font-medium">
              Cordoned
            </th>
          </tr>
        </thead>
        <tbody>
          {logDirs.map((logDir) => (
            <tr key={`${String(logDir.brokerId)}:${logDir.path}`} className="border-b border-border last:border-0">
              <td className="px-3 py-2 tabular-nums">{logDir.brokerId}</td>
              <td className="px-3 py-2 font-mono text-xs whitespace-nowrap">{logDir.path}</td>
              <td className="px-3 py-2">
                <UsedSpace logDir={logDir} />
              </td>
              <td className="px-3 py-2 tabular-nums">{formatBytes(logDir.usableBytes)}</td>
              <td className="px-3 py-2">
                <CordonedCell isCordoned={logDir.isCordoned} />
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

/** Every broker's log dirs with their volume usage and whether new replicas are kept off them. */
export function LogDirsCard() {
  const logDirs = useQuery({ queryKey: logDirsQueryKey, queryFn: fetchLogDirs });

  return (
    <Card>
      <CardHeader>
        <CardTitle>Log directories</CardTitle>
        {logDirs.data !== undefined && <Badge variant="outline">{logDirs.data.logDirs.length}</Badge>}
      </CardHeader>
      <CardContent>
        <QueryBoundary
          query={logDirs}
          skeleton={<Skeleton className="h-16 w-full" />}
          errorTitle="Could not load log directories"
        >
          {(data) => <LogDirsTable logDirs={data.logDirs} />}
        </QueryBoundary>
      </CardContent>
    </Card>
  );
}
