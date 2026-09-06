import { Gauge } from 'lucide-react';
import { useQuery } from '@tanstack/react-query';
import { EmptyState } from '../ui/empty-state';
import { ErrorState } from '../ui/error-state';
import { listQuotas, settingsQueryKeys } from '../../lib/settings-api';
import { LoadingRows, TableShell } from './table-shell';

export function QuotaTable() {
  const { data, isPending, isError, error, refetch } = useQuery({
    queryKey: settingsQueryKeys.quotas,
    queryFn: listQuotas,
  });

  if (isPending) return <LoadingRows />;
  if (isError) return <ErrorState title="Could not load quotas" error={error} onRetry={() => void refetch()} />;
  if (data.quotas.length === 0) {
    return (
      <EmptyState icon={Gauge} title="No client quotas set" description="Every client uses the broker defaults." />
    );
  }

  return (
    <TableShell label="Quotas">
      <thead>
        <tr className="border-b border-border text-left text-xs text-muted-foreground">
          <th scope="col" className="px-3 py-2 font-medium">
            Entity
          </th>
          <th scope="col" className="px-3 py-2 font-medium">
            Key
          </th>
          <th scope="col" className="px-3 py-2 font-medium">
            Value
          </th>
        </tr>
      </thead>
      <tbody>
        {data.quotas.flatMap((entry, entryIndex) => {
          const entityLabel =
            entry.entity.length === 0
              ? '(cluster default)'
              : entry.entity
                  .map((component) => `${component.entityType}=${component.entityName ?? '(default)'}`)
                  .join(', ');
          if (entry.values.length === 0) {
            return (
              <tr key={entryIndex} className="border-b border-border last:border-0">
                <td className="px-3 py-2">{entityLabel}</td>
                <td className="px-3 py-2 text-muted-foreground" colSpan={2}>
                  (none)
                </td>
              </tr>
            );
          }
          return entry.values.map((value, valueIndex) => (
            <tr key={`${String(entryIndex)}:${value.key}`} className="border-b border-border last:border-0">
              {valueIndex === 0 && (
                <td className="px-3 py-2 align-top" rowSpan={entry.values.length}>
                  {entityLabel}
                </td>
              )}
              <td className="px-3 py-2">{value.key}</td>
              <td className="px-3 py-2">{value.value}</td>
            </tr>
          ));
        })}
      </tbody>
    </TableShell>
  );
}
