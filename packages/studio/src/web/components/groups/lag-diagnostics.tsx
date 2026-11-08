import { useEffect, useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import { getGroup } from '../../lib/groups-api';
import { fetchProfiles, profilesQueryKey } from '../../lib/profiles-api';
import { lagDiagnostics, type LagSample } from '../../lib/debug-analysis';
import { ErrorState } from '../ui/error-state';

export function LagDiagnostics({ groupId }: { readonly groupId: string }) {
  const { data } = useQuery({ queryKey: profilesQueryKey, queryFn: fetchProfiles });
  return (
    <LagSession
      key={JSON.stringify([data?.active ?? null, groupId])}
      profile={data?.active ?? null}
      groupId={groupId}
    />
  );
}

function LagSession({ groupId, profile }: { readonly groupId: string; readonly profile: string | null }) {
  const [samples, setSamples] = useState<readonly LagSample[]>([]);
  const query = useQuery({
    queryKey: ['lag-diagnostics', profile, groupId],
    queryFn: () => getGroup(groupId),
    refetchInterval: 5000,
  });
  useEffect(() => {
    if (!query.data) return;
    const sample = { time: query.dataUpdatedAt, partitions: query.data.partitionLag };
    setSamples((previous) => [...previous.filter((entry) => entry.time !== sample.time), sample].slice(-60));
  }, [query.data, query.dataUpdatedAt]);
  const diagnostics = lagDiagnostics(samples);
  const rate = (value: number | null) => (value === null ? '—' : value.toFixed(1));
  return (
    <section className="rounded-xl border border-border bg-card p-4" aria-label="Consumer lag diagnostics">
      <h2 className="text-sm font-semibold">Lag diagnostics</h2>
      <p className="mt-2 text-xs text-muted-foreground">
        Samples every five seconds while this page is open; last 60 samples kept in memory. Rates measure committed
        offset movement, including possible skips, rather than application processing. Catch-up estimates assume these
        rates continue.
      </p>
      {query.isError && (
        <ErrorState title="Could not sample lag" error={query.error} onRetry={() => void query.refetch()} />
      )}
      <p role="status" aria-live="polite" className="mt-3 text-sm">
        {samples.length < 2 ? 'Collecting samples…' : `${String(samples.length)} samples collected`}
      </p>
      <div
        role="region"
        aria-label="Per-partition lag diagnostics"
        tabIndex={0}
        className="mt-3 overflow-auto focus-visible:ring-2 focus-visible:ring-ring"
      >
        <table className="w-full text-left text-sm">
          <thead>
            <tr>
              {[
                'Topic / partition',
                'Lag (offsets)',
                'Incoming offsets/s',
                'Committed offsets/s',
                'Catch-up (s)',
                'Observation',
              ].map((title) => (
                <th className="p-2" scope="col" key={title}>
                  {title}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {diagnostics.map((entry) => (
              <tr className="border-t border-border" key={`${entry.topic}/${String(entry.partition)}`}>
                <td className="p-2">
                  {entry.topic} / {entry.partition}
                </td>
                <td className="p-2">{entry.lag ?? 'uncommitted'}</td>
                <td className="p-2">{rate(entry.incomingRate)}</td>
                <td className="p-2">{rate(entry.committedRate)}</td>
                <td className="p-2">{rate(entry.catchUpSeconds)}</td>
                <td className="p-2">
                  {[
                    entry.reset ? 'Offset moved backwards; rates unavailable' : '',
                    entry.stalled ? 'No commit progress for at least 30 seconds with lag' : '',
                  ]
                    .filter(Boolean)
                    .join('; ') || 'Observing'}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      <details className="mt-3">
        <summary className="cursor-pointer text-sm">Session history</summary>
        <div
          role="region"
          aria-label="Lag sample history"
          tabIndex={0}
          className="max-h-64 overflow-auto focus-visible:ring-2 focus-visible:ring-ring"
        >
          <table className="w-full text-left text-sm">
            <thead>
              <tr>
                <th className="p-2" scope="col">
                  Sample time
                </th>
                <th className="p-2" scope="col">
                  Known lag (offsets)
                </th>
                <th className="p-2" scope="col">
                  Uncommitted partitions
                </th>
              </tr>
            </thead>
            <tbody>
              {samples.map((sample) => (
                <tr key={sample.time}>
                  <td className="p-2">{new Date(sample.time).toLocaleTimeString()}</td>
                  <td className="p-2">
                    {sample.partitions.reduce((total, entry) => total + BigInt(entry.lag ?? '0'), 0n).toString()}
                  </td>
                  <td className="p-2">{sample.partitions.filter((entry) => entry.lag === null).length}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </details>
    </section>
  );
}
