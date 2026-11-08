import { formatLocalDateTime } from '../lib/local-date-time';
import { DateTimePicker } from '../components/ui/date-time-picker';
import { SelectInput } from '../components/ui/base-select';
import { Field, Output } from '../components/debug/fields';
import { ComparePanel } from '../components/debug/compare-panel';
import { ScenarioPanel } from '../components/debug/scenario-panel';
import { SchemaPanel } from '../components/debug/schema-panel';
import { ReplayPanel } from '../components/debug/replay-panel';
import { useEffect, useRef, useState } from 'react';
import { createLazyRoute } from '@tanstack/react-router';
import { useQuery } from '@tanstack/react-query';
import {
  scanRequestSchema,
  type DebugRecord,
  type ScanResult,
  type MessagePredicate,
} from '../../shared/contracts/debug';
import { parseMessageQuery, correlationPredicates } from '../../shared/message-query';
import { PageLayout } from '../components/layout/page';
import { Button } from '../components/ui/button';
import { Input } from '../components/ui/input';
import { apiSend, fetchHealth, healthQueryKey } from '../lib/api';
import { decodeMessageField } from '../lib/decode';
import { compactedState, sampledKeys } from '../lib/debug-analysis';
import { streamDebug, downloadJson } from '../lib/debug-api';
import { fetchProfiles, profilesQueryKey } from '../lib/profiles-api';

export const Route = createLazyRoute('/debug')({ component: DebugPage });
const MODES = ['Search', 'Correlate', 'Replay', 'State', 'Partitions', 'Schemas', 'Scenarios', 'Compare'] as const;
type Mode = (typeof MODES)[number];
const text = (value: string | null) => (value === null ? 'null' : decodeMessageField(value, 'utf8').text);
const recordId = (record: DebugRecord) => JSON.stringify([record.topic, record.partition, record.offset]);

function DebugPage() {
  const { data: profiles } = useQuery({ queryKey: profilesQueryKey, queryFn: fetchProfiles });
  // Profile switches remount all drafts and cancel in-flight requests before their results can be reused.
  return (
    <DebugWorkspace
      key={JSON.stringify(profiles?.active ?? null)}
      profile={profiles?.active ?? null}
      profiles={Object.keys(profiles?.profiles ?? {})}
    />
  );
}

function DebugWorkspace({
  profile,
  profiles,
}: {
  readonly profile: string | null;
  readonly profiles: readonly string[];
}) {
  const [mode, setMode] = useState<Mode>('Search');
  const [topics, setTopics] = useState('');
  const [query, setQuery] = useState('');
  const [correlationField, setCorrelationField] = useState('value.orderId');
  const [correlation, setCorrelation] = useState('');
  const [correlationType, setCorrelationType] = useState<'text' | 'json'>('text');
  const [startTime, setStartTime] = useState('');
  const [endTime, setEndTime] = useState('');
  const [limit, setLimit] = useState('1000');
  const [result, setResult] = useState<ScanResult | null>(null);
  const [progress, setProgress] = useState('');
  const [error, setError] = useState('');
  const [running, setRunning] = useState(false);
  const controller = useRef<AbortController | null>(null);
  const [selected, setSelected] = useState<Set<string>>(new Set());
  const [page, setPage] = useState(0);
  const [replay, setReplay] = useState('[]');
  const [output, setOutput] = useState<unknown>(null);
  const [key, setKey] = useState('');
  const [partitioner, setPartitioner] = useState('default');
  const [schemaBytes, setSchemaBytes] = useState('');
  const [saved, setSaved] = useState<Record<string, unknown>>({});
  const storageKey = `kafka-studio-searches:v1:${JSON.stringify(profile)}`;
  const { data: health } = useQuery({ queryKey: healthQueryKey, queryFn: fetchHealth });
  const canWrite = health?.readOnly === false;
  useEffect(() => {
    try {
      const raw: unknown = JSON.parse(localStorage.getItem(storageKey) ?? '{}');
      if (raw && typeof raw === 'object' && !Array.isArray(raw)) setSaved(raw as Record<string, unknown>);
    } catch {
      /* A corrupt saved search does not prevent a scan. */
    }
    return () => {
      controller.current?.abort();
    };
  }, [storageKey]);

  async function operate(action: (signal: AbortSignal) => Promise<void> | void) {
    controller.current?.abort();
    const current = new AbortController();
    controller.current = current;
    setRunning(true);
    setError('');
    setOutput(null);
    try {
      await action(current.signal);
    } catch (failure) {
      if (!current.signal.aborted) setError(failure instanceof Error ? failure.message : 'Operation failed');
    } finally {
      if (controller.current === current) setRunning(false);
    }
  }
  function buildScan() {
    let predicates: MessagePredicate[] = [];
    if (mode === 'Correlate') predicates = correlationPredicates(correlationField, correlation, correlationType);
    else if (mode === 'Search') predicates = parseMessageQuery(query);
    return scanRequestSchema.parse({
      topics: topics
        .split(',')
        .map((entry) => entry.trim())
        .filter(Boolean),
      predicates,
      maxRecords: Number(limit),
      maxResults: 1000,
      ...(startTime ? { startTime: new Date(startTime).getTime() } : {}),
      ...(endTime ? { endTime: new Date(endTime).getTime() } : {}),
    });
  }
  async function scan() {
    await operate(async (signal) => {
      const input = buildScan();
      setResult(null);
      setSelected(new Set());
      setPage(0);
      await streamDebug('/api/debug/scan', input, signal, (event, value) => {
        if (event === 'progress') {
          const progress = value as { scanned: number; matched: number };
          setProgress(`Scanned ${String(progress.scanned)}; matched ${String(progress.matched)}`);
        }
        if (event === 'result') {
          const final = value as ScanResult;
          setResult(final);
          setProgress(`Scanned ${String(final.scanned)}; matched ${String(final.matched)}; ${final.reason}`);
        }
      });
    });
  }
  function saveSearch() {
    try {
      const input = buildScan();
      const name = `${topics}: ${query || 'all records'}`;
      const next = { ...saved, [name]: input };
      localStorage.setItem(storageKey, JSON.stringify(next));
      setSaved(next);
      setError('');
    } catch (failure) {
      setError(failure instanceof Error ? failure.message : 'Could not save search');
    }
  }
  function loadSearch(name: string) {
    const parsed = scanRequestSchema.safeParse(saved[name]);
    if (!parsed.success) {
      setError('This saved search is invalid');
      return;
    }
    setTopics(parsed.data.topics.join(', '));
    setLimit(String(parsed.data.maxRecords));
    const operators = { eq: '==', ne: '!=', contains: 'contains', gt: '>', gte: '>=', lt: '<', lte: '<=' };
    setQuery(
      parsed.data.predicates
        .map((entry) => `${entry.field} ${operators[entry.operator]} ${JSON.stringify(entry.value)}`)
        .join(' AND '),
    );
    const localDate = (time: number | undefined) => (time === undefined ? '' : formatLocalDateTime(new Date(time)));
    setStartTime(localDate(parsed.data.startTime));
    setEndTime(localDate(parsed.data.endTime));
  }
  function stage(records: readonly DebugRecord[]) {
    setReplay(
      JSON.stringify(
        records.map(({ key, value, headers, headerValues }) => ({ key, value, headers: headerValues ?? headers })),
        null,
        2,
      ),
    );
    setMode('Replay');
  }
  const records = result?.records ?? [];
  const displayed =
    mode === 'Correlate' ? [...records].sort((a, b) => Number(BigInt(a.timestamp) - BigInt(b.timestamp))) : records;
  const controls = (
    <div className="flex flex-wrap gap-2">
      {MODES.map((item) => (
        <Button
          key={item}
          variant={mode === item ? 'default' : 'outline'}
          aria-pressed={mode === item}
          disabled={running}
          onClick={() => {
            setMode(item);
            setResult(null);
            setSelected(new Set());
            setPage(0);
            setProgress('');
            setOutput(null);
            setError('');
          }}
        >
          {item}
        </Button>
      ))}
    </div>
  );
  return (
    <PageLayout toolbar={controls}>
      <div className="flex flex-col gap-5">
        <p className="text-sm text-muted-foreground">
          Debugger · profile {profile ?? 'default'}. Reads use isolated consumers and do not commit group offsets.
        </p>
        {(mode === 'Search' || mode === 'Correlate' || mode === 'State' || mode === 'Partitions') && (
          <section className="flex flex-col gap-4 rounded-xl border border-border p-4" aria-label="Scan options">
            <Field label="Topics (comma separated, up to eight)">
              <Input
                value={topics}
                onChange={(event) => setTopics(event.target.value)}
                placeholder="orders, payments"
              />
            </Field>
            {mode === 'Search' && (
              <Field label="Query (all conditions must match)">
                <Input
                  value={query}
                  onChange={(event) => setQuery(event.target.value)}
                  placeholder={'value.orderId == "123" AND headers.source == "checkout"'}
                />
              </Field>
            )}
            {mode === 'Correlate' && (
              <div className="grid gap-4 sm:grid-cols-3">
                <Field label="Correlation field">
                  <Input value={correlationField} onChange={(event) => setCorrelationField(event.target.value)} />
                </Field>
                <Field label="Correlation value type">
                  <SelectInput
                    aria-label="Correlation value type"
                    value={correlationType}
                    onValueChange={(value) => setCorrelationType(value === 'json' ? 'json' : 'text')}
                    items={[
                      { value: 'text', label: 'Text' },
                      { value: 'json', label: 'JSON literal (number, boolean, null, or quoted text)' },
                    ]}
                  />
                </Field>
                <Field label="Exact correlation value">
                  <Input value={correlation} onChange={(event) => setCorrelation(event.target.value)} />
                </Field>
              </div>
            )}
            <div className="grid gap-4 sm:grid-cols-3">
              <Field label="From time (local)">
                <DateTimePicker label="From time (local)" value={startTime} onChange={setStartTime} />
              </Field>
              <Field label="Through time (local)">
                <DateTimePicker label="Through time (local)" value={endTime} onChange={setEndTime} />
              </Field>
              <Field label="Maximum scanned records">
                <Input
                  type="number"
                  className="h-10"
                  min={1}
                  max={10000}
                  value={limit}
                  onChange={(event) => setLimit(event.target.value)}
                />
              </Field>
            </div>
            <div className="flex flex-wrap gap-2">
              <Button disabled={running} onClick={() => void scan()}>
                Scan snapshot
              </Button>
              {mode === 'Search' && (
                <Button variant="outline" disabled={running} onClick={saveSearch}>
                  Save search locally
                </Button>
              )}
            </div>
            {mode === 'Search' && Object.keys(saved).length > 0 && (
              <Field label="Saved searches">
                <SelectInput
                  aria-label="Saved searches"
                  value=""
                  onValueChange={loadSearch}
                  items={[
                    { value: '', label: 'Load a search…' },
                    ...Object.keys(saved).map((name) => ({ value: name, label: name })),
                  ]}
                />
              </Field>
            )}
            <p className="text-xs text-muted-foreground">
              Bounds: 30 seconds, 16 MiB, up to 10,000 scanned records and 1,000 results across all topics. Offset and
              timestamp query values must be quoted decimal strings. Scans start at retained history or the chosen time.
            </p>
          </section>
        )}
        <div role="status" aria-live="polite" className="text-sm">
          {running ? `Running… ${progress}` : progress}
        </div>
        {running && (
          <Button
            variant="outline"
            onClick={() => {
              controller.current?.abort();
              setProgress('Cancelled. Writes already acknowledged remain in Kafka.');
            }}
          >
            Cancel operation
          </Button>
        )}
        {error && (
          <div role="alert" className="rounded-lg border border-destructive p-3 text-sm">
            {error}
          </div>
        )}
        {mode === 'Correlate' && (
          <p className="text-sm">
            Matches are ordered by record timestamp. Correlation and timestamp order do not prove causality or
            processing latency.
          </p>
        )}
        {result && ['Search', 'Correlate', 'State', 'Partitions'].includes(mode) && (
          <details>
            <summary className="cursor-pointer text-sm">
              Scan coverage — {result.complete ? 'complete within captured bounds' : `partial (${result.reason})`}
            </summary>
            <Output value={result.coverage} />
          </details>
        )}
        {(mode === 'Search' || mode === 'Correlate') && result && (
          <>
            <div className="flex flex-wrap items-center gap-2">
              <Button
                variant="outline"
                disabled={selected.size === 0 || selected.size > 100 || !canWrite}
                onClick={() => stage(records.filter((record) => selected.has(recordId(record))))}
              >
                Stage selected for replay ({selected.size})
              </Button>
              <Button variant="outline" onClick={() => downloadJson('kafka-search.json', result)}>
                Export results and coverage
              </Button>
            </div>
            <div
              role="region"
              aria-label="Search results"
              tabIndex={0}
              className="overflow-auto focus-visible:ring-2 focus-visible:ring-ring"
            >
              <table className="w-full text-left text-sm">
                <thead>
                  <tr>
                    {['Select', 'Topic / partition / offset', 'Timestamp', 'Key', 'Value'].map((heading) => (
                      <th scope="col" className="p-2" key={heading}>
                        {heading}
                      </th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {displayed.slice(page * 50, (page + 1) * 50).map((record) => (
                    <tr className="border-t border-border" key={recordId(record)}>
                      <td className="p-2">
                        <input
                          type="checkbox"
                          className="size-6 focus-visible:ring-2 focus-visible:ring-ring"
                          aria-label={`Select ${record.topic} ${String(record.partition)} ${record.offset}`}
                          checked={selected.has(recordId(record))}
                          onChange={(event) =>
                            setSelected((previous) => {
                              const next = new Set(previous);
                              if (event.target.checked) next.add(recordId(record));
                              else next.delete(recordId(record));
                              return next;
                            })
                          }
                        />
                      </td>
                      <td className="p-2">
                        {record.topic} / {record.partition} / {record.offset}
                      </td>
                      <td className="p-2">{new Date(Number(record.timestamp)).toLocaleString()}</td>
                      <td className="max-w-48 truncate p-2">{text(record.key)}</td>
                      <td className="max-w-96 p-2">
                        <details>
                          <summary className="cursor-pointer truncate">{text(record.value).slice(0, 100)}</summary>
                          <Output
                            value={{ ...record, decodedKey: text(record.key), decodedValue: text(record.value) }}
                          />
                          <Button
                            variant="outline"
                            size="sm"
                            onClick={() => {
                              setSchemaBytes(record.value ?? '');
                              setMode('Schemas');
                            }}
                          >
                            Decode with schema
                          </Button>
                        </details>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
              {records.length === 0 && <p className="p-4">No matches within the reported scan coverage.</p>}
            </div>
            <div className="flex items-center gap-2">
              <Button variant="outline" disabled={page === 0} onClick={() => setPage(page - 1)}>
                Previous
              </Button>
              <span className="text-sm">Page {page + 1}</span>
              <Button variant="outline" disabled={(page + 1) * 50 >= records.length} onClick={() => setPage(page + 1)}>
                Next
              </Button>
            </div>
          </>
        )}
        {mode === 'State' && (
          <section aria-label="Compacted state" className="flex flex-col gap-3">
            <p className="text-sm">
              Latest observed value per topic, partition, and key. Tombstones remain visible. A partial scan or a
              start-time filter is a partial state view; null keys are excluded.
            </p>
            {compactedState(records).map(({ latest, history, tombstone }) => (
              <details key={recordId(latest)} className="rounded-lg border border-border p-3">
                <summary className="cursor-pointer text-sm">
                  {latest.topic} / {latest.partition} · {text(latest.key)} ·{' '}
                  {tombstone ? 'deleted (tombstone)' : 'value'} · {history.length} observed versions
                </summary>
                <Output
                  value={history.map((record) => ({
                    offset: record.offset,
                    value: record.value === null ? null : text(record.value),
                  }))}
                />
              </details>
            ))}
          </section>
        )}
        {mode === 'Partitions' && (
          <section className="flex flex-col gap-4 rounded-xl border border-border p-4" aria-label="Partition inspector">
            <p className="text-sm">
              Lookup uses the first topic above and the selected built-in partitioner. Custom and sticky routing cannot
              be predicted from a key alone.
            </p>
            <Field label="Key (UTF-8)">
              <Input value={key} onChange={(event) => setKey(event.target.value)} />
            </Field>
            <Field label="Partitioner">
              <SelectInput
                aria-label="Partitioner"
                value={partitioner}
                onValueChange={setPartitioner}
                items={[
                  { value: 'default', label: 'Default (murmur2)' },
                  { value: 'legacy', label: 'Legacy' },
                ]}
              />
            </Field>
            <Button
              disabled={running}
              onClick={() =>
                void operate(async (signal) => {
                  setOutput(
                    await apiSend(
                      'POST',
                      '/api/debug/partition',
                      {
                        topic: topics.split(',')[0]?.trim(),
                        key,
                        partitioner,
                      },
                      signal,
                    ),
                  );
                })
              }
            >
              Find partition
            </Button>
            {result && (
              <>
                <h2 className="font-semibold">Distribution in this scan</h2>
                <Output
                  value={Object.fromEntries(
                    [...new Set(records.map((record) => `${record.topic}/${String(record.partition)}`))].map(
                      (partition) => [
                        partition,
                        records.filter((record) => `${record.topic}/${String(record.partition)}` === partition).length,
                      ],
                    ),
                  )}
                />
                <h2 className="font-semibold">Most frequent sampled keys</h2>
                <Output value={sampledKeys(records)} />
              </>
            )}
          </section>
        )}
        <div hidden={mode !== 'Replay'}>
          <ReplayPanel
            running={running}
            canWrite={canWrite}
            operate={operate}
            setOutput={setOutput}
            replay={replay}
            setReplay={setReplay}
          />
        </div>
        <div hidden={mode !== 'Schemas'}>
          <SchemaPanel
            running={running}
            canWrite={canWrite}
            operate={operate}
            setOutput={setOutput}
            schemaBytes={schemaBytes}
            setSchemaBytes={setSchemaBytes}
            onStage={(value) => {
              setReplay(value);
              setMode('Replay');
            }}
          />
        </div>
        <div hidden={mode !== 'Scenarios'}>
          <ScenarioPanel
            running={running}
            canWrite={canWrite}
            operate={operate}
            setOutput={setOutput}
            setError={setError}
          />
        </div>
        <div hidden={mode !== 'Compare'}>
          <ComparePanel
            running={running}
            operate={operate}
            setOutput={setOutput}
            profile={profile}
            profiles={profiles}
          />
        </div>
        {output !== null && (
          <section aria-label="Operation results">
            <h2 className="mb-2 font-semibold">Results</h2>
            <Output value={output} />
          </section>
        )}
      </div>
    </PageLayout>
  );
}
