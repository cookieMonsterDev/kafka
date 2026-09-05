import { useState } from 'react';
import { Play, Send, Shuffle, Square } from 'lucide-react';
import { useMutation } from '@tanstack/react-query';
import { createRoute } from '@tanstack/react-router';
import type { BurstProgress } from '../../shared/contracts/produce';
import { ProduceHistory, type ProduceHistoryEntry } from '../components/producer/history';
import { PayloadEditor } from '../components/producer/payload-editor';
import { PAYLOAD_TEMPLATES } from '../components/producer/templates';
import { TopicPicker } from '../components/producer/topic-picker';
import { PageLayout } from '../components/layout/page';
import { Badge } from '../components/ui/badge';
import { Button } from '../components/ui/button';
import { Input } from '../components/ui/input';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '../components/ui/select';
import { errorMessage } from '../components/ui/error-state';
import { burstProgressUrl, cancelBurst, produceMessages, startBurst } from '../lib/produce-api';
import {
  buildProduceMessage,
  createEmptyPayloadValue,
  payloadEditorValueError,
  type PayloadEditorValue,
} from '../lib/payload-editor-schema';
import { useEventSource } from '../lib/sse';
import { rootRoute } from './root';

export const producerRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: '/producer',
  component: ProducerPage,
});

let nextHistoryId = 0;

const BURST_STATUS_VARIANT: Record<BurstProgress['status'], 'default' | 'accent' | 'destructive'> = {
  running: 'accent',
  completed: 'default',
  cancelled: 'default',
  failed: 'destructive',
};

function ProducerPage() {
  const [topic, setTopic] = useState<string | null>(null);
  const [payload, setPayload] = useState<PayloadEditorValue>(createEmptyPayloadValue());
  const [templateId, setTemplateId] = useState<string | null>(null);
  const [history, setHistory] = useState<readonly ProduceHistoryEntry[]>([]);

  const [count, setCount] = useState('100');
  const [ratePerSecond, setRatePerSecond] = useState('');
  const [jobId, setJobId] = useState<string | null>(null);

  const payloadError = payloadEditorValueError(payload);
  const { data: progress } = useEventSource<BurstProgress>(jobId !== null ? burstProgressUrl(jobId) : null, 'progress');

  function pushHistory(entry: Omit<ProduceHistoryEntry, 'id' | 'sentAt'>): void {
    nextHistoryId += 1;
    setHistory((entries) =>
      [{ ...entry, id: `history-${String(nextHistoryId)}`, sentAt: new Date().toISOString() }, ...entries].slice(0, 50),
    );
  }

  const sendMutation = useMutation({
    mutationFn: async () => {
      if (topic === null) throw new Error('select a topic first');
      return produceMessages({ topic, messages: [buildProduceMessage(payload)] });
    },
    onSuccess: (result) => {
      const [first] = result.results;
      pushHistory({
        topic: topic ?? '',
        key: payload.key,
        value: payload.value,
        outcome: { ok: true, detail: first ? `partition ${String(first.partition)}, offset ${first.offset}` : 'sent' },
      });
    },
    onError: (error) => {
      pushHistory({
        topic: topic ?? '',
        key: payload.key,
        value: payload.value,
        outcome: { ok: false, detail: errorMessage(error) ?? 'send failed' },
      });
    },
  });

  const burstMutation = useMutation({
    mutationFn: async () => {
      if (topic === null) throw new Error('select a topic first');
      const parsedCount = Number(count);
      const parsedRate = ratePerSecond.trim() === '' ? undefined : Number(ratePerSecond);
      return startBurst({
        topic,
        template: buildProduceMessage(payload),
        count: parsedCount,
        ...(parsedRate !== undefined ? { ratePerSecond: parsedRate } : {}),
      });
    },
    onSuccess: (result) => setJobId(result.jobId),
  });

  const cancelMutation = useMutation({
    mutationFn: () => {
      if (jobId === null) throw new Error('no burst is running');
      return cancelBurst(jobId);
    },
  });

  function applyTemplate(id: string): void {
    const template = PAYLOAD_TEMPLATES.find((entry) => entry.id === id);
    if (template === undefined) return;
    setTemplateId(id);
    const built = template.build();
    setPayload((current) => ({ ...current, key: built.key, value: built.value }));
  }

  function randomizeTemplate(): void {
    if (templateId !== null) applyTemplate(templateId);
  }

  const countValid = /^\d+$/.test(count.trim()) && Number(count) > 0;
  const rail = (
    <ProduceHistory
      entries={history}
      onClear={() => setHistory([])}
      onReplay={(entry) => {
        setTopic(entry.topic);
        setPayload((current) => ({ ...current, key: entry.key, value: entry.value }));
      }}
    />
  );

  return (
    <PageLayout rail={rail} railLabel="Recent sends">
      <section aria-label="Producer" className="flex flex-col gap-6">
        <div className="rounded-xl border border-border bg-card p-4 text-card-foreground">
          <div className="flex flex-wrap items-center justify-between gap-3">
            <TopicPicker
              value={topic}
              onChange={setTopic}
              disabled={sendMutation.isPending || burstMutation.isPending}
            />
            <div className="flex items-center gap-2">
              <Select value={templateId ?? undefined} onValueChange={applyTemplate}>
                <SelectTrigger size="sm" className="w-56" aria-label="Payload template">
                  <SelectValue placeholder="Use a template…" />
                </SelectTrigger>
                <SelectContent>
                  {PAYLOAD_TEMPLATES.map((template) => (
                    <SelectItem key={template.id} value={template.id}>
                      {template.label}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
              <Button
                type="button"
                variant="outline"
                size="sm"
                disabled={templateId === null}
                onClick={randomizeTemplate}
                aria-label="Regenerate from the selected template"
              >
                <Shuffle className="size-4" aria-hidden="true" />
              </Button>
            </div>
          </div>

          <div className="mt-4">
            <PayloadEditor value={payload} onChange={setPayload} disabled={sendMutation.isPending} />
          </div>

          <div className="mt-4 flex justify-end">
            <Button
              type="button"
              disabled={topic === null || payloadError !== null || sendMutation.isPending}
              onClick={() => sendMutation.mutate()}
            >
              <Send className="size-4" aria-hidden="true" />
              {sendMutation.isPending ? 'Sending…' : 'Send message'}
            </Button>
          </div>
        </div>

        <div className="rounded-xl border border-border bg-card p-4 text-card-foreground">
          <h2 className="text-sm font-semibold">Burst</h2>
          <p className="mt-1 text-sm text-muted-foreground">
            Sends the payload above repeatedly, rate-limited to avoid overwhelming the broker.{' '}
            <code className="rounded bg-muted px-1 py-0.5 font-mono text-xs text-foreground">{'{{seq}}'}</code> in the
            key or value is replaced with the message index.
          </p>

          <div className="mt-3 flex flex-wrap items-end gap-3">
            <label className="flex flex-col gap-1.5">
              <span className="text-sm font-medium">Count</span>
              <Input
                type="number"
                min={1}
                step={1}
                className="w-28"
                value={count}
                onChange={(event) => setCount(event.target.value)}
                disabled={jobId !== null && progress?.status === 'running'}
              />
            </label>
            <label className="flex flex-col gap-1.5">
              <span className="text-sm font-medium">Rate / sec</span>
              <Input
                type="number"
                min={1}
                step={1}
                className="w-28"
                placeholder="200"
                value={ratePerSecond}
                onChange={(event) => setRatePerSecond(event.target.value)}
                disabled={jobId !== null && progress?.status === 'running'}
              />
            </label>

            {progress?.status === 'running' ? (
              <Button
                type="button"
                variant="destructive"
                disabled={cancelMutation.isPending}
                onClick={() => cancelMutation.mutate()}
              >
                <Square className="size-4" aria-hidden="true" />
                Stop
              </Button>
            ) : (
              <Button
                type="button"
                disabled={topic === null || payloadError !== null || !countValid || burstMutation.isPending}
                onClick={() => burstMutation.mutate()}
              >
                <Play className="size-4" aria-hidden="true" />
                {burstMutation.isPending ? 'Starting…' : 'Start burst'}
              </Button>
            )}
          </div>

          {progress && (
            <div className="mt-3 flex items-center gap-2 text-sm">
              <Badge variant={BURST_STATUS_VARIANT[progress.status]}>{progress.status}</Badge>
              <span className="text-muted-foreground">
                {progress.sent} / {progress.total} sent
              </span>
              {progress.error !== undefined && <span className="text-destructive">{progress.error}</span>}
            </div>
          )}
          {burstMutation.isError && (
            <p className="mt-2 text-sm text-destructive" role="alert">
              {errorMessage(burstMutation.error) ?? 'could not start the burst'}
            </p>
          )}
        </div>
      </section>
    </PageLayout>
  );
}
