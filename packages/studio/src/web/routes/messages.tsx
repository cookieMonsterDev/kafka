import { useMemo, useState } from 'react';
import { Download, Inbox, Pause, Play } from 'lucide-react';
import { useQuery } from '@tanstack/react-query';
import { createRoute } from '@tanstack/react-router';
import type { MessageRecord } from '../../shared/contracts/message';
import { downloadMessagesAsJsonl } from '../components/messages/export';
import { MessageDetailRail } from '../components/messages/detail-rail';
import { MessageFilters, type MessageFiltersValue } from '../components/messages/filters';
import { messageKey, MessagesTable } from '../components/messages/table';
import { TopicPicker } from '../components/producer/topic-picker';
import { PageLayout } from '../components/layout/page';
import { Button } from '../components/ui/button';
import { EmptyState } from '../components/ui/empty-state';
import { ErrorState } from '../components/ui/error-state';
import { Skeleton } from '../components/ui/skeleton';
import { decodeMessageField } from '../lib/decode';
import { listMessages, messagesQueryKeys, tailUrl } from '../lib/messages-api';
import { useMessageTail } from '../lib/sse';
import { getTopic, topicQueryKeys } from '../lib/topics-api';
import { rootRoute } from './root';

export interface MessagesSearch {
  /** Prefills the topic picker — the board's "Tail this topic" action links in with this. */
  readonly topic?: string;
}

export const messagesRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: '/messages',
  validateSearch: (search: Record<string, unknown>): MessagesSearch => ({
    topic: typeof search.topic === 'string' ? search.topic : undefined,
  }),
  component: MessagesPage,
});

const HISTORY_LIMIT = 200;

type Mode = 'history' | 'live';

function matchesSearch(message: MessageRecord, search: string, decoder: MessageFiltersValue['decoder']): boolean {
  const query = search.trim().toLowerCase();
  if (query === '') return true;

  const key = message.key === null ? '' : decodeMessageField(message.key, decoder).text;
  const value = message.value === null ? '' : decodeMessageField(message.value, decoder).text;
  if (key.toLowerCase().includes(query) || value.toLowerCase().includes(query)) return true;

  return Object.entries(message.headers).some(([headerKey, headerValue]) => {
    if (headerKey.toLowerCase().includes(query)) return true;
    return headerValue !== null && decodeMessageField(headerValue, decoder).text.toLowerCase().includes(query);
  });
}

function MessagesPage() {
  const { topic: initialTopic } = messagesRoute.useSearch();
  const [topic, setTopic] = useState<string | null>(initialTopic ?? null);
  const [mode, setMode] = useState<Mode>('history');
  const [filters, setFilters] = useState<MessageFiltersValue>({
    partition: null,
    from: 'latest',
    search: '',
    decoder: 'utf8',
  });
  const [selectedKey, setSelectedKey] = useState<string | null>(null);

  const topicDetail = useQuery({
    queryKey: topic === null ? topicQueryKeys.all : topicQueryKeys.detail(topic),
    queryFn: () => getTopic(topic ?? ''),
    enabled: topic !== null,
  });
  const partitions = useMemo(
    () => (topicDetail.data?.partitions ?? []).map((partition) => partition.partitionIndex).sort((a, b) => a - b),
    [topicDetail.data],
  );

  const historyQuery = useQuery({
    queryKey: messagesQueryKeys.page(topic ?? '', {
      partition: filters.partition ?? undefined,
      from: filters.from,
      limit: HISTORY_LIMIT,
    }),
    queryFn: () =>
      listMessages(topic ?? '', {
        partition: filters.partition ?? undefined,
        from: filters.from,
        limit: HISTORY_LIMIT,
      }),
    enabled: topic !== null && mode === 'history',
  });

  const tail = useMessageTail(
    topic !== null && mode === 'live' ? tailUrl(topic, filters.partition ?? undefined) : null,
  );

  const messages = mode === 'history' ? (historyQuery.data?.messages ?? []) : tail.messages;
  const filtered = useMemo(
    () => messages.filter((message) => matchesSearch(message, filters.search, filters.decoder)),
    [messages, filters.search, filters.decoder, matchesSearch],
  );
  const selected = filtered.find((message) => messageKey(message) === selectedKey) ?? null;

  const rail =
    topic !== null && selected !== null ? (
      <MessageDetailRail topic={topic} message={selected} decoder={filters.decoder} />
    ) : undefined;

  // Kept to the controls that always fit on one line — the toolbar slot is a fixed-height row
  // (`PageLayout`), so anything that wraps onto a second line would overlap the content below it.
  // The rest (partition/decoder/search) is a second, ordinary-flow row inside the page content.
  const toolbar = (
    <div className="flex w-full min-w-0 items-center gap-2">
      <TopicPicker
        value={topic}
        onChange={(next) => {
          setTopic(next);
          setSelectedKey(null);
        }}
      />
      <div className="flex shrink-0 items-center gap-1 rounded-lg border border-border p-0.5">
        <Button
          type="button"
          size="sm"
          variant={mode === 'history' ? 'secondary' : 'ghost'}
          onClick={() => setMode('history')}
          aria-pressed={mode === 'history'}
        >
          History
        </Button>
        <Button
          type="button"
          size="sm"
          variant={mode === 'live' ? 'secondary' : 'ghost'}
          onClick={() => setMode('live')}
          aria-pressed={mode === 'live'}
        >
          {mode === 'live' ? (
            <Pause className="size-4" aria-hidden="true" />
          ) : (
            <Play className="size-4" aria-hidden="true" />
          )}
          Live
        </Button>
      </div>
      <Button
        type="button"
        variant="outline"
        size="sm"
        className="ml-auto shrink-0"
        disabled={filtered.length === 0}
        onClick={() => downloadMessagesAsJsonl(filtered, filters.decoder, `${topic ?? 'messages'}.jsonl`)}
      >
        <Download className="size-4" aria-hidden="true" />
        <span className="max-sm:sr-only">Export</span>
      </Button>
    </div>
  );

  return (
    <PageLayout toolbar={toolbar} rail={rail} railLabel="Message detail">
      <section aria-label="Messages" className="flex flex-col gap-4">
        {topic !== null && (
          <MessageFilters value={filters} onChange={setFilters} partitions={partitions} showFrom={mode === 'history'} />
        )}

        {topic === null && (
          <EmptyState
            icon={Inbox}
            title="Choose a topic"
            description="Pick a topic above to browse or tail its messages."
          />
        )}

        {topic !== null && mode === 'history' && historyQuery.isPending && (
          <div
            className="flex flex-col gap-1 overflow-hidden rounded-xl border border-border p-2"
            role="status"
            aria-busy="true"
          >
            <span className="sr-only">Loading messages…</span>
            {[0, 1, 2, 3, 4, 5].map((row) => (
              <Skeleton key={row} className="h-9 w-full" />
            ))}
          </div>
        )}
        {topic !== null && mode === 'history' && historyQuery.isError && (
          <div className="rounded-xl border border-border">
            <ErrorState
              title="Could not load messages"
              error={historyQuery.error}
              onRetry={() => void historyQuery.refetch()}
            />
          </div>
        )}
        {topic !== null && mode === 'live' && tail.error !== null && (
          <div className="rounded-xl border border-destructive/30">
            <ErrorState title="The live tail stopped" error={tail.error} />
          </div>
        )}

        {topic !== null && (mode === 'history' ? historyQuery.data !== undefined : true) && (
          <MessagesTable
            mode={mode}
            filtered={filtered}
            decoder={filters.decoder}
            selectedKey={selectedKey}
            onSelectKey={setSelectedKey}
            droppedCount={tail.droppedCount}
          />
        )}
      </section>
    </PageLayout>
  );
}
