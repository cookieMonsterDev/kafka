import { useMemo, useRef, useState } from 'react';
import { Check, Copy, Download, Inbox, Pause, Play, Trash2 } from 'lucide-react';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { createRoute } from '@tanstack/react-router';
import { useVirtualizer } from '@tanstack/react-virtual';
import type { MessageRecord } from '../../shared/contracts/message';
import { downloadMessagesAsJsonl } from '../components/messages/export';
import { MessageFilters, type MessageFiltersValue } from '../components/messages/filters';
import { TopicPicker } from '../components/producer/topic-picker';
import { PageLayout } from '../components/layout/page';
import { Badge } from '../components/ui/badge';
import { Button } from '../components/ui/button';
import { ConfirmDialog } from '../components/ui/confirm-dialog';
import { EmptyState } from '../components/ui/empty-state';
import { ErrorState, errorMessage } from '../components/ui/error-state';
import { Skeleton } from '../components/ui/skeleton';
import { toast } from '../components/ui/toast';
import { decodeMessageField } from '../lib/decode';
import { deleteRecords, listMessages, messagesQueryKeys, tailUrl } from '../lib/messages-api';
import { useMessageTail } from '../lib/sse';
import { getTopic, topicQueryKeys } from '../lib/topics-api';
import { formatTimestamp } from '../lib/utils';
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

const ROW_HEIGHT_PX = 36;
const HISTORY_LIMIT = 200;

type Mode = 'history' | 'live';

function messageKey(message: MessageRecord): string {
  return `${String(message.partition)}:${message.offset}`;
}

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

const COPY_CONFIRMATION_MS = 1500;

/** Copies decoded text to the clipboard, with a brief inline confirmation instead of only a toast — the toast can be missed while looking at the rail. */
function CopyButton({ text, label }: { readonly text: string; readonly label: string }) {
  const [copied, setCopied] = useState(false);

  async function handleCopy(): Promise<void> {
    try {
      await navigator.clipboard.writeText(text);
      setCopied(true);
      setTimeout(() => setCopied(false), COPY_CONFIRMATION_MS);
    } catch (error) {
      toast({ title: 'Could not copy to clipboard', description: errorMessage(error), variant: 'destructive' });
    }
  }

  return (
    <Button type="button" variant="ghost" size="icon-xs" aria-label={label} onClick={() => void handleCopy()}>
      {copied ? (
        <Check className="size-3.5 text-primary" aria-hidden="true" />
      ) : (
        <Copy className="size-3.5" aria-hidden="true" />
      )}
    </Button>
  );
}

function MessageDetailRail({
  topic,
  message,
  decoder,
}: {
  readonly topic: string;
  readonly message: MessageRecord;
  readonly decoder: MessageFiltersValue['decoder'];
}) {
  const queryClient = useQueryClient();
  const key = message.key === null ? null : decodeMessageField(message.key, decoder);
  const value = message.value === null ? null : decodeMessageField(message.value, decoder);
  const headerEntries = Object.entries(message.headers);

  const deleteMutation = useMutation({
    mutationFn: () =>
      deleteRecords(topic, {
        partitions: [{ partition: message.partition, beforeOffset: String(BigInt(message.offset) + 1n) }],
      }),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: messagesQueryKeys.all }),
  });

  return (
    <div className="flex flex-col gap-4">
      <div className="flex flex-wrap items-center gap-2">
        <Badge variant="outline">Partition {message.partition}</Badge>
        <Badge variant="outline">Offset {message.offset}</Badge>
      </div>
      <dl className="grid grid-cols-[auto_1fr] gap-x-3 gap-y-1 text-sm">
        <dt className="text-muted-foreground">Timestamp</dt>
        <dd>{formatTimestamp(message.timestamp)}</dd>
        <dt className="text-muted-foreground">Size</dt>
        <dd>{message.size} B</dd>
      </dl>

      <div className="flex flex-col gap-1.5">
        <div className="flex items-center justify-between">
          <span className="text-sm font-medium">Key</span>
          <CopyButton text={key === null ? 'null' : key.text} label="Copy key" />
        </div>
        <pre className="max-h-32 overflow-auto rounded-lg border border-border bg-muted/30 p-2 font-mono text-xs break-all whitespace-pre-wrap">
          {key === null ? 'null' : key.text}
        </pre>
        {key?.error !== undefined && <p className="text-xs text-muted-foreground">{key.error}</p>}
      </div>

      <div className="flex flex-col gap-1.5">
        <div className="flex items-center justify-between">
          <span className="text-sm font-medium">Value</span>
          <CopyButton text={value === null ? 'null' : value.text} label="Copy value" />
        </div>
        <pre className="max-h-64 overflow-auto rounded-lg border border-border bg-muted/30 p-2 font-mono text-xs break-all whitespace-pre-wrap">
          {value === null ? 'null' : value.text}
        </pre>
        {value?.error !== undefined && <p className="text-xs text-muted-foreground">{value.error}</p>}
      </div>

      {headerEntries.length > 0 && (
        <div className="flex flex-col gap-1.5">
          <span className="text-sm font-medium">Headers</span>
          <dl className="flex flex-col gap-1 text-xs">
            {headerEntries.map(([headerKey, headerValue]) => (
              <div key={headerKey} className="flex gap-2">
                <dt className="shrink-0 font-medium text-muted-foreground">{headerKey}</dt>
                <dd className="min-w-0 break-all">
                  {headerValue === null ? 'null' : decodeMessageField(headerValue, decoder).text}
                </dd>
              </div>
            ))}
          </dl>
        </div>
      )}

      <div className="mt-2 border-t border-border pt-4">
        <ConfirmDialog
          trigger={
            <Button type="button" variant="destructive" size="sm">
              <Trash2 className="size-4" aria-hidden="true" />
              Delete this record and everything before it
            </Button>
          }
          title="Delete records?"
          description={`Permanently deletes every record on partition ${String(message.partition)} up to and including offset ${message.offset}. This cannot be undone.`}
          confirmValue={topic}
          confirmLabel="Delete records"
          pending={deleteMutation.isPending}
          onConfirm={() => deleteMutation.mutate()}
        />
        {deleteMutation.isError && (
          <p className="mt-2 text-sm text-destructive" role="alert">
            {errorMessage(deleteMutation.error) ?? 'delete failed'}
          </p>
        )}
      </div>
    </div>
  );
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
  const scrollRef = useRef<HTMLDivElement>(null);

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
    [messages, filters.search, filters.decoder],
  );
  const selected = filtered.find((message) => messageKey(message) === selectedKey) ?? null;

  const virtualizer = useVirtualizer({
    count: filtered.length,
    getScrollElement: () => scrollRef.current,
    estimateSize: () => ROW_HEIGHT_PX,
    getItemKey: (index) => {
      const message = filtered[index];
      return message === undefined ? index : messageKey(message);
    },
    overscan: 16,
  });

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
          <div className="overflow-hidden rounded-xl border border-border">
            {mode === 'live' && tail.droppedCount > 0 && (
              <p className="border-b border-border bg-muted/40 px-3 py-1.5 text-xs text-muted-foreground">
                {tail.droppedCount} message{tail.droppedCount === 1 ? '' : 's'} arrived too fast to keep up with and
                were dropped from this view.
              </p>
            )}
            <div
              tabIndex={0}
              role="region"
              aria-label="Messages table, scroll horizontally for more columns"
              className="overflow-x-auto"
            >
              <div className="min-w-2xl">
                <table className="w-full table-fixed text-sm">
                  <thead>
                    <tr className="border-b border-border bg-muted/40 text-left text-xs font-medium text-muted-foreground">
                      <th scope="col" className="w-20 px-3 py-2">
                        Partition
                      </th>
                      <th scope="col" className="w-24 px-3 py-2">
                        Offset
                      </th>
                      <th scope="col" className="w-44 px-3 py-2">
                        Timestamp
                      </th>
                      <th scope="col" className="w-1/4 px-3 py-2">
                        Key
                      </th>
                      <th scope="col" className="px-3 py-2">
                        Value
                      </th>
                    </tr>
                  </thead>
                </table>
                <div ref={scrollRef} className="max-h-[60vh] overflow-y-auto">
                  {filtered.length === 0 ? (
                    <EmptyState
                      icon={Inbox}
                      title={mode === 'live' ? 'Waiting for messages…' : 'No messages found'}
                      description={
                        mode === 'live'
                          ? 'New messages produced to this topic will appear here.'
                          : 'Nothing matched, or this topic has no messages yet.'
                      }
                    />
                  ) : (
                    <table className="w-full table-fixed text-sm">
                      <tbody
                        style={{
                          height: `${String(virtualizer.getTotalSize())}px`,
                          position: 'relative',
                          display: 'block',
                        }}
                      >
                        {virtualizer.getVirtualItems().map((virtualRow) => {
                          const message = filtered[virtualRow.index];
                          if (message === undefined) return null;
                          const key = messageKey(message);
                          const decodedKey =
                            message.key === null ? 'null' : decodeMessageField(message.key, filters.decoder).text;
                          const decodedValue =
                            message.value === null ? 'null' : decodeMessageField(message.value, filters.decoder).text;
                          return (
                            <tr
                              key={key}
                              data-index={virtualRow.index}
                              ref={virtualizer.measureElement}
                              style={{
                                position: 'absolute',
                                top: 0,
                                left: 0,
                                width: '100%',
                                minHeight: `${String(ROW_HEIGHT_PX)}px`,
                                transform: `translateY(${String(virtualRow.start)}px)`,
                              }}
                              aria-selected={key === selectedKey}
                              className="flex cursor-pointer items-center border-b border-border last:border-0 hover:bg-muted/40 aria-selected:bg-accent"
                              onClick={() => setSelectedKey(key === selectedKey ? null : key)}
                            >
                              <td className="w-20 px-3 py-1.5">{message.partition}</td>
                              <td className="w-24 px-3 py-1.5 tabular-nums">{message.offset}</td>
                              <td className="w-44 px-3 py-1.5 text-muted-foreground">
                                {formatTimestamp(message.timestamp)}
                              </td>
                              <td className="w-1/4 min-w-0 truncate px-3 py-1.5 font-mono text-xs">{decodedKey}</td>
                              <td className="min-w-0 flex-1 truncate px-3 py-1.5 font-mono text-xs">{decodedValue}</td>
                            </tr>
                          );
                        })}
                      </tbody>
                    </table>
                  )}
                </div>
              </div>
            </div>
          </div>
        )}
      </section>
    </PageLayout>
  );
}
