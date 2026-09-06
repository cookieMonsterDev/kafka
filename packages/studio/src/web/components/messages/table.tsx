import { useRef } from 'react';
import { Inbox } from 'lucide-react';
import { useVirtualizer } from '@tanstack/react-virtual';
import type { MessageRecord } from '../../../shared/contracts/message';
import { decodeMessageField } from '../../lib/decode';
import { formatTimestamp } from '../../lib/utils';
import { EmptyState } from '../ui/empty-state';
import type { MessageFiltersValue } from './filters';

const ROW_HEIGHT_PX = 36;

export function messageKey(message: MessageRecord): string {
  return `${String(message.partition)}:${message.offset}`;
}

export interface MessagesTableProps {
  readonly mode: 'history' | 'live';
  readonly filtered: readonly MessageRecord[];
  readonly decoder: MessageFiltersValue['decoder'];
  readonly selectedKey: string | null;
  readonly onSelectKey: (key: string | null) => void;
  readonly droppedCount: number;
}

/** The virtualized message list — its own component so `MessagesPage` isn't also the thing owning a `useVirtualizer` instance and 100+ lines of row markup. */
export function MessagesTable({ mode, filtered, decoder, selectedKey, onSelectKey, droppedCount }: MessagesTableProps) {
  const scrollRef = useRef<HTMLDivElement>(null);

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

  function toggleSelected(key: string): void {
    onSelectKey(key === selectedKey ? null : key);
  }

  return (
    <div className="overflow-hidden rounded-xl border border-border">
      {mode === 'live' && droppedCount > 0 && (
        <p className="border-b border-border bg-muted/40 px-3 py-1.5 text-xs text-muted-foreground">
          {droppedCount} message{droppedCount === 1 ? '' : 's'} arrived too fast to keep up with and were dropped from
          this view.
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
                  style={{ height: `${String(virtualizer.getTotalSize())}px`, position: 'relative', display: 'block' }}
                >
                  {virtualizer.getVirtualItems().map((virtualRow) => {
                    const message = filtered[virtualRow.index];
                    if (message === undefined) return null;
                    const key = messageKey(message);
                    const decodedKey = message.key === null ? 'null' : decodeMessageField(message.key, decoder).text;
                    const decodedValue =
                      message.value === null ? 'null' : decodeMessageField(message.value, decoder).text;
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
                        tabIndex={0}
                        aria-selected={key === selectedKey}
                        className="flex cursor-pointer items-center border-b border-border last:border-0 outline-none hover:bg-muted/40 aria-selected:bg-accent focus-visible:bg-muted/40"
                        onClick={() => toggleSelected(key)}
                        onKeyDown={(event) => {
                          if (event.key !== 'Enter' && event.key !== ' ') return;
                          event.preventDefault();
                          toggleSelected(key);
                        }}
                      >
                        <td className="w-20 px-3 py-1.5">{message.partition}</td>
                        <td className="w-24 px-3 py-1.5 tabular-nums">{message.offset}</td>
                        <td className="w-44 px-3 py-1.5 text-muted-foreground">{formatTimestamp(message.timestamp)}</td>
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
  );
}
