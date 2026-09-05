import { useEffect, useState } from 'react';
import type { MessageRecord } from '../../shared/contracts/message';

/**
 * Subscribes to a named SSE event on `url` and keeps the most recently received payload. Pass
 * `null` to stay disconnected (e.g. no job started yet) — the effect tears the connection down
 * whenever `url` changes or the component unmounts, and `EventSource`'s own automatic-reconnect
 * behavior covers a dropped connection while `url` stays the same.
 */
export function useEventSource<T>(url: string | null, eventName = 'message'): { data: T | null } {
  const [data, setData] = useState<T | null>(null);

  useEffect(() => {
    setData(null);
    if (url === null) return;

    const source = new EventSource(url);
    const handleMessage = (event: MessageEvent<string>): void => {
      try {
        setData(JSON.parse(event.data) as T);
      } catch {
        // A malformed frame is skipped rather than surfaced — the next one (or the connection's
        // own retry) is what recovers, not an error state over one bad payload.
      }
    };
    source.addEventListener(eventName, handleMessage);

    return () => {
      source.removeEventListener(eventName, handleMessage);
      source.close();
    };
  }, [url, eventName]);

  return { data };
}

const DEFAULT_MAX_TAILED_MESSAGES = 500;

export interface UseMessageTailOptions {
  /** Client-side ring buffer cap — oldest message dropped once past this. */
  readonly maxMessages?: number;
}

export interface UseMessageTailResult {
  readonly messages: readonly MessageRecord[];
  /** Total messages the server's buffer had to drop because this connection fell behind. */
  readonly droppedCount: number;
  readonly error: string | null;
  clear(): void;
}

/** Subscribes to a live message tail, accumulating frames into a capped buffer (unlike {@link useEventSource}, which only keeps the latest payload). Pass `url: null` to stay disconnected. */
export function useMessageTail(url: string | null, options: UseMessageTailOptions = {}): UseMessageTailResult {
  const maxMessages = options.maxMessages ?? DEFAULT_MAX_TAILED_MESSAGES;
  const [messages, setMessages] = useState<readonly MessageRecord[]>([]);
  const [droppedCount, setDroppedCount] = useState(0);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    setMessages([]);
    setDroppedCount(0);
    setError(null);
    if (url === null) return;

    const source = new EventSource(url);

    const handleMessage = (event: MessageEvent<string>): void => {
      try {
        const record = JSON.parse(event.data) as MessageRecord;
        setMessages((current) => {
          const next = [...current, record];
          return next.length > maxMessages ? next.slice(next.length - maxMessages) : next;
        });
      } catch {
        // malformed frame, skip it
      }
    };
    const handleGap = (event: MessageEvent<string>): void => {
      try {
        const { dropped } = JSON.parse(event.data) as { dropped: number };
        setDroppedCount((count) => count + dropped);
      } catch {
        // malformed frame, skip it
      }
    };
    // `tail-error`, not `error` — `EventSource`'s own native `error` fires on every reconnect blip.
    const handleServerError = (event: MessageEvent<string>): void => {
      try {
        const { message } = JSON.parse(event.data) as { message: string };
        setError(message);
      } catch {
        setError('the tail stream failed');
      }
    };

    source.addEventListener('message', handleMessage);
    source.addEventListener('gap', handleGap);
    source.addEventListener('tail-error', handleServerError);

    return () => {
      source.removeEventListener('message', handleMessage);
      source.removeEventListener('gap', handleGap);
      source.removeEventListener('tail-error', handleServerError);
      source.close();
    };
  }, [url, maxMessages]);

  return { messages, droppedCount, error, clear: () => setMessages([]) };
}
