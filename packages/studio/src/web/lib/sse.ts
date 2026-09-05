import { useEffect, useState } from 'react';

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
