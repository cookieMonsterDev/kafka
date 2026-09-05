import type { IncomingMessage, ServerResponse } from 'node:http';
import { stringifyJson } from './json';

const HEARTBEAT_INTERVAL_MS = 15_000;

export interface SseStream {
  /** Writes one `event: <name>\ndata: <json>\n\n` frame. A no-op once the stream is closed. */
  send(event: string, data: unknown): void;
  /** Ends the response and runs the `onClose` callback, if it hasn't already run. Idempotent. */
  close(): void;
}

/**
 * Opens a `text/event-stream` response and keeps it alive with a periodic comment heartbeat, so an
 * idle proxy in front of the studio doesn't time out the connection. `onClose` runs exactly once,
 * whichever side ends the connection first — the client navigating away (`req` emits `close`) or a
 * route calling {@link SseStream.close} itself once it has nothing more to send.
 */
export function openSseStream(req: IncomingMessage, res: ServerResponse, onClose: () => void): SseStream {
  res.writeHead(200, {
    'content-type': 'text/event-stream',
    'cache-control': 'no-store',
    connection: 'keep-alive',
  });

  const heartbeat = setInterval(() => {
    res.write(':heartbeat\n\n');
  }, HEARTBEAT_INTERVAL_MS);

  let closed = false;
  function close(): void {
    if (closed) return;
    closed = true;
    clearInterval(heartbeat);
    res.end();
    onClose();
  }

  req.on('close', close);

  return {
    send(event, data) {
      if (closed) return;
      res.write(`event: ${event}\ndata: ${stringifyJson(data)}\n\n`);
    },
    close,
  };
}
