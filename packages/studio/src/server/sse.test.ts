import http from 'node:http';
import type { AddressInfo } from 'node:net';
import { describe, expect, it, vi } from 'vitest';
import { openSseStream } from './sse';

async function withServer<T>(
  handler: (req: http.IncomingMessage, res: http.ServerResponse) => void,
  run: (baseUrl: string) => Promise<T>,
): Promise<T> {
  const server = http.createServer(handler);
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
  const { port } = server.address() as AddressInfo;
  try {
    return await run(`http://127.0.0.1:${String(port)}`);
  } finally {
    await new Promise<void>((resolve) => server.close(() => resolve()));
  }
}

describe('openSseStream', () => {
  it('writes an event-stream content type and streamed frames', async () => {
    await withServer(
      (req, res) => {
        const stream = openSseStream(req, res, () => {});
        stream.send('progress', { sent: 1, total: 2 });
        stream.send('progress', { sent: 2, total: 2 });
        stream.close();
      },
      async (baseUrl) => {
        const res = await fetch(baseUrl);
        expect(res.headers.get('content-type')).toBe('text/event-stream');
        expect(res.headers.get('cache-control')).toBe('no-store');
        const body = await res.text();
        expect(body).toBe(
          'event: progress\ndata: {"sent":1,"total":2}\n\nevent: progress\ndata: {"sent":2,"total":2}\n\n',
        );
      },
    );
  });

  it('serializes bigint values the same way sendJson does', async () => {
    await withServer(
      (req, res) => {
        const stream = openSseStream(req, res, () => {});
        stream.send('progress', { offset: 5n });
        stream.close();
      },
      async (baseUrl) => {
        const res = await fetch(baseUrl);
        await expect(res.text()).resolves.toBe('event: progress\ndata: {"offset":"5"}\n\n');
      },
    );
  });

  it('calls onClose exactly once when the route closes the stream itself', async () => {
    const onClose = vi.fn();
    await withServer(
      (req, res) => {
        const stream = openSseStream(req, res, onClose);
        stream.close();
        stream.close();
      },
      async (baseUrl) => {
        await fetch(baseUrl);
      },
    );
    expect(onClose).toHaveBeenCalledOnce();
  });

  it('drops sends after the stream is closed', async () => {
    await withServer(
      (req, res) => {
        const stream = openSseStream(req, res, () => {});
        stream.close();
        stream.send('progress', { sent: 1, total: 1 });
      },
      async (baseUrl) => {
        const res = await fetch(baseUrl);
        await expect(res.text()).resolves.toBe('');
      },
    );
  });
});
