import type { AddressInfo } from 'node:net';
import { describe, expect, it } from 'vitest';
import { createStudioServer } from '../create-server';
import { StudioEventBus } from '../kafka/events';
import { Router } from '../router';
import { registerEventRoutes } from './events';

async function withServer<T>(events: StudioEventBus, run: (baseUrl: string) => Promise<T>): Promise<T> {
  const router = new Router();
  registerEventRoutes(router, { events });
  const server = createStudioServer({ router });

  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
  const { port } = server.address() as AddressInfo;
  try {
    return await run(`http://127.0.0.1:${String(port)}`);
  } finally {
    await new Promise<void>((resolve) => server.close(() => resolve()));
  }
}

describe('registerEventRoutes', () => {
  it('GET /api/events streams published activity as SSE frames', async () => {
    const events = new StudioEventBus();

    await withServer(events, async (baseUrl) => {
      const controller = new AbortController();
      const res = await fetch(`${baseUrl}/api/events`, { signal: controller.signal });
      expect(res.status).toBe(200);
      expect(res.headers.get('content-type')).toBe('text/event-stream');

      const reader = res.body?.getReader();
      if (reader === undefined) throw new Error('no readable body');

      events.publish({ kind: 'produce', topic: 'orders', partition: 0, count: 1, bytes: 5 });
      const { value } = await reader.read();
      const frame = Buffer.from(value ?? new Uint8Array()).toString('utf8');

      expect(frame).toContain('event: activity');
      expect(frame).toContain('"topic":"orders"');
      controller.abort();
    });
  });

  it('unsubscribes from the bus once the connection closes', async () => {
    const events = new StudioEventBus();

    await withServer(events, async (baseUrl) => {
      const controller = new AbortController();
      await fetch(`${baseUrl}/api/events`, { signal: controller.signal });
      controller.abort();
    });

    await new Promise((resolve) => setTimeout(resolve, 20));
    expect(events.hasListeners()).toBe(false);
  });
});
