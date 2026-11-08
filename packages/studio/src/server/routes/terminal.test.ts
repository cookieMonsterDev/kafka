import type { AddressInfo } from 'node:net';
import { describe, expect, it } from 'vitest';
import { createStudioServer } from '../create-server';
import { AdminPool } from '../kafka/admin-pool';
import { ProducerPool } from '../kafka/produce';
import { StudioEventBus } from '../kafka/events';
import { Router } from '../router';
import { registerTerminalRoutes } from './terminal';

async function withTerminal(run: (url: string, changeProfile: () => void) => Promise<void>) {
  let profile: string | null = null;
  const router = new Router();
  const terminal = registerTerminalRoutes(router, {
    connection: { path: null, fileConfig: null, env: {}, profiles: {} },
    readOnly: true,
    pool: new AdminPool(() => {
      throw new Error('No Kafka access expected');
    }),
    producers: new ProducerPool(() => {
      throw new Error('No producer expected');
    }),
    consumerFactory: () => {
      throw new Error('No consumer expected');
    },
    events: new StudioEventBus(),
    getActiveProfile: () => profile,
  });
  const server = createStudioServer({ router, readOnly: true });
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
  const { port } = server.address() as AddressInfo;
  try {
    await run(`http://127.0.0.1:${String(port)}`, () => {
      profile = 'other';
    });
  } finally {
    terminal.dispose();
    server.closeAllConnections();
    await new Promise<void>((resolve) => server.close(() => resolve()));
  }
}
const post = (url: string, body?: unknown) =>
  fetch(url, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: body === undefined ? undefined : JSON.stringify(body),
  });

describe('terminal session HTTP routes', () => {
  it('keeps shell controls available in read-only mode and validates input', async () => {
    await withTerminal(async (url) => {
      const created = await post(`${url}/api/terminal/sessions`);
      expect(created.status).toBe(201);
      const session = (await created.json()) as { id: string; readOnly: boolean };
      expect(session.readOnly).toBe(true);
      expect((await post(`${url}/api/terminal/sessions/${session.id}/resize`, { columns: 40 })).status).toBe(200);
      expect((await post(`${url}/api/terminal/sessions/${session.id}/resize`, { columns: 0 })).status).toBe(400);
      expect((await post(`${url}/api/terminal/sessions/${session.id}/input`, { data: 'x'.repeat(8193) })).status).toBe(
        400,
      );
      expect((await fetch(`${url}/api/terminal/sessions/${session.id}`, { method: 'DELETE' })).status).toBe(200);
      expect((await post(`${url}/api/terminal/sessions/${session.id}/input`, { data: 'help\r' })).status).toBe(404);
    });
  });

  it('streams native CLI output and rejects input after a profile change', async () => {
    await withTerminal(async (url, changeProfile) => {
      const response = await post(`${url}/api/terminal/sessions`);
      const { id } = (await response.json()) as { id: string };
      const controller = new AbortController();
      const events = await fetch(`${url}/api/terminal/sessions/${id}/events`, { signal: controller.signal });
      const reader = events.body?.getReader() as ReadableStreamDefaultReader<Uint8Array> | undefined;
      if (reader === undefined) throw new Error('Missing terminal event stream');
      expect((await post(`${url}/api/terminal/sessions/${id}/input`, { data: 'help\r' })).status).toBe(200);
      let output = '';
      const timeout = setTimeout(() => controller.abort(), 3000);
      try {
        while (!output.includes('Usage:')) {
          const chunk = await reader.read();
          if (chunk.done) break;
          output += new TextDecoder().decode(chunk.value);
        }
        expect(output).toContain('Usage:');
        expect(output).toContain('event: output');
      } finally {
        clearTimeout(timeout);
        controller.abort();
      }
      changeProfile();
      expect((await post(`${url}/api/terminal/sessions/${id}/input`, { data: 'topic list\r' })).status).toBe(409);
    });
  });
});
