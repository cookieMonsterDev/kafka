import type { AddressInfo } from 'node:net';
import { describe, expect, it } from 'vitest';
import { createStudioServer } from './create-server';
import { DebugOperations } from './debug-operation';
import { readJsonBody } from './json';
import { Router } from './router';

async function withOperations(run: (url: string, finished: Promise<void>) => Promise<void>) {
  const operations = new DebugOperations(() => 'staging');
  const router = new Router();
  let finish = () => {};
  const finished = new Promise<void>((resolve) => {
    finish = resolve;
  });
  router.post('/api/run/:kind', async (req, res, params) => {
    await readJsonBody(req);
    await operations.run(
      req,
      res,
      async ({ profile, signal, stream }) => {
        if (params.kind === 'fail') throw new Error('work failed');
        if (params.kind === 'wait' || params.kind === 'timeout') {
          await new Promise<void>((resolve) => signal.addEventListener('abort', () => resolve(), { once: true }));
        }
        stream.send('result', { profile, aborted: signal.aborted });
      },
      { failureMessage: 'Operation failed', ...(params.kind === 'timeout' ? { timeoutMs: 10 } : {}) },
    );
    if (params.kind === 'wait') finish();
  });
  const server = createStudioServer({ router });
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
  try {
    await run(`http://127.0.0.1:${String((server.address() as AddressInfo).port)}`, finished);
  } finally {
    server.closeAllConnections();
    await new Promise<void>((resolve) => server.close(() => resolve()));
  }
}

function post(url: string, kind: string) {
  return fetch(`${url}/api/run/${kind}`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: '{}',
  });
}

describe('DebugOperations', () => {
  it('reports work failures and releases admission for subsequent operations', async () => {
    await withOperations(async (url) => {
      for (let attempt = 0; attempt < 5; attempt++) {
        const response = await post(url, 'fail');
        expect(response.status).toBe(200);
        expect(await response.text()).toContain('event: operation-error\ndata: {"message":"work failed"}');
      }
      expect(await (await post(url, 'complete')).text()).toContain('"profile":"staging"');
    });
  });

  it('limits concurrent work and releases a slot when a POST response disconnects', async () => {
    await withOperations(async (url, finished) => {
      const held = await Promise.all(Array.from({ length: 4 }, () => post(url, 'wait')));
      const refused = await post(url, 'complete');
      expect(refused.status).toBe(500);
      expect(await refused.text()).toContain('Four debugger operations');
      await held[0]?.body?.cancel();
      await finished;
      const next = await post(url, 'complete');
      expect(next.status).toBe(200);
      expect(await next.text()).toContain('event: result');
      await Promise.all(
        held.slice(1).map(async (response) => {
          await response.body?.cancel();
        }),
      );
    });
  });

  it('propagates an operation deadline without closing on POST body consumption', async () => {
    await withOperations(async (url) => {
      const response = await post(url, 'timeout');
      expect(await response.text()).toContain('"aborted":true');
    });
  });
});
