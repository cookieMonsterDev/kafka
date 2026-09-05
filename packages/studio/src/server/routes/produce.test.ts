import type { AddressInfo } from 'node:net';
import { describe, expect, it, vi } from 'vitest';
import type { RecordMetadata } from '@cookiemonsterdev/kafka-core';
import { createStudioServer } from '../create-server';
import { createFakeProducer, type FakeProducerOverrides } from '../kafka/create-fake-producer';
import { BurstJobManager, ProducerPool } from '../kafka/produce';
import { Router } from '../router';
import { registerProduceRoutes, type ProduceRouteContext } from './produce';

function metadata(overrides: Partial<RecordMetadata> = {}): RecordMetadata {
  return {
    topicName: 'orders',
    partition: 0,
    errorCode: 0,
    baseOffset: 0n,
    logAppendTime: -1n,
    logStartOffset: 0n,
    ...overrides,
  };
}

function buildContext(overrides: FakeProducerOverrides = {}): ProduceRouteContext {
  const producers = new ProducerPool(() => ({
    producer: () => createFakeProducer({ connect: async () => {}, disconnect: async () => {}, ...overrides }),
  }));
  return { producers, jobs: new BurstJobManager(), getActiveProfile: () => null };
}

async function withServer<T>(context: ProduceRouteContext, run: (baseUrl: string) => Promise<T>): Promise<T> {
  const router = new Router();
  registerProduceRoutes(router, context);
  const server = createStudioServer({ router });

  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
  const { port } = server.address() as AddressInfo;
  try {
    return await run(`http://127.0.0.1:${String(port)}`);
  } finally {
    await new Promise<void>((resolve) => server.close(() => resolve()));
  }
}

describe('registerProduceRoutes', () => {
  it('POST /api/produce sends messages and returns partition/offset results', async () => {
    const send = vi.fn(async () => [metadata({ partition: 1, baseOffset: 7n })]);
    const context = buildContext({ send });

    await withServer(context, async (baseUrl) => {
      const res = await fetch(`${baseUrl}/api/produce`, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ topic: 'orders', messages: [{ value: 'hello' }] }),
      });
      expect(res.status).toBe(200);
      await expect(res.json()).resolves.toEqual({ results: [{ partition: 1, offset: '7' }] });
    });

    expect(send).toHaveBeenCalledWith({ topic: 'orders', messages: [{ value: 'hello' }] });
  });

  it('POST /api/produce rejects an invalid body with 400', async () => {
    const context = buildContext();

    await withServer(context, async (baseUrl) => {
      const res = await fetch(`${baseUrl}/api/produce`, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ topic: 'orders', messages: [] }),
      });
      expect(res.status).toBe(400);
    });
  });

  it('POST /api/produce/burst starts a job and returns its id', async () => {
    const send = vi.fn(async () => [metadata()]);
    const context = buildContext({ send });

    await withServer(context, async (baseUrl) => {
      const res = await fetch(`${baseUrl}/api/produce/burst`, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ topic: 'orders', template: { value: 'v' }, count: 2 }),
      });
      expect(res.status).toBe(202);
      const body = (await res.json()) as { jobId: string };
      expect(typeof body.jobId).toBe('string');
      expect(context.jobs.get(body.jobId)).toBeDefined();
    });
  });

  it('POST /api/produce/burst rejects an invalid body with 400', async () => {
    const context = buildContext();

    await withServer(context, async (baseUrl) => {
      const res = await fetch(`${baseUrl}/api/produce/burst`, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ topic: 'orders', template: { value: 'v' }, count: 0 }),
      });
      expect(res.status).toBe(400);
    });
  });

  it('GET /api/produce/burst/:jobId streams progress and closes on completion', async () => {
    const send = vi.fn(async () => [metadata()]);
    const context = buildContext({ send });
    const job = context.jobs.start(await context.producers.get(null), {
      topic: 'orders',
      template: { value: 'v' },
      count: 1,
      ratePerSecond: 1000,
    });

    await withServer(context, async (baseUrl) => {
      const res = await fetch(`${baseUrl}/api/produce/burst/${job.id}`);
      expect(res.status).toBe(200);
      expect(res.headers.get('content-type')).toBe('text/event-stream');
      const body = await res.text();
      expect(body).toContain('event: progress');
      expect(body).toContain('"status":"completed"');
    });
  });

  it('GET /api/produce/burst/:jobId returns 404 for an unknown job', async () => {
    const context = buildContext();

    await withServer(context, async (baseUrl) => {
      const res = await fetch(`${baseUrl}/api/produce/burst/missing`);
      expect(res.status).toBe(404);
    });
  });

  it('DELETE /api/produce/burst/:jobId cancels a running job', async () => {
    const context = buildContext({ send: async () => [metadata()] });
    const job = context.jobs.start(await context.producers.get(null), {
      topic: 'orders',
      template: { value: 'v' },
      count: 1000,
      ratePerSecond: 50,
    });

    await withServer(context, async (baseUrl) => {
      const res = await fetch(`${baseUrl}/api/produce/burst/${job.id}`, { method: 'DELETE' });
      expect(res.status).toBe(202);
      await expect(res.json()).resolves.toEqual({ jobId: job.id });
    });

    await new Promise<void>((resolve) => {
      if (job.snapshot().status !== 'running') {
        resolve();
        return;
      }
      const unsubscribe = job.onProgress((progress) => {
        if (progress.status !== 'running') {
          unsubscribe();
          resolve();
        }
      });
    });
    expect(job.snapshot().status).toBe('cancelled');
  });

  it('DELETE /api/produce/burst/:jobId returns 404 for an unknown job', async () => {
    const context = buildContext();

    await withServer(context, async (baseUrl) => {
      const res = await fetch(`${baseUrl}/api/produce/burst/missing`, { method: 'DELETE' });
      expect(res.status).toBe(404);
    });
  });
});
