import { describe, expect, it, vi } from 'vitest';
import type { ProducerRecord, RecordMetadata } from '@cookiemonsterdev/kafka-core';
import { createFakeProducer } from './create-fake-producer';
import { BurstJobManager, ProducerPool, sendMessages, type ProducerClientFactory } from './produce';

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

describe('ProducerPool', () => {
  function fakeFactory(): {
    factory: ProducerClientFactory;
    connects: ReturnType<typeof vi.fn>;
    disconnects: ReturnType<typeof vi.fn>;
  } {
    const connects = vi.fn(async () => {});
    const disconnects = vi.fn(async () => {});
    const factory: ProducerClientFactory = () => ({
      producer: () => createFakeProducer({ connect: connects, disconnect: disconnects }),
    });
    return { factory, connects, disconnects };
  }

  it('builds and connects a producer on the first get() for a profile', async () => {
    const { factory, connects } = fakeFactory();
    const pool = new ProducerPool(factory);

    await pool.get('staging');
    expect(connects).toHaveBeenCalledOnce();
  });

  it('reuses the same connecting producer for repeated calls with the same profile', async () => {
    const created = vi.fn();
    const factory: ProducerClientFactory = (profileName) => {
      created(profileName);
      return { producer: () => createFakeProducer({ connect: async () => {}, disconnect: async () => {} }) };
    };
    const pool = new ProducerPool(factory);

    const [first, second] = await Promise.all([pool.get('staging'), pool.get('staging')]);
    expect(first).toBe(second);
    expect(created).toHaveBeenCalledOnce();
  });

  it('pools null and a named profile separately', async () => {
    const created = vi.fn();
    const factory: ProducerClientFactory = (profileName) => {
      created(profileName);
      return { producer: () => createFakeProducer({ connect: async () => {}, disconnect: async () => {} }) };
    };
    const pool = new ProducerPool(factory);

    await pool.get(null);
    await pool.get('staging');
    expect(created).toHaveBeenCalledTimes(2);
  });

  it('forgets a profile whose connect() rejected, so the next get() retries', async () => {
    let attempt = 0;
    const factory: ProducerClientFactory = () => ({
      producer: () =>
        createFakeProducer({
          connect: async () => {
            attempt += 1;
            if (attempt === 1) throw new Error('connect failed');
          },
          disconnect: async () => {},
        }),
    });
    const pool = new ProducerPool(factory);

    await expect(pool.get('staging')).rejects.toThrow('connect failed');
    await pool.get('staging');
    expect(attempt).toBe(2);
  });

  it('invalidate() disconnects and forgets a pooled producer', async () => {
    const { factory, disconnects } = fakeFactory();
    const pool = new ProducerPool(factory);

    await pool.get('staging');
    await pool.invalidate('staging');
    expect(disconnects).toHaveBeenCalledOnce();
  });

  it('invalidate() on a profile that was never pooled is a no-op', async () => {
    const { factory } = fakeFactory();
    const pool = new ProducerPool(factory);
    await expect(pool.invalidate('never-pooled')).resolves.toBeUndefined();
  });

  it('disposeAll() disconnects every pooled producer and clears the pool', async () => {
    const { factory, disconnects } = fakeFactory();
    const pool = new ProducerPool(factory);

    await pool.get(null);
    await pool.get('staging');
    await pool.disposeAll();
    expect(disconnects).toHaveBeenCalledTimes(2);
  });
});

describe('sendMessages', () => {
  it('maps produce messages to core messages and offsets to decimal strings', async () => {
    const send = vi.fn(async () => [metadata({ partition: 2, baseOffset: 42n })]);
    const producer = createFakeProducer({ send });

    const response = await sendMessages(producer, {
      topic: 'orders',
      messages: [{ key: 'k', value: 'v', partition: 2, headers: { 'x-test': '1' }, timestamp: 1000 }],
    });

    expect(send).toHaveBeenCalledWith({
      topic: 'orders',
      messages: [{ key: 'k', value: 'v', partition: 2, headers: { 'x-test': '1' }, timestamp: 1000 }],
    });
    expect(response).toEqual({ results: [{ partition: 2, offset: '42' }] });
  });

  it('omits optional fields the message did not set', async () => {
    const send = vi.fn(async () => [metadata()]);
    const producer = createFakeProducer({ send });

    await sendMessages(producer, { topic: 'orders', messages: [{ value: 'v' }] });

    expect(send).toHaveBeenCalledWith({ topic: 'orders', messages: [{ value: 'v' }] });
  });

  it('passes acks through when given', async () => {
    const send = vi.fn(async () => [metadata()]);
    const producer = createFakeProducer({ send });

    await sendMessages(producer, { topic: 'orders', messages: [{ value: 'v' }], acks: -1 });

    expect(send).toHaveBeenCalledWith(expect.objectContaining({ acks: -1 }));
  });
});

describe('BurstJobManager', () => {
  it('runs a burst to completion and reports the final progress', async () => {
    const sent: string[] = [];
    const send = vi.fn(async (record: ProducerRecord) => {
      sent.push(String(record.messages[0]?.value ?? ''));
      return [metadata()];
    });
    const producer = createFakeProducer({ send });
    const manager = new BurstJobManager();

    const job = manager.start(producer, {
      topic: 'orders',
      template: { value: 'msg-{{seq}}' },
      count: 3,
      ratePerSecond: 1000,
    });
    const final = await new Promise((resolve) => {
      const unsubscribe = job.onProgress((progress) => {
        if (progress.status !== 'running') {
          unsubscribe();
          resolve(progress);
        }
      });
    });

    expect(final).toEqual({ jobId: job.id, sent: 3, total: 3, status: 'completed' });
    expect(sent).toEqual(['msg-0', 'msg-1', 'msg-2']);
    expect(manager.get(job.id)).toBe(job);
  });

  it('reports failure when the producer rejects', async () => {
    const producer = createFakeProducer({
      send: async () => {
        throw new Error('broker unreachable');
      },
    });
    const manager = new BurstJobManager();

    const job = manager.start(producer, { topic: 'orders', template: { value: 'v' }, count: 5, ratePerSecond: 1000 });
    const final = await new Promise((resolve) => {
      const unsubscribe = job.onProgress((progress) => {
        if (progress.status !== 'running') {
          unsubscribe();
          resolve(progress);
        }
      });
    });

    expect(final).toEqual({ jobId: job.id, sent: 0, total: 5, status: 'failed', error: 'broker unreachable' });
  });

  it('cancels a running job before it sends every message', async () => {
    let callCount = 0;
    const producer = createFakeProducer({
      send: async () => {
        callCount += 1;
        return [metadata()];
      },
    });
    const manager = new BurstJobManager();

    const job = manager.start(producer, {
      topic: 'orders',
      template: { value: 'v' },
      count: 1000,
      ratePerSecond: 100,
    });

    const finalPromise = new Promise((resolve) => {
      const unsubscribe = job.onProgress((progress) => {
        if (progress.status !== 'running') {
          unsubscribe();
          resolve(progress);
        }
      });
    });

    expect(manager.cancel(job.id)).toBe(true);
    const final = await finalPromise;

    expect(final).toMatchObject({ status: 'cancelled' });
    expect(callCount).toBeLessThan(1000);
  });

  it('cancel() returns false for an unknown job', () => {
    const manager = new BurstJobManager();
    expect(manager.cancel('missing')).toBe(false);
  });

  it('notifies onSent with the topic and an estimated byte count as records go out', async () => {
    const producer = createFakeProducer({ send: async () => [metadata()] });
    const onSent = vi.fn();
    const manager = new BurstJobManager(onSent);

    const job = manager.start(producer, { topic: 'orders', template: { value: 'msg' }, count: 3, ratePerSecond: 1000 });
    await new Promise((resolve) => {
      const unsubscribe = job.onProgress((progress) => {
        if (progress.status !== 'running') {
          unsubscribe();
          resolve(undefined);
        }
      });
    });

    expect(onSent).toHaveBeenCalledWith({ topic: 'orders', count: 3, bytes: 9 });
  });
});
