import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import { BenchmarkAttempts } from './attempt.ts';
import type { BrokerHandle, ClientContainerOptions, ClientContainerOutcome } from './types.ts';

const broker: BrokerHandle = {
  containerName: 'broker',
  network: 'test',
  bootstrap: 'broker:9092',
  image: 'test',
  imageDigest: null,
};
const target = { broker, topic: 'test-attempt', partitions: 1 };
const options: ClientContainerOptions = {
  image: 'test',
  spec: {},
  network: 'test',
  cpuset: '0',
  memoryBytes: 1024,
  timeoutMs: 1000,
};
const outcome: ClientContainerOutcome = {
  result: null,
  exitCode: 0,
  timedOut: false,
  wallMs: 1,
  stdoutTail: '',
  stderrTail: '',
};

function fixture(overrides: Partial<ConstructorParameters<typeof BenchmarkAttempts>[0]> = {}) {
  const events: string[] = [];
  const attempts = new BenchmarkAttempts({
    createTopic: async () => {
      events.push('create');
    },
    deleteTopic: async () => {
      events.push('delete');
    },
    runClientContainer: async () => {
      events.push('client');
      return outcome;
    },
    ...overrides,
  });
  return { attempts, events };
}

describe('benchmark attempt ownership', () => {
  it('disposes a successful attempt before returning its value', async () => {
    const { attempts, events } = fixture();
    const result = await attempts.run(target, async ({ runClient }) => runClient(options));
    assert.deepEqual(result, { ok: true, value: outcome, cleanupErrors: [] });
    assert.deepEqual(events, ['create', 'client', 'delete']);
  });

  it('disposes a partially created topic without replacing the setup error', async () => {
    const failure = new Error('leader readiness failed');
    const { attempts, events } = fixture({
      createTopic: async () => {
        throw failure;
      },
    });
    const result = await attempts.run(target, async () => assert.fail('must not execute'));
    assert.equal(result.ok, false);
    if (!result.ok) assert.equal(result.error, failure);
    assert.deepEqual(events, ['delete']);
  });

  it('reports cleanup failure separately from the original execution failure', async () => {
    const failure = new Error('seed failed');
    const { attempts } = fixture({
      deleteTopic: async () => {
        throw new Error('broker unavailable');
      },
    });
    const result = await attempts.run(target, async () => {
      throw failure;
    });
    assert.equal(result.ok, false);
    if (!result.ok) assert.equal(result.error, failure);
    assert.deepEqual(result.cleanupErrors, ['cannot delete topic test-attempt: broker unavailable']);
  });

  it('reports cleanup failure even when the measurement succeeds', async () => {
    const { attempts } = fixture({
      deleteTopic: async () => {
        throw new Error('delete failed');
      },
    });
    const result = await attempts.run(target, async () => 42);
    assert.equal(result.ok, true);
    assert.equal(result.cleanupErrors.length, 1);
  });

  it('collects container disposal errors without replacing the client failure', async () => {
    const failure = new Error('cancelled');
    const { attempts } = fixture({
      runClientContainer: async ({ onCleanupError }) => {
        onCleanupError?.(new Error('cannot remove container'));
        throw failure;
      },
    });
    const result = await attempts.run(target, ({ runClient }) => runClient(options));
    assert.equal(result.ok, false);
    if (!result.ok) assert.equal(result.error, failure);
    assert.deepEqual(result.cleanupErrors, ['cannot dispose client container: cannot remove container']);
  });

  it('waits for topic acquisition before deleting on repeated cancellation', async () => {
    const creating = Promise.withResolvers<void>();
    const { attempts, events } = fixture({ createTopic: () => creating.promise });
    const running = attempts.run(target, async () => assert.fail('must not execute'));
    const cancelled = attempts.cancelAll();
    const cancelledAgain = attempts.cancelAll();
    assert.deepEqual(events, []);
    creating.resolve();
    await Promise.all([cancelled, cancelledAgain]);
    assert.equal((await running).ok, false);
    assert.deepEqual(events, ['delete']);
    await assert.rejects(
      attempts.run(target, async () => undefined),
      /stopping/,
    );
  });

  it('prevents a container from starting after cancellation during seeding', async () => {
    const seeding = Promise.withResolvers<void>();
    const seeded = Promise.withResolvers<void>();
    const { attempts, events } = fixture();
    const running = attempts.run(target, async ({ runClient }) => {
      seeded.resolve();
      await seeding.promise;
      return runClient(options);
    });
    await seeded.promise;
    const cancelled = attempts.cancelAll();
    seeding.resolve();
    await cancelled;
    assert.equal((await running).ok, false);
    assert.deepEqual(events, ['create', 'delete']);
  });

  it('cancels a probe container and waits for its disposal before deleting its topic', async () => {
    const started = Promise.withResolvers<void>();
    const disposed = Promise.withResolvers<void>();
    const { attempts, events } = fixture({
      runClientContainer: async ({ signal }) => {
        assert.ok(signal);
        started.resolve();
        await new Promise<void>((resolve) => signal.addEventListener('abort', () => resolve(), { once: true }));
        events.push('cancel-client');
        await disposed.promise;
        events.push('disposed-client');
        signal.throwIfAborted();
        return outcome;
      },
    });
    const running = attempts.run(target, ({ runClient }) => runClient({ ...options, args: ['probe'] }));
    await started.promise;
    const cancelled = attempts.cancelAll();
    await Promise.resolve();
    assert.deepEqual(events, ['create', 'cancel-client']);
    disposed.resolve();
    await cancelled;
    assert.equal((await running).ok, false);
    assert.deepEqual(events, ['create', 'cancel-client', 'disposed-client', 'delete']);
  });
});
