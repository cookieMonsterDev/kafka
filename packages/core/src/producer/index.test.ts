import { describe, expect, it, vi } from 'vitest';
import type { Cluster } from '../cluster/index.js';
import { KafkaJSNonRetriableError } from '../errors.js';
import { InstrumentationEventEmitter } from '../instrumentation/emitter.js';
import { createLogger, LOG_LEVELS } from '../loggers/index.js';
import { NETWORK_REQUEST } from '../network/instrumentation-events.js';
import { createProducer } from './index.js';

const silentLogger = createLogger({ level: LOG_LEVELS.NOTHING, logCreator: () => () => {} });

function fakeBroker() {
  return {
    nodeId: 1,
    initProducerId: vi.fn().mockResolvedValue({
      producerId: 1000n,
      producerEpoch: 1,
      errorCode: 0,
      throttleTime: 0,
      clientSideThrottleTime: 0,
    }),
  };
}

function fakeCluster(overrides: Partial<Record<string, unknown>> = {}) {
  const broker = fakeBroker();
  return {
    connect: vi.fn().mockResolvedValue(undefined),
    disconnect: vi.fn().mockResolvedValue(undefined),
    isConnected: vi.fn().mockReturnValue(true),
    refreshMetadataIfNecessary: vi.fn().mockResolvedValue(undefined),
    findControllerBroker: vi.fn().mockResolvedValue(broker),
    findGroupCoordinator: vi.fn().mockResolvedValue(broker),
    markOffsetAsCommitted: vi.fn(),
    ...overrides,
  };
}

describe('producer', () => {
  it('throws when the topic is missing', async () => {
    const producer = createProducer({ cluster: fakeCluster() as unknown as Cluster, logger: silentLogger });
    await expect(producer.send({ acks: 1, topic: '', messages: [] })).rejects.toThrow('Invalid topic');
  });

  it('throws when the messages array is missing', async () => {
    const producer = createProducer({ cluster: fakeCluster() as unknown as Cluster, logger: silentLogger });
    await expect(producer.send({ acks: 1, topic: 'topic', messages: null as never })).rejects.toThrow(
      'Invalid messages array [null] for topic "topic"',
    );
  });

  it('throws for a message with a missing value', async () => {
    const producer = createProducer({ cluster: fakeCluster() as unknown as Cluster, logger: silentLogger });
    await expect(producer.send({ acks: 1, topic: 'topic', messages: [{ key: 'k' } as never] })).rejects.toThrow(
      'Invalid message without value for topic "topic"',
    );
  });

  it('throws when sending while disconnected', async () => {
    const producer = createProducer({ cluster: fakeCluster() as unknown as Cluster, logger: silentLogger });
    await expect(producer.send({ topic: 'topic', messages: [{ key: 'key', value: 'value' }] })).rejects.toThrow(
      'The producer is disconnected',
    );
  });

  it('allows a null message value, for tombstones', async () => {
    const cluster = fakeCluster();
    const producer = createProducer({ cluster: cluster as unknown as Cluster, logger: silentLogger });
    await producer.connect();

    // The fake cluster doesn't implement enough of the real send path to succeed end to end, but
    // it fails for an unrelated reason (a missing mock method) - proving the value: null message
    // itself cleared the "Invalid message without value" validation.
    let rejection: unknown;
    try {
      await producer.send({ topic: 'topic', messages: [{ value: null }] });
    } catch (e) {
      rejection = e;
    }

    expect(rejection).toBeInstanceOf(Error);
    expect((rejection as Error).message).not.toMatch(/Invalid message/);
  });

  it('rejects an unknown event name', () => {
    const producer = createProducer({ cluster: fakeCluster() as unknown as Cluster, logger: silentLogger });
    expect(() => producer.on('NON_EXISTENT_EVENT' as never, () => {})).toThrow(
      /Event name should be one of producer\.events\./,
    );
  });

  it('emits connect/disconnect events', async () => {
    const cluster = fakeCluster();
    const producer = createProducer({ cluster: cluster as unknown as Cluster, logger: silentLogger });
    const connectListener = vi.fn();
    const disconnectListener = vi.fn();
    producer.on(producer.events.CONNECT, connectListener);
    producer.on(producer.events.DISCONNECT, disconnectListener);

    await producer.connect();
    expect(connectListener).toHaveBeenCalled();

    await producer.disconnect();
    expect(disconnectListener).toHaveBeenCalled();
  });

  it('forwards network request events, rewriting the event type back to the public name', async () => {
    const emitter = new InstrumentationEventEmitter();
    const cluster = fakeCluster();
    const producer = createProducer({
      cluster: cluster as unknown as Cluster,
      logger: silentLogger,
      instrumentationEmitter: emitter,
    });

    const requestListener = vi.fn();
    producer.on(producer.events.REQUEST, requestListener);

    emitter.emit(NETWORK_REQUEST, { apiName: 'Produce' });

    expect(requestListener).toHaveBeenCalledWith(
      expect.objectContaining({ type: 'producer.network.request', payload: { apiName: 'Produce' } }),
    );
  });

  it('reports itself as idempotent when configured that way', () => {
    const producer = createProducer({
      cluster: fakeCluster() as unknown as Cluster,
      logger: silentLogger,
      idempotent: true,
    });
    expect(producer.isIdempotent()).toBe(true);
  });

  it('throws when an idempotent producer disallows retries', () => {
    expect(() =>
      createProducer({
        cluster: fakeCluster() as unknown as Cluster,
        logger: silentLogger,
        idempotent: true,
        retry: { retries: 0 },
      }),
    ).toThrow(
      new KafkaJSNonRetriableError('Idempotent producer must allow retries to protect against transient errors'),
    );
  });

  it('only initializes the producer id once for an idempotent producer', async () => {
    const cluster = fakeCluster();
    const producer = createProducer({ cluster: cluster as unknown as Cluster, logger: silentLogger, idempotent: true });

    await producer.connect();
    await producer.connect();

    const broker = (await cluster.findControllerBroker()) as unknown as ReturnType<typeof fakeBroker>;
    expect(broker.initProducerId).toHaveBeenCalledTimes(1);
  });

  it('exposes its namespaced logger', () => {
    const producer = createProducer({ cluster: fakeCluster() as unknown as Cluster, logger: silentLogger });
    expect(producer.logger()).toBeDefined();
  });

  it('rejects connect when the signal is already aborted', async () => {
    const cluster = fakeCluster();
    const producer = createProducer({ cluster: cluster as unknown as Cluster, logger: silentLogger });
    await expect(producer.connect({ signal: AbortSignal.abort() })).rejects.toThrow(/aborted/i);
    expect(cluster.connect).not.toHaveBeenCalled();
  });

  it('disconnects through Symbol.asyncDispose', async () => {
    const cluster = fakeCluster();
    const producer = createProducer({ cluster: cluster as unknown as Cluster, logger: silentLogger });
    await producer[Symbol.asyncDispose]();
    expect(cluster.disconnect).toHaveBeenCalled();
  });

  describe('transaction', () => {
    it('requires a transactionalId', async () => {
      const producer = createProducer({ cluster: fakeCluster() as unknown as Cluster, logger: silentLogger });
      await expect(producer.transaction()).rejects.toEqual(
        new KafkaJSNonRetriableError('Must provide transactional id for transactional producer'),
      );
    });

    it('rejects a second concurrent transaction', async () => {
      const cluster = fakeCluster();
      const producer = createProducer({
        cluster: cluster as unknown as Cluster,
        logger: silentLogger,
        transactionalId: 'txn-id',
      });

      await producer.transaction();
      await expect(producer.transaction()).rejects.toEqual(
        new KafkaJSNonRetriableError(
          'There is already an ongoing transaction for this producer. Please end the transaction before beginning another.',
        ),
      );
    });

    it('rejects every method once the transaction has ended', async () => {
      const cluster = fakeCluster();
      const producer = createProducer({
        cluster: cluster as unknown as Cluster,
        logger: silentLogger,
        transactionalId: 'txn-id',
      });

      const txn = await producer.transaction();
      expect(txn.isActive()).toBe(true);
      await txn.abort();
      expect(txn.isActive()).toBe(false);

      const ended = new KafkaJSNonRetriableError('Cannot continue to use transaction once ended');
      await expect(txn.send({ topic: 't', messages: [] })).rejects.toEqual(ended);
      await expect(txn.sendBatch({ topicMessages: [] })).rejects.toEqual(ended);
      await expect(txn.commit()).rejects.toEqual(ended);
      await expect(txn.abort()).rejects.toEqual(ended);
    });

    it('allows a new transaction after the previous one ended', async () => {
      const cluster = fakeCluster();
      const producer = createProducer({
        cluster: cluster as unknown as Cluster,
        logger: silentLogger,
        transactionalId: 'txn-id',
      });

      const first = await producer.transaction();
      await first.commit();

      await expect(producer.transaction()).resolves.toBeTruthy();
    });
  });
});
