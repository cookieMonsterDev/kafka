import { describe, expect, it, vi } from 'vitest';
import type { Cluster } from '../cluster/index';
import { KafkaNonRetriableError } from '../errors';
import { createLogger, LOG_LEVELS } from '../loggers/index';
import { createConsumer, events } from './index';

const silentLogger = createLogger({ level: LOG_LEVELS.NOTHING, logCreator: () => () => {} });

function fakeCluster(): Cluster {
  return {
    connect: vi.fn(async () => undefined),
    disconnect: vi.fn(async () => undefined),
    addMultipleTargetTopics: vi.fn(async () => undefined),
    metadata: vi.fn(async () => ({ topicMetadata: [] })),
    findGroupCoordinator: vi.fn(),
    removeBroker: vi.fn(),
  } as unknown as Cluster;
}

describe('consumer', () => {
  it('throws when heartbeatInterval is greater than or equal to sessionTimeout', () => {
    expect(() =>
      createConsumer({
        cluster: fakeCluster(),
        logger: silentLogger,
        groupId: 'test-group-id',
        heartbeatInterval: 10_000,
        sessionTimeout: 10_000,
      }),
    ).toThrow(KafkaNonRetriableError);
  });

  it('allows heartbeatInterval >= sessionTimeout when groupProtocol is consumer', () => {
    expect(() =>
      createConsumer({
        cluster: fakeCluster(),
        logger: silentLogger,
        groupId: 'test-group-id',
        heartbeatInterval: 10_000,
        sessionTimeout: 10_000,
        groupProtocol: 'consumer',
      }),
    ).not.toThrow();
  });

  it('does not require groupId at construction (assign() mode may omit it)', () => {
    expect(() =>
      createConsumer({
        cluster: fakeCluster(),
        logger: silentLogger,
      }),
    ).not.toThrow();
  });

  it('subscribe() throws when groupId is missing', async () => {
    const consumer = createConsumer({ cluster: fakeCluster(), logger: silentLogger });
    await expect(consumer.subscribe({ topic: 't' })).rejects.toThrow(
      'Consumer groupId must be a non-empty string to use subscribe().',
    );
  });

  it('exposes a namespaced logger', () => {
    const consumer = createConsumer({
      cluster: fakeCluster(),
      groupId: 'test-consumer',
      logger: silentLogger,
    });
    expect(consumer.logger()).toBeDefined();
    expect(typeof consumer.logger().info).toBe('function');
  });

  it('rejects an unknown event name', () => {
    const consumer = createConsumer({ cluster: fakeCluster(), groupId: 'g', logger: silentLogger });
    expect(() => consumer.on('NON_EXISTENT_EVENT' as never, () => {})).toThrow(
      /Event name should be one of consumer\.events\./,
    );
  });

  it('exposes the public events map', () => {
    const consumer = createConsumer({ cluster: fakeCluster(), groupId: 'g', logger: silentLogger });
    expect(consumer.events).toBe(events);
  });

  it('throws when seeking before run()', () => {
    const consumer = createConsumer({ cluster: fakeCluster(), groupId: 'g', logger: silentLogger });
    expect(() => consumer.seek({ topic: 't', partition: 0, offset: 1n })).toThrow(
      'Consumer group was not initialized, consumer#run must be called first',
    );
  });

  it('throws when committing offsets before run()', async () => {
    const consumer = createConsumer({ cluster: fakeCluster(), groupId: 'g', logger: silentLogger });
    await expect(consumer.commitOffsets([{ topic: 't', partition: 0, offset: 1n }])).rejects.toThrow(
      'Consumer group was not initialized, consumer#run must be called first',
    );
  });

  it('rejects a negative seek offset that is not earliest/latest', () => {
    const consumer = createConsumer({ cluster: fakeCluster(), groupId: 'g', logger: silentLogger });
    expect(() => consumer.seek({ topic: 't', partition: 0, offset: -5n })).toThrow(
      'Offset must not be a negative number',
    );
  });

  it('paused() returns an empty list before run()', () => {
    const consumer = createConsumer({ cluster: fakeCluster(), groupId: 'g', logger: silentLogger });
    expect(consumer.paused()).toEqual([]);
  });

  it('rejects connect when the signal is already aborted', async () => {
    const connect = vi.fn(async () => undefined);
    const cluster = { ...fakeCluster(), connect } as unknown as Cluster;
    const consumer = createConsumer({ cluster, groupId: 'g', logger: silentLogger });
    await expect(consumer.connect({ signal: AbortSignal.abort() })).rejects.toThrow(/aborted/i);
    expect(connect).not.toHaveBeenCalled();
  });

  it('disconnects through Symbol.asyncDispose', async () => {
    const disconnect = vi.fn(async () => undefined);
    const cluster = { ...fakeCluster(), disconnect } as unknown as Cluster;
    const consumer = createConsumer({ cluster, groupId: 'g', logger: silentLogger });
    await consumer[Symbol.asyncDispose]();
    expect(disconnect).toHaveBeenCalled();
  });

  it('matches regexp subscriptions without the global flag advancing lastIndex', async () => {
    const metadata = vi.fn(async () => ({
      topicMetadata: [{ topic: 'foo-one' }, { topic: 'foo-two' }, { topic: 'bar' }],
    }));
    const addMultipleTargetTopics = vi.fn(async () => undefined);
    const cluster = { ...fakeCluster(), metadata, addMultipleTargetTopics } as unknown as Cluster;
    const consumer = createConsumer({ cluster, groupId: 'g', logger: silentLogger });

    await consumer.subscribe({ topics: [/foo.*/g] });

    expect(addMultipleTargetTopics).toHaveBeenCalledWith(['foo-one', 'foo-two']);
  });

  it('still runs the client-side metadata scan for RegExp subscriptions under classic protocol', async () => {
    const metadata = vi.fn(async () => ({
      topicMetadata: [{ topic: 'foo-one' }, { topic: 'bar' }],
    }));
    const addMultipleTargetTopics = vi.fn(async () => undefined);
    const cluster = { ...fakeCluster(), metadata, addMultipleTargetTopics } as unknown as Cluster;
    const consumer = createConsumer({ cluster, groupId: 'g', logger: silentLogger, groupProtocol: 'classic' });

    await consumer.subscribe({ topics: [/foo.*/] });

    expect(metadata).toHaveBeenCalled();
    expect(addMultipleTargetTopics).toHaveBeenCalledWith(['foo-one']);
  });

  it('sends a RegExp subscription server-side instead of scanning metadata when groupProtocol is consumer', async () => {
    const metadata = vi.fn(async () => ({ topicMetadata: [{ topic: 'foo-one' }, { topic: 'bar' }] }));
    const addMultipleTargetTopics = vi.fn(async () => undefined);
    const cluster = { ...fakeCluster(), metadata, addMultipleTargetTopics } as unknown as Cluster;
    const consumer = createConsumer({ cluster, groupId: 'g', logger: silentLogger, groupProtocol: 'consumer' });

    await consumer.subscribe({ topics: [/foo.*/] });

    expect(metadata).not.toHaveBeenCalled();
    expect(addMultipleTargetTopics).toHaveBeenCalledWith([]);
  });

  it('sends literal topic names alongside a RegExp subscription when groupProtocol is consumer', async () => {
    const addMultipleTargetTopics = vi.fn(async () => undefined);
    const cluster = { ...fakeCluster(), addMultipleTargetTopics } as unknown as Cluster;
    const consumer = createConsumer({ cluster, groupId: 'g', logger: silentLogger, groupProtocol: 'consumer' });

    await consumer.subscribe({ topics: ['literal-topic', /foo.*/] });

    expect(addMultipleTargetTopics).toHaveBeenCalledWith(['literal-topic']);
  });

  it('rejects more than one RegExp subscription when groupProtocol is consumer', async () => {
    const cluster = fakeCluster();
    const consumer = createConsumer({ cluster, groupId: 'g', logger: silentLogger, groupProtocol: 'consumer' });

    await expect(consumer.subscribe({ topics: [/foo.*/, /bar.*/] })).rejects.toThrow(
      /Only one RegExp subscription is supported/,
    );
  });

  it('rejects a second, different RegExp subscription across subscribe() calls when groupProtocol is consumer', async () => {
    const addMultipleTargetTopics = vi.fn(async () => undefined);
    const cluster = { ...fakeCluster(), addMultipleTargetTopics } as unknown as Cluster;
    const consumer = createConsumer({ cluster, groupId: 'g', logger: silentLogger, groupProtocol: 'consumer' });

    await consumer.subscribe({ topics: [/foo.*/] });

    await expect(consumer.subscribe({ topics: [/bar.*/] })).rejects.toThrow(
      /Only one RegExp subscription is supported/,
    );
  });

  it('allows re-subscribing with the same RegExp source when groupProtocol is consumer', async () => {
    const addMultipleTargetTopics = vi.fn(async () => undefined);
    const cluster = { ...fakeCluster(), addMultipleTargetTopics } as unknown as Cluster;
    const consumer = createConsumer({ cluster, groupId: 'g', logger: silentLogger, groupProtocol: 'consumer' });

    await consumer.subscribe({ topics: [/foo.*/] });

    await expect(consumer.subscribe({ topics: [/foo.*/] })).resolves.not.toThrow();
  });

  it('stores autoOffsetReset from subscribe options', async () => {
    const addMultipleTargetTopics = vi.fn(async () => undefined);
    const cluster = { ...fakeCluster(), addMultipleTargetTopics } as unknown as Cluster;
    const consumer = createConsumer({ cluster, groupId: 'g', logger: silentLogger });

    await consumer.subscribe({ topics: ['events'], autoOffsetReset: 'none' });

    expect(addMultipleTargetTopics).toHaveBeenCalledWith(['events']);
  });

  it('rejects subscribe when topics is not an array', async () => {
    const consumer = createConsumer({ cluster: fakeCluster(), groupId: 'g', logger: silentLogger });
    await expect(consumer.subscribe({ topics: 'events' as never })).rejects.toThrow(
      'Argument "topics" must be an array',
    );
  });

  it('rejects subscribe when a topic entry is neither a string nor a RegExp', async () => {
    const consumer = createConsumer({ cluster: fakeCluster(), groupId: 'g', logger: silentLogger });
    await expect(consumer.subscribe({ topics: [42 as never] })).rejects.toThrow(/Invalid topic/);
    await expect(consumer.subscribe({ topic: 42 as never })).rejects.toThrow(/Invalid topic/);
  });

  it('rejects subscribe when both topic and topics are missing', async () => {
    const consumer = createConsumer({ cluster: fakeCluster(), groupId: 'g', logger: silentLogger });
    await expect(consumer.subscribe({} as never)).rejects.toThrow('Missing required argument "topics"');
  });

  it('rejects pause and resume before run and on invalid topic/partitions', () => {
    const consumer = createConsumer({ cluster: fakeCluster(), groupId: 'g', logger: silentLogger });
    expect(() => consumer.pause([{ topic: '' }])).toThrow('Invalid topic');
    expect(() => consumer.pause([{ topic: 't', partitions: Number.NaN as never }])).toThrow(
      /Array of valid partitions required to pause/,
    );
    expect(() => consumer.pause([{ topic: 't' }])).toThrow(
      'Consumer group was not initialized, consumer#run must be called first',
    );
    expect(() => consumer.resume([{ topic: '' }])).toThrow('Invalid topic');
    expect(() => consumer.resume([{ topic: 't', partitions: Number.NaN as never }])).toThrow(
      /Array of valid partitions required to resume/,
    );
    expect(() => consumer.resume([{ topic: 't' }])).toThrow(
      'Consumer group was not initialized, consumer#run must be called first',
    );
  });

  it('rejects commitOffsets with a negative offset or non-string metadata', async () => {
    const consumer = createConsumer({ cluster: fakeCluster(), groupId: 'g', logger: silentLogger });
    await expect(consumer.commitOffsets([{ topic: 't', partition: 0, offset: -1n }])).rejects.toThrow(
      'Offset must not be a negative number',
    );
    await expect(
      consumer.commitOffsets([{ topic: 't', partition: 0, offset: 1n, metadata: 1 as never }]),
    ).rejects.toThrow('Invalid offset metadata');
    await expect(consumer.commitOffsets([{ topic: '', partition: 0, offset: 1n }])).rejects.toThrow('Invalid topic');
    await expect(consumer.commitOffsets([{ topic: 't', partition: Number.NaN, offset: 1n }])).rejects.toThrow(
      'Invalid partition',
    );
  });

  it('rejects seek with a missing topic or non-numeric partition', () => {
    const consumer = createConsumer({ cluster: fakeCluster(), groupId: 'g', logger: silentLogger });
    expect(() => consumer.seek({ topic: '', partition: 0, offset: 1n })).toThrow('Invalid topic');
    expect(() => consumer.seek({ topic: 't', partition: Number.NaN, offset: 1n })).toThrow('Invalid partition');
  });

  describe('assign() mode', () => {
    function fakeAssignCluster(overrides: Partial<Cluster> = {}): Cluster {
      return {
        ...fakeCluster(),
        refreshMetadata: vi.fn(async () => undefined),
        refreshMetadataIfNecessary: vi.fn(async () => undefined),
        getNodeIds: vi.fn(() => ['1']),
        findTopicPartitionMetadata: vi.fn(() => [{ partitionId: 0, leader: 1 }]),
        findTopicId: vi.fn(() => undefined),
        findBroker: vi.fn(async () => ({ fetch: vi.fn(async () => ({ responses: [], sessionId: 0 })) })),
        fetchTopicsOffset: vi.fn(async () => [{ topic: 't', partitions: [{ partition: 0, offset: 0n }] }]),
        defaultOffset: vi.fn(() => 0n),
        ...overrides,
      } as unknown as Cluster;
    }

    it('does not require a groupId to be constructed or to call assign()', async () => {
      const cluster = fakeAssignCluster();
      const consumer = createConsumer({ cluster, logger: silentLogger });
      await expect(consumer.assign([{ topic: 't', partition: 0 }])).resolves.toBeUndefined();
    });

    it('groups and sorts partitions by topic when registering target topics', async () => {
      const addMultipleTargetTopics = vi.fn(async () => undefined);
      const cluster = fakeAssignCluster({ addMultipleTargetTopics });
      const consumer = createConsumer({ cluster, logger: silentLogger });

      await consumer.assign([
        { topic: 'b', partition: 1 },
        { topic: 'a', partition: 2 },
        { topic: 'a', partition: 0 },
      ]);

      expect(addMultipleTargetTopics).toHaveBeenCalledWith(['b', 'a']);
    });

    it('rejects assign() with an invalid topic or non-numeric partition', async () => {
      const consumer = createConsumer({ cluster: fakeAssignCluster(), logger: silentLogger });
      await expect(consumer.assign([{ topic: '', partition: 0 }])).rejects.toThrow('Invalid topic');
      await expect(consumer.assign([{ topic: 't', partition: Number.NaN }])).rejects.toThrow('Invalid partition');
      await expect(consumer.assign('not-an-array' as never)).rejects.toThrow(
        'Argument "topicPartitions" must be an array',
      );
    });

    it('assign() and subscribe() are mutually exclusive', async () => {
      const consumerAssignFirst = createConsumer({ cluster: fakeAssignCluster(), groupId: 'g', logger: silentLogger });
      await consumerAssignFirst.assign([{ topic: 't', partition: 0 }]);
      await expect(consumerAssignFirst.subscribe({ topic: 't' })).rejects.toThrow(
        'Cannot call subscribe() after assign()',
      );

      const consumerSubscribeFirst = createConsumer({
        cluster: fakeAssignCluster(),
        groupId: 'g',
        logger: silentLogger,
      });
      await consumerSubscribeFirst.subscribe({ topic: 't' });
      await expect(consumerSubscribeFirst.assign([{ topic: 't', partition: 0 }])).rejects.toThrow(
        'Cannot call assign() after subscribe()',
      );
    });

    it('run() throws when neither subscribe() nor assign() was called', async () => {
      const consumer = createConsumer({ cluster: fakeAssignCluster(), logger: silentLogger });
      await expect(consumer.run()).rejects.toThrow('Consumer must call subscribe() or assign() before run().');
    });

    it('stream() throws when neither subscribe() nor assign() was called', async () => {
      const consumer = createConsumer({ cluster: fakeAssignCluster(), logger: silentLogger });
      const iterator = consumer.stream()[Symbol.asyncIterator]();
      await expect(iterator.next()).rejects.toThrow('Consumer must call subscribe() or assign() before run().');
    });

    it('fetches without any JoinGroup/group-membership RPC and never requires a groupId', async () => {
      const findGroupCoordinator = vi.fn();
      const cluster = fakeAssignCluster({ findGroupCoordinator });
      const consumer = createConsumer({ cluster, logger: silentLogger });

      await consumer.assign([{ topic: 't', partition: 0 }]);
      await consumer.run();
      try {
        expect(findGroupCoordinator).not.toHaveBeenCalled();
      } finally {
        await consumer.stop();
      }
    });

    it('pause/resume/seek work in assign mode', async () => {
      const consumer = createConsumer({ cluster: fakeAssignCluster(), logger: silentLogger });
      await consumer.assign([{ topic: 't', partition: 0 }]);
      await consumer.run();

      try {
        consumer.pause([{ topic: 't', partitions: [0] }]);
        expect(consumer.paused()).toEqual([{ topic: 't', partitions: [0] }]);

        consumer.resume([{ topic: 't', partitions: [0] }]);
        expect(consumer.paused()).toEqual([]);

        expect(() => consumer.seek({ topic: 't', partition: 0, offset: 10n })).not.toThrow();
      } finally {
        await consumer.stop();
      }
    });

    it('commitOffsets() throws a clear error in assign mode without a configured groupId', async () => {
      const consumer = createConsumer({ cluster: fakeAssignCluster(), logger: silentLogger });
      await consumer.assign([{ topic: 't', partition: 0 }]);
      await consumer.run();

      try {
        await expect(consumer.commitOffsets([{ topic: 't', partition: 0, offset: 5n }])).rejects.toThrow(
          'Cannot commit offsets in assign() mode without a configured groupId',
        );
      } finally {
        await consumer.stop();
      }
    });

    it('commitOffsets() works in assign mode when a groupId is configured', async () => {
      const offsetCommit = vi.fn(async () => undefined);
      const findGroupCoordinator = vi.fn(async () => ({ offsetCommit, isConnected: () => true }));
      const cluster = fakeAssignCluster({ findGroupCoordinator: findGroupCoordinator as never });
      const consumer = createConsumer({ cluster, groupId: 'my-group', logger: silentLogger });
      await consumer.assign([{ topic: 't', partition: 0 }]);
      await consumer.run();

      try {
        await consumer.commitOffsets([{ topic: 't', partition: 0, offset: 5n }]);
        expect(offsetCommit).toHaveBeenCalledWith(
          expect.objectContaining({ groupId: 'my-group', memberId: '', groupGenerationId: -1 }),
        );
      } finally {
        await consumer.stop();
      }
    });
  });

  describe('committed', () => {
    it('returns [] without an RPC when no partitions are requested', async () => {
      const findGroupCoordinator = vi.fn();
      const cluster = { ...fakeCluster(), findGroupCoordinator } as unknown as Cluster;
      const consumer = createConsumer({ cluster, groupId: 'g', logger: silentLogger });

      await expect(consumer.committed([])).resolves.toEqual([]);
      expect(findGroupCoordinator).not.toHaveBeenCalled();
    });

    it('fetches committed offsets from the group coordinator, grouped by topic', async () => {
      const offsetFetch = vi.fn(async () => ({
        responses: [
          {
            topic: 't',
            partitions: [
              { partition: 0, offset: 5n, metadata: 'meta', errorCode: 0 },
              { partition: 1, offset: -1n, metadata: null, errorCode: 0 },
            ],
          },
        ],
      }));
      const findGroupCoordinator = vi.fn(async () => ({ offsetFetch }));
      const cluster = { ...fakeCluster(), findGroupCoordinator } as unknown as Cluster;
      const consumer = createConsumer({ cluster, groupId: 'g', logger: silentLogger });

      const result = await consumer.committed([
        { topic: 't', partition: 0 },
        { topic: 't', partition: 1 },
      ]);

      expect(findGroupCoordinator).toHaveBeenCalledWith({ groupId: 'g' });
      expect(offsetFetch).toHaveBeenCalledWith({
        groupId: 'g',
        topics: [{ topic: 't', partitions: [{ partition: 0 }, { partition: 1 }] }],
      });
      expect(result).toEqual([
        { topic: 't', partition: 0, offset: 5n, metadata: 'meta' },
        { topic: 't', partition: 1, offset: -1n, metadata: null },
      ]);
    });

    it('normalizes empty offset metadata to null', async () => {
      const offsetFetch = vi.fn(async () => ({
        responses: [{ topic: 't', partitions: [{ partition: 0, offset: 7n, metadata: '', errorCode: 0 }] }],
      }));
      const findGroupCoordinator = vi.fn(async () => ({ offsetFetch }));
      const cluster = { ...fakeCluster(), findGroupCoordinator } as unknown as Cluster;
      const consumer = createConsumer({ cluster, groupId: 'g', logger: silentLogger });

      await expect(consumer.committed([{ topic: 't', partition: 0 }])).resolves.toEqual([
        { topic: 't', partition: 0, offset: 7n, metadata: null },
      ]);
    });

    it('defaults a partition missing from the response to offset -1n and metadata null', async () => {
      const offsetFetch = vi.fn(async () => ({ responses: [] }));
      const findGroupCoordinator = vi.fn(async () => ({ offsetFetch }));
      const cluster = { ...fakeCluster(), findGroupCoordinator } as unknown as Cluster;
      const consumer = createConsumer({ cluster, groupId: 'g', logger: silentLogger });

      const result = await consumer.committed([{ topic: 't', partition: 0 }]);

      expect(result).toEqual([{ topic: 't', partition: 0, offset: -1n, metadata: null }]);
    });

    it('rejects an invalid topic or partition', async () => {
      const consumer = createConsumer({ cluster: fakeCluster(), groupId: 'g', logger: silentLogger });
      await expect(consumer.committed([{ topic: '', partition: 0 }])).rejects.toThrow('Invalid topic');
      await expect(consumer.committed([{ topic: 't', partition: Number.NaN }])).rejects.toThrow('Invalid partition');
    });
  });

  describe('position', () => {
    it('throws when the consumer group has not started', () => {
      const consumer = createConsumer({ cluster: fakeCluster(), groupId: 'g', logger: silentLogger });
      expect(() => consumer.position({ topic: 't', partition: 0 })).toThrow(
        'Consumer group was not initialized, consumer#run must be called first',
      );
    });

    it('rejects an invalid topic or partition', () => {
      const consumer = createConsumer({ cluster: fakeCluster(), groupId: 'g', logger: silentLogger });
      expect(() => consumer.position({ topic: '', partition: 0 })).toThrow('Invalid topic');
      expect(() => consumer.position({ topic: 't', partition: Number.NaN })).toThrow('Invalid partition');
    });
  });

  describe('currentLag', () => {
    it('throws when the consumer group has not started', () => {
      const consumer = createConsumer({ cluster: fakeCluster(), groupId: 'g', logger: silentLogger });
      expect(() => consumer.currentLag({ topic: 't', partition: 0 })).toThrow(
        'Consumer group was not initialized, consumer#run must be called first',
      );
    });

    it('rejects an invalid topic or partition', () => {
      const consumer = createConsumer({ cluster: fakeCluster(), groupId: 'g', logger: silentLogger });
      expect(() => consumer.currentLag({ topic: '', partition: 0 })).toThrow('Invalid topic');
      expect(() => consumer.currentLag({ topic: 't', partition: Number.NaN })).toThrow('Invalid partition');
    });
  });
});
