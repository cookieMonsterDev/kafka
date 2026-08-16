import { describe, expect, it, vi } from 'vitest';
import type { Cluster } from '../cluster/index.js';
import { InstrumentationEventEmitter } from '../instrumentation/emitter.js';
import { createLogger, LOG_LEVELS } from '../loggers/index.js';
import { ISOLATION_LEVEL } from '../protocol/enums/isolation-level.js';
import { ConsumerGroup } from './consumer-group.js';
import type { OffsetManager } from './offset-manager/index.js';

const silentLogger = createLogger({ level: LOG_LEVELS.NOTHING, logCreator: () => () => {} });

function createGroup(): ConsumerGroup {
  return new ConsumerGroup({
    logger: silentLogger,
    topics: ['topic1'],
    topicConfigurations: {},
    cluster: {} as Cluster,
    groupId: 'group',
    assigners: [],
    sessionTimeout: 30_000,
    rebalanceTimeout: 60_000,
    maxBytesPerPartition: 1024,
    minBytes: 1,
    maxBytes: 1024,
    maxWaitTimeInMs: 100,
    instrumentationEmitter: new InstrumentationEventEmitter(),
    isolationLevel: ISOLATION_LEVEL.READ_COMMITTED,
    rackId: '',
    metadataMaxAge: 300_000,
    autoCommit: true,
    autoCommitInterval: null,
    autoCommitThreshold: null,
  });
}

describe('consumer/consumer-group', () => {
  it("delegates uncommittedOffsets to the offset manager", () => {
    const consumerGroup = createGroup();
    const mockOffsets = { topics: [] };
    const uncommittedOffsets = vi.fn(() => mockOffsets);
    consumerGroup.offsetManager = { uncommittedOffsets } as unknown as OffsetManager;

    expect(consumerGroup.uncommittedOffsets()).toStrictEqual(mockOffsets);
    expect(uncommittedOffsets).toHaveBeenCalled();
  });

  it("delegates commitOffsets to the offset manager", async () => {
    const consumerGroup = createGroup();
    const commitOffsets = vi.fn(async () => undefined);
    consumerGroup.offsetManager = { commitOffsets } as unknown as OffsetManager;

    const offsets = { topics: [{ topic: 'topic1', partitions: [{ offset: 0n, partition: 0 }] }] };
    await consumerGroup.commitOffsets(offsets);
    expect(commitOffsets).toHaveBeenCalledTimes(1);
    expect(commitOffsets).toHaveBeenCalledWith(offsets);
  });
});
