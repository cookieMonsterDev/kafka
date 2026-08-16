import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { Broker } from '../../../src/broker/index';
import { COMPRESSION_TYPES } from '../../../src/protocol/compression/index';
import {
  advertisedAddress,
  createConnectionPool,
  createTopic,
  newLogger,
  retryProtocol,
  secureRandom,
  TRANSIENT_METADATA_ERRORS,
} from '../../helpers/index';

const timestamp = 1_509_928_155_660;

describe('broker.produceFetch', () => {
  let topicName: string;
  let broker: Broker | undefined;
  let leader: Broker | undefined;

  beforeEach(async () => {
    topicName = `test-topic-${secureRandom()}`;
    broker = new Broker({ connectionPool: createConnectionPool(), logger: newLogger() });
    await broker.connect();
    await createTopic({ topic: topicName });
  });

  afterEach(async () => {
    await leader?.disconnect();
    await broker?.disconnect();
  });

  async function connectToLeader(): Promise<Broker> {
    const metadata = await retryProtocol(TRANSIENT_METADATA_ERRORS, () => broker!.metadata([topicName]));
    const partition = metadata.topicMetadata[0]!.partitionMetadata[0]!;
    const brokerData = metadata.brokers.find((b) => b.nodeId === partition.leader)!;
    leader = new Broker({
      connectionPool: createConnectionPool(advertisedAddress(brokerData.host, brokerData.port)),
      logger: newLogger(),
    });
    await leader.connect();
    return leader;
  }

  it('produces and fetches records', async () => {
    const target = await connectToLeader();
    const messages = [
      { key: `key-${secureRandom()}`, value: `value-${secureRandom()}`, timestamp },
      { key: `key-${secureRandom()}`, value: `value-${secureRandom()}`, timestamp },
    ];

    const produced = await target.produce({
      acks: 1,
      timeout: 30_000,
      topicData: [{ topic: topicName, partitions: [{ partition: 0, messages }] }],
    });
    expect(produced?.topics[0]?.partitions[0]?.errorCode).toBe(0);
    expect(produced?.topics[0]?.partitions[0]?.baseOffset).toBe(0n);

    const fetched = await target.fetch({
      replicaId: -1,
      maxWaitTime: 1000,
      minBytes: 1,
      maxBytes: 10_485_760,
      topics: [{ topic: topicName, partitions: [{ partition: 0, fetchOffset: 0n, maxBytes: 1_048_576 }] }],
    });
    const records = fetched.responses[0]?.partitions[0]?.messages ?? [];
    expect(records).toHaveLength(2);
    expect(records[0]?.offset).toBe(0n);
    expect(records[0]?.key?.toString()).toBe(messages[0]!.key);
    expect(records[1]?.offset).toBe(1n);
  });

  it('produces gzip-compressed records', async () => {
    const target = await connectToLeader();
    await target.produce({
      acks: 1,
      timeout: 30_000,
      compression: COMPRESSION_TYPES.GZIP,
      topicData: [
        {
          topic: topicName,
          partitions: [{ partition: 0, messages: [{ key: 'k', value: 'v', timestamp }] }],
        },
      ],
    });
    const fetched = await target.fetch({
      replicaId: -1,
      maxWaitTime: 1000,
      minBytes: 1,
      maxBytes: 10_485_760,
      topics: [{ topic: topicName, partitions: [{ partition: 0, fetchOffset: 0n, maxBytes: 1_048_576 }] }],
    });
    expect(fetched.responses[0]?.partitions[0]?.messages[0]?.value?.toString()).toBe('v');
  });
});
