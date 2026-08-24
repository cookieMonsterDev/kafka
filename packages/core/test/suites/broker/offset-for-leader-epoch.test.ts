import { afterEach, beforeEach, describe, expect } from 'vitest';
import { Broker } from '../../../src/broker/index';
import {
  advertisedAddress,
  createConnectionPool,
  createTopic,
  generateMessages,
  newLogger,
  retryProtocol,
  secureRandom,
  testIfKafkaAtLeast_2_1,
  TRANSIENT_METADATA_ERRORS,
} from '../../helpers/index';

/**
 * Exercises the real OffsetForLeaderEpoch RPC end-to-end (the primitive `recoverFromTruncation`
 * in consumer-group.ts relies on for KIP-320), without forcing an actual unclean leader election.
 * Forcing a genuine truncation live would require stopping/restarting a broker container in the
 * shared docker-compose cluster used by every other integration test file in the same run - out
 * of scope here; that scenario stays covered by the mocked unit tests in consumer-group.test.ts.
 */
describe('broker.offsetForLeaderEpoch (KIP-320 primitive)', () => {
  let topicName: string;
  let seedBroker: Broker | undefined;
  let leader: Broker | undefined;

  beforeEach(async () => {
    topicName = `test-topic-${secureRandom()}`;
    seedBroker = new Broker({ connectionPool: createConnectionPool(), logger: newLogger() });
    await seedBroker.connect();
    await createTopic({ topic: topicName });
  });

  afterEach(async () => {
    await leader?.disconnect();
    await seedBroker?.disconnect();
  });

  testIfKafkaAtLeast_2_1('resolves the end offset for the partition’s current leader epoch', async () => {
    const metadata = await retryProtocol(TRANSIENT_METADATA_ERRORS, () => seedBroker!.metadata([topicName]));
    const partition = metadata.topicMetadata[0]!.partitionMetadata[0]!;
    const brokerData = metadata.brokers.find((b) => b.nodeId === partition.leader)!;
    leader = new Broker({
      connectionPool: createConnectionPool(advertisedAddress(brokerData.host, brokerData.port)),
      logger: newLogger(),
    });
    await leader.connect();

    const messages = generateMessages({ number: 5 });
    await retryProtocol(TRANSIENT_METADATA_ERRORS, () =>
      leader!.produce({
        acks: 1,
        timeout: 30_000,
        topicData: [{ topic: topicName, partitions: [{ partition: 0, messages }] }],
      }),
    );

    const leaderEpoch = partition.leaderEpoch ?? 0;
    const { topics } = await leader.offsetForLeaderEpoch({
      topics: [{ topic: topicName, partitions: [{ partition: 0, currentLeaderEpoch: leaderEpoch, leaderEpoch }] }],
    });

    const result = topics[0]?.partitions[0];
    expect(result?.errorCode).toBe(0);
    expect(result?.endOffset).toBe(BigInt(messages.length));
  });
});
