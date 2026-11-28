import { afterEach, beforeEach, describe, expect } from 'vitest';
import { createConsumer } from '../../../src/consumer/index';
import { API_KEYS } from '../../../src/protocol/requests/api-keys';
import { END_TXN_TRANSACTION_V2_MIN_VERSION } from '../../../src/protocol/requests/end-txn/index';
import { PRODUCE_TRANSACTION_V2_MIN_VERSION } from '../../../src/protocol/requests/produce/shared';
import {
  TXN_OFFSET_COMMIT_TOPIC_ID_MIN_VERSION,
  TXN_OFFSET_COMMIT_TRANSACTION_V2_MIN_VERSION,
} from '../../../src/protocol/requests/txn-offset-commit/index';
import { createProducer } from '../../../src/producer/index';
import {
  createCluster,
  createTopic,
  newLogger,
  recordRequests,
  secureRandom,
  waitForCommittedOffset,
  waitForConsumerToJoinGroup,
  waitForMessages,
  testIfKafkaAtLeast_0_11,
  testIfKafkaTransactionV1,
  testIfKafkaTransactionV2,
} from '../../helpers/index';

describe('producer.transactions', () => {
  let topicName: string;
  let transactionalId: string;
  let producer: ReturnType<typeof createProducer> | undefined;
  let consumer: ReturnType<typeof createConsumer> | undefined;

  beforeEach(async () => {
    topicName = `test-topic-${secureRandom()}`;
    transactionalId = `transactional-id-${secureRandom()}`;
    await createTopic({ topic: topicName });
  });

  afterEach(async () => {
    await consumer?.disconnect();
    await producer?.disconnect();
  });

  testIfKafkaAtLeast_0_11('commits a transaction so the consumer sees the messages', async () => {
    producer = createProducer({
      cluster: createCluster(),
      logger: newLogger(),
      idempotent: true,
      transactionalId,
    });
    consumer = createConsumer({
      cluster: createCluster(),
      groupId: `group-${secureRandom()}`,
      maxWaitTimeInMs: 100,
      logger: newLogger(),
    });
    await producer.connect();
    await consumer.connect();
    await consumer.subscribe({ topic: topicName, fromBeginning: true });
    const consumed: { message: { value: Buffer | null } }[] = [];
    const join = waitForConsumerToJoinGroup(consumer);
    await consumer.run({
      eachMessage: async (event) => {
        consumed.push(event);
      },
    });
    await join;

    const transaction = await producer.transaction();
    await transaction.send({ topic: topicName, messages: [{ key: 'k', value: 'committed' }] });
    await transaction.commit();
    await waitForMessages(consumed, { number: 1 });
    expect(consumed[0]?.message.value?.toString()).toBe('committed');
  });

  testIfKafkaAtLeast_0_11('aborts a transaction so the consumer sees nothing', async () => {
    producer = createProducer({
      cluster: createCluster(),
      logger: newLogger(),
      idempotent: true,
      transactionalId,
    });
    consumer = createConsumer({
      cluster: createCluster(),
      groupId: `group-${secureRandom()}`,
      maxWaitTimeInMs: 100,
      logger: newLogger(),
    });
    await producer.connect();
    await consumer.connect();
    await consumer.subscribe({ topic: topicName, fromBeginning: true });
    const consumed: unknown[] = [];
    const join = waitForConsumerToJoinGroup(consumer);
    await consumer.run({
      eachMessage: async (event) => {
        consumed.push(event);
      },
    });
    await join;

    const transaction = await producer.transaction();
    await transaction.send({ topic: topicName, messages: [{ key: 'k', value: 'aborted' }] });
    await transaction.abort();
    await new Promise((resolve) => setTimeout(resolve, 500));
    expect(consumed).toHaveLength(0);
  });

  testIfKafkaAtLeast_0_11('cancels an in-flight transaction when a second producer uses the same id', async () => {
    const first = createProducer({
      cluster: createCluster(),
      logger: newLogger(),
      idempotent: true,
      transactionalId,
      transactionTimeout: 2000,
    });
    const second = createProducer({
      cluster: createCluster(),
      logger: newLogger(),
      idempotent: true,
      transactionalId,
    });
    producer = second;
    await first.connect();
    const tx1 = await first.transaction();
    await tx1.send({ topic: topicName, messages: [{ key: 'k', value: 'v' }] });
    await second.connect();
    const tx2 = await second.transaction();
    expect(tx2.isActive()).toBe(true);
    await tx2.send({ topic: topicName, messages: [{ key: 'k2', value: 'v2' }] });
    await expect(tx2.commit()).resolves.toBeUndefined();
    expect(tx2.isActive()).toBe(false);
    await first.disconnect();
  });

  testIfKafkaAtLeast_0_11('adds offsets first in a transaction begun right after the previous one ended', async () => {
    producer = createProducer({ cluster: createCluster(), logger: newLogger(), idempotent: true, transactionalId });
    await producer.connect();
    const consumerGroupId = `group-${secureRandom()}`;

    // The coordinator answers CONCURRENT_TRANSACTIONS until it has finished ending the previous
    // transaction; AddOffsetsToTxn has to wait that out.
    for (let offset = 1n; offset <= 4n; offset++) {
      const transaction = await producer.transaction();
      await transaction.sendOffsets({
        consumerGroupId,
        topics: [{ topic: topicName, partitions: [{ partition: 0, offset }] }],
      });
      await (offset % 2n === 0n ? transaction.commit() : transaction.abort());
    }
  });

  testIfKafkaTransactionV2('does not send AddPartitionsToTxn under transaction V2', async () => {
    const recorder = recordRequests();
    producer = createProducer({
      cluster: createCluster({ instrumentationEmitter: recorder.instrumentationEmitter }),
      logger: newLogger(),
      idempotent: true,
      transactionalId,
    });
    consumer = createConsumer({
      cluster: createCluster(),
      groupId: `group-${secureRandom()}`,
      maxWaitTimeInMs: 100,
      logger: newLogger(),
    });
    await producer.connect();
    await consumer.connect();
    await consumer.subscribe({ topic: topicName, fromBeginning: true });
    const consumed: unknown[] = [];
    const join = waitForConsumerToJoinGroup(consumer);
    await consumer.run({
      eachMessage: async (event) => {
        consumed.push(event);
      },
    });
    await join;

    const transaction = await producer.transaction();
    await transaction.send({ topic: topicName, messages: [{ key: 'k', value: 'v2-committed' }] });
    await transaction.commit();
    await waitForMessages(consumed, { number: 1 });

    expect(recorder.versionsOf('AddPartitionsToTxn')).toEqual([]);
    expect(Math.min(...recorder.versionsOf('Produce'))).toBeGreaterThanOrEqual(PRODUCE_TRANSACTION_V2_MIN_VERSION);
  });

  testIfKafkaTransactionV2(
    'bumps the epoch through EndTxn v5 and adds offsets with TxnOffsetCommit v5+ under transaction V2',
    async () => {
      const recorder = recordRequests();
      const cluster = createCluster({ instrumentationEmitter: recorder.instrumentationEmitter });
      producer = createProducer({
        cluster,
        logger: newLogger(),
        idempotent: true,
        transactionalId,
      });
      await producer.connect();
      // The idempotent producer allocates its own producer id on connect.
      const initProducerIdsAtConnect = recorder.versionsOf('InitProducerId').length;
      const consumerGroupId = `group-${secureRandom()}`;

      for (const offset of [1n, 2n]) {
        const transaction = await producer.transaction();
        await transaction.send({ topic: topicName, messages: [{ key: 'k', value: `v${offset}` }] });
        await transaction.sendOffsets({
          consumerGroupId,
          topics: [{ topic: topicName, partitions: [{ partition: 0, offset }] }],
        });
        await transaction.commit();
      }
      await waitForCommittedOffset({ groupId: consumerGroupId, topic: topicName, offset: 2n });

      // An aborted transaction leaves the committed offset where it was.
      const aborted = await producer.transaction();
      await aborted.sendOffsets({
        consumerGroupId,
        topics: [{ topic: topicName, partitions: [{ partition: 0, offset: 3n }] }],
      });
      await aborted.abort();
      const next = await producer.transaction();
      await next.send({ topic: topicName, messages: [{ key: 'k', value: 'after-abort' }] });
      await next.commit();
      await waitForCommittedOffset({ groupId: consumerGroupId, topic: topicName, offset: 2n });

      // Only the first transaction needed InitProducerId; every EndTxn after it carried the bumped epoch.
      expect(recorder.versionsOf('InitProducerId').length - initProducerIdsAtConnect).toBe(1);
      expect(recorder.distinctVersionsOf('EndTxn')).toEqual([5]);
      // Kafka 4.4+ also takes the topic by id (v6, KIP-1319).
      const txnOffsetCommitMaxVersion = cluster.brokerPool.versions?.[API_KEYS.TxnOffsetCommit]?.maxVersion ?? 0;
      expect(recorder.distinctVersionsOf('TxnOffsetCommit')).toEqual([
        Math.min(txnOffsetCommitMaxVersion, TXN_OFFSET_COMMIT_TOPIC_ID_MIN_VERSION),
      ]);
      expect(recorder.versionsOf('AddOffsetsToTxn')).toEqual([]);
      expect(recorder.versionsOf('AddPartitionsToTxn')).toEqual([]);
    },
  );

  testIfKafkaTransactionV1('stays below the transaction V2 request versions on brokers older than it', async () => {
    const recorder = recordRequests();
    const cluster = createCluster({ instrumentationEmitter: recorder.instrumentationEmitter });
    producer = createProducer({ cluster, logger: newLogger(), idempotent: true, transactionalId });
    await producer.connect();
    const consumerGroupId = `group-${secureRandom()}`;

    const transaction = await producer.transaction();
    await transaction.send({ topic: topicName, messages: [{ key: 'k', value: 'v1-committed' }] });
    await transaction.sendOffsets({
      consumerGroupId,
      topics: [{ topic: topicName, partitions: [{ partition: 0, offset: 1n }] }],
    });
    await transaction.commit();
    await waitForCommittedOffset({ groupId: consumerGroupId, topic: topicName, offset: 1n });

    expect(recorder.versionsOf('AddPartitionsToTxn').length).toBeGreaterThan(0);
    expect(recorder.versionsOf('AddOffsetsToTxn').length).toBeGreaterThan(0);
    // The highest version the broker advertises, but none that the broker reads as transaction V2.
    const expectedVersion = (apiKey: number, transactionV2MinVersion: number) =>
      Math.min(cluster.brokerPool.versions?.[apiKey]?.maxVersion ?? 0, transactionV2MinVersion - 1);
    expect(recorder.distinctVersionsOf('EndTxn')).toEqual([
      expectedVersion(API_KEYS.EndTxn, END_TXN_TRANSACTION_V2_MIN_VERSION),
    ]);
    expect(recorder.distinctVersionsOf('TxnOffsetCommit')).toEqual([
      expectedVersion(API_KEYS.TxnOffsetCommit, TXN_OFFSET_COMMIT_TRANSACTION_V2_MIN_VERSION),
    ]);
    expect(Math.max(...recorder.versionsOf('Produce'))).toBeLessThan(PRODUCE_TRANSACTION_V2_MIN_VERSION);
  });
});
