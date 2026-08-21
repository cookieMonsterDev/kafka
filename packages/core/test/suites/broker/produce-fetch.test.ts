import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { Broker } from '../../../src/broker/index';
import { COMPRESSION_TYPES } from '../../../src/protocol/compression/index';
import { API_KEYS } from '../../../src/protocol/requests/api-keys';
import { lookup } from '../../../src/protocol/requests/index';
import { Fetch } from '../../../src/protocol/requests/fetch/index';
import { Produce } from '../../../src/protocol/requests/produce/index';
import {
  advertisedAddress,
  createConnectionPool,
  createTopic,
  newLogger,
  retryProtocol,
  secureRandom,
  testIfKafkaAtLeast_0_11,
  testIfKafkaAtMost_0_10,
  testIfKafkaAtLeast_1_1,
  testIfKafkaAtLeast_2_4,
  testIfKafkaAtLeast_4_0,
  testIfKafkaAtLeast_4_3,
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

  testIfKafkaAtMost_0_10('on Kafka 0.10 produce/fetch uses MessageSet (Produce v2, Fetch v3)', async () => {
    const target = await connectToLeader();
    const produceVersion = lookup(target.versions!)(API_KEYS.Produce, Produce)({
      acks: 1,
      timeout: 30_000,
      topicData: [],
    }).request.apiVersion;
    const fetchVersion = lookup(target.versions!)(API_KEYS.Fetch, Fetch)({
      replicaId: -1,
      maxWaitTime: 100,
      minBytes: 1,
      maxBytes: 1_048_576,
      topics: [],
    }).request.apiVersion;
    console.log(`0.10 produce/fetch smoke negotiated Produce v${produceVersion}, Fetch v${fetchVersion}`);
    expect(produceVersion).toBe(2);
    expect(fetchVersion).toBe(3);

    const produced = await retryProtocol(TRANSIENT_METADATA_ERRORS, () =>
      target.produce({
        acks: 1,
        timeout: 30_000,
        topicData: [
          {
            topic: topicName,
            partitions: [{ partition: 0, messages: [{ key: 'k', value: 'v', timestamp }] }],
          },
        ],
      }),
    );
    expect(produced?.topics[0]?.partitions[0]?.errorCode).toBe(0);
    expect(produced?.topics[0]?.partitions[0]?.baseOffset).toBe(0n);

    const fetched = await target.fetch({
      replicaId: -1,
      maxWaitTime: 1000,
      minBytes: 1,
      maxBytes: 10_485_760,
      topics: [{ topic: topicName, partitions: [{ partition: 0, fetchOffset: 0n, maxBytes: 1_048_576 }] }],
    });
    const record = fetched.responses[0]?.partitions[0]?.messages[0];
    expect(record?.magicByte).toBe(1);
    expect(record?.value?.toString()).toBe('v');
    expect(record?.headers).toEqual({});
    expect(record?.offset).toBe(0n);
  });

  testIfKafkaAtLeast_0_11('on Kafka 0.11+ produce/fetch uses RecordBatch (Produce >= 3, Fetch >= 4)', async () => {
    const target = await connectToLeader();
    const produceVersion = lookup(target.versions!)(API_KEYS.Produce, Produce)({
      acks: 1,
      timeout: 30_000,
      topicData: [],
    }).request.apiVersion;
    const fetchVersion = lookup(target.versions!)(API_KEYS.Fetch, Fetch)({
      replicaId: -1,
      maxWaitTime: 100,
      minBytes: 1,
      maxBytes: 1_048_576,
      topics: [],
    }).request.apiVersion;
    console.log(`0.11+ produce/fetch smoke negotiated Produce v${produceVersion}, Fetch v${fetchVersion}`);
    expect(produceVersion).toBeGreaterThanOrEqual(3);
    expect(fetchVersion).toBeGreaterThanOrEqual(4);

    const produced = await retryProtocol(TRANSIENT_METADATA_ERRORS, () =>
      target.produce({
        acks: 1,
        timeout: 30_000,
        topicData: [
          {
            topic: topicName,
            partitions: [
              {
                partition: 0,
                messages: [{ key: 'k', value: 'v', timestamp, headers: { h: '1' } }],
              },
            ],
          },
        ],
      }),
    );
    expect(produced?.topics[0]?.partitions[0]?.errorCode).toBe(0);

    const fetched = await target.fetch({
      replicaId: -1,
      maxWaitTime: 1000,
      minBytes: 1,
      maxBytes: 10_485_760,
      topics: [{ topic: topicName, partitions: [{ partition: 0, fetchOffset: 0n, maxBytes: 1_048_576 }] }],
    });
    const record = fetched.responses[0]?.partitions[0]?.messages[0];
    expect(record?.value?.toString()).toBe('v');
    const header = record?.headers?.h;
    const headerValue = Array.isArray(header) ? header[0] : header;
    expect(headerValue?.toString()).toBe('1');
  });

  testIfKafkaAtLeast_1_1('on Kafka 1.1+ Fetch incremental sessions (v7+) accept a session id', async () => {
    const target = await connectToLeader();
    const fetchVersion = lookup(target.versions!)(API_KEYS.Fetch, Fetch)({
      replicaId: -1,
      maxWaitTime: 100,
      minBytes: 1,
      maxBytes: 1_048_576,
      topics: [],
    }).request.apiVersion;
    expect(fetchVersion).toBeGreaterThanOrEqual(7);

    await retryProtocol(TRANSIENT_METADATA_ERRORS, () =>
      target.produce({
        acks: 1,
        timeout: 30_000,
        topicData: [
          { topic: topicName, partitions: [{ partition: 0, messages: [{ key: 'k', value: 'session', timestamp }] }] },
        ],
      }),
    );
    const fetched = await target.fetch({
      replicaId: -1,
      maxWaitTime: 1000,
      minBytes: 1,
      maxBytes: 10_485_760,
      sessionId: 0,
      sessionEpoch: -1,
      topics: [{ topic: topicName, partitions: [{ partition: 0, fetchOffset: 0n, maxBytes: 1_048_576 }] }],
    });
    expect(fetched.sessionId).toEqual(expect.any(Number));
    expect(fetched.responses[0]?.partitions[0]?.messages[0]?.value?.toString()).toBe('session');
  });

  testIfKafkaAtLeast_2_4('on Kafka 2.4+ produce/fetch uses RecordBatch (Produce >= 3, Fetch >= 8)', async () => {
    const target = await connectToLeader();
    const produceVersion = lookup(target.versions!)(API_KEYS.Produce, Produce)({
      acks: 1,
      timeout: 30_000,
      topicData: [],
    }).request.apiVersion;
    const fetchVersion = lookup(target.versions!)(API_KEYS.Fetch, Fetch)({
      replicaId: -1,
      maxWaitTime: 100,
      minBytes: 1,
      maxBytes: 1_048_576,
      topics: [],
    }).request.apiVersion;
    console.log(`produce/fetch smoke negotiated Produce v${produceVersion}, Fetch v${fetchVersion}`);
    expect(produceVersion).toBeGreaterThanOrEqual(3);
    expect(fetchVersion).toBeGreaterThanOrEqual(8);

    const produced = await target.produce({
      acks: 1,
      timeout: 30_000,
      topicData: [
        { topic: topicName, partitions: [{ partition: 0, messages: [{ key: 'k', value: 'v', timestamp }] }] },
      ],
    });
    expect(produced?.topics[0]?.partitions[0]?.errorCode).toBe(0);

    const fetched = await target.fetch({
      replicaId: -1,
      maxWaitTime: 1000,
      minBytes: 1,
      maxBytes: 10_485_760,
      topics: [{ topic: topicName, partitions: [{ partition: 0, fetchOffset: 0n, maxBytes: 1_048_576 }] }],
    });
    expect(fetched.responses[0]?.partitions[0]?.messages[0]?.value?.toString()).toBe('v');
  });

  it('produces and fetches records', async () => {
    const target = await connectToLeader();
    const messages = [
      { key: `key-${secureRandom()}`, value: `value-${secureRandom()}`, timestamp },
      { key: `key-${secureRandom()}`, value: `value-${secureRandom()}`, timestamp },
    ];

    const produced = await retryProtocol(TRANSIENT_METADATA_ERRORS, () =>
      target.produce({
        acks: 1,
        timeout: 30_000,
        topicData: [{ topic: topicName, partitions: [{ partition: 0, messages }] }],
      }),
    );
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
    await retryProtocol(TRANSIENT_METADATA_ERRORS, () =>
      target.produce({
        acks: 1,
        timeout: 30_000,
        compression: COMPRESSION_TYPES.GZIP,
        topicData: [
          {
            topic: topicName,
            partitions: [{ partition: 0, messages: [{ key: 'k', value: 'v', timestamp }] }],
          },
        ],
      }),
    );
    const fetched = await target.fetch({
      replicaId: -1,
      maxWaitTime: 1000,
      minBytes: 1,
      maxBytes: 10_485_760,
      topics: [{ topic: topicName, partitions: [{ partition: 0, fetchOffset: 0n, maxBytes: 1_048_576 }] }],
    });
    expect(fetched.responses[0]?.partitions[0]?.messages[0]?.value?.toString()).toBe('v');
  });

  testIfKafkaAtLeast_4_0('Kafka 4.0+ negotiates Produce v11+', async () => {
    const target = await connectToLeader();
    const produceVersion = lookup(target.versions!)(API_KEYS.Produce, Produce)({
      acks: 1,
      timeout: 30_000,
      topicData: [],
    }).request.apiVersion;
    expect(produceVersion).toBeGreaterThanOrEqual(11);
  });

  testIfKafkaAtLeast_4_0('Kafka 4.0+ Fetch v13 uses topic IDs from metadata', async () => {
    const metadata = await retryProtocol(TRANSIENT_METADATA_ERRORS, () => broker!.metadata([topicName]));
    const topicId = metadata.topicMetadata[0]?.topicId;
    expect(topicId).toBeInstanceOf(Buffer);
    expect(topicId?.length).toBe(16);

    const target = await connectToLeader();
    const fetchVersion = lookup(target.versions!)(API_KEYS.Fetch, Fetch)({
      replicaId: -1,
      maxWaitTime: 100,
      minBytes: 1,
      maxBytes: 1_048_576,
      topics: [{ topic: topicName, topicId, partitions: [] }],
    }).request.apiVersion;
    expect(fetchVersion).toBeGreaterThanOrEqual(13);

    await retryProtocol(TRANSIENT_METADATA_ERRORS, () =>
      target.produce({
        acks: 1,
        timeout: 30_000,
        topicData: [
          {
            topic: topicName,
            partitions: [{ partition: 0, messages: [{ key: 'k', value: 'v13', timestamp }] }],
          },
        ],
      }),
    );

    const fetched = await target.fetch({
      replicaId: -1,
      maxWaitTime: 1000,
      minBytes: 1,
      maxBytes: 10_485_760,
      topics: [{ topic: topicName, topicId, partitions: [{ partition: 0, fetchOffset: 0n, maxBytes: 1_048_576 }] }],
    });
    expect(fetched.responses[0]?.topicName).toBe(topicName);
    expect(fetched.responses[0]?.topicId).toEqual(topicId);
    expect(fetched.responses[0]?.partitions[0]?.messages[0]?.value?.toString()).toBe('v13');
  });

  testIfKafkaAtLeast_4_3('Kafka 4.3+ Produce v13 uses topic IDs from metadata', async () => {
    const metadata = await retryProtocol(TRANSIENT_METADATA_ERRORS, () => broker!.metadata([topicName]));
    const topicId = metadata.topicMetadata[0]?.topicId;
    expect(topicId).toBeInstanceOf(Buffer);
    expect(topicId?.length).toBe(16);

    const target = await connectToLeader();
    const produceVersion = lookup(target.versions!)(API_KEYS.Produce, Produce)({
      acks: 1,
      timeout: 30_000,
      topicData: [{ topic: topicName, topicId, partitions: [] }],
    }).request.apiVersion;
    expect(produceVersion).toBeGreaterThanOrEqual(13);

    const produced = await target.produce({
      acks: 1,
      timeout: 30_000,
      topicData: [
        {
          topic: topicName,
          topicId,
          partitions: [{ partition: 0, messages: [{ key: 'k', value: 'v13', timestamp }] }],
        },
      ],
    });
    expect(produced?.topics[0]?.partitions[0]?.errorCode).toBe(0);
    expect(produced?.topics[0]?.topicName).toBe(topicName);
  });
});
