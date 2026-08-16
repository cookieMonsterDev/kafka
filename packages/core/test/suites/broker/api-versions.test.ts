import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { Broker } from '../../../src/broker/index';
import { API_KEYS } from '../../../src/protocol/requests/api-keys';
import { lookup } from '../../../src/protocol/requests/index';
import { Fetch } from '../../../src/protocol/requests/fetch/index';
import { Produce } from '../../../src/protocol/requests/produce/index';
import {
  connectionOpts,
  createConnectionPool,
  newLogger,
  testIfKafkaAtLeast_0_11,
  testIfKafkaAtLeast_1_0,
  testIfKafkaAtLeast_1_1,
  testIfKafkaAtLeast_2_4,
  testIfKafkaAtLeast_4_0,
  testIfKafkaAtMost_3_6,
  testIfKafkaEquals_0_11,
  testIfKafkaEquals_1_1,
} from '../../helpers/index';

const emptyProduce = { acks: 1, timeout: 30_000, topicData: [] };
const emptyFetch = {
  replicaId: -1,
  maxWaitTime: 100,
  minBytes: 1,
  maxBytes: 1_048_576,
  topics: [],
};

describe('broker.apiVersions', () => {
  let broker: Broker | undefined;

  beforeEach(async () => {
    broker = new Broker({ connectionPool: createConnectionPool(connectionOpts()), logger: newLogger() });
    await broker.connect();
  });

  afterEach(async () => {
    await broker?.disconnect();
  });

  it('returns broker API versions', async () => {
    const versions = await broker!.apiVersions();
    expect(versions[API_KEYS.Metadata]).toEqual(expect.objectContaining({ minVersion: expect.any(Number) }));
    expect(versions[API_KEYS.Produce]).toEqual(expect.objectContaining({ maxVersion: expect.any(Number) }));
  });

  it('negotiates a supported ApiVersions version on connect', async () => {
    expect(broker!.isConnected()).toBe(true);
    const versions = await broker!.apiVersions();
    expect(versions[API_KEYS.ApiVersions]?.maxVersion).toBeGreaterThanOrEqual(0);
  });

  it('selects Produce >= 3 and Fetch >= 4 even when the broker advertises older floors', () => {
    const versions = broker!.versions!;
    const produceVersion = lookup(versions)(API_KEYS.Produce, Produce)(emptyProduce).request.apiVersion;
    const fetchVersion = lookup(versions)(API_KEYS.Fetch, Fetch)(emptyFetch).request.apiVersion;

    expect(produceVersion).toBeGreaterThanOrEqual(3);
    expect(fetchVersion).toBeGreaterThanOrEqual(4);
  });

  testIfKafkaAtLeast_0_11('Kafka 0.11+ advertises InitProducerId (transactions and headers)', () => {
    expect(broker!.versions![API_KEYS.InitProducerId]).toEqual(
      expect.objectContaining({ maxVersion: expect.any(Number) }),
    );
  });

  testIfKafkaEquals_0_11('Kafka 0.11 negotiates Produce v3 and Fetch v5', () => {
    const versions = broker!.versions!;
    const produceVersion = lookup(versions)(API_KEYS.Produce, Produce)(emptyProduce).request.apiVersion;
    const fetchVersion = lookup(versions)(API_KEYS.Fetch, Fetch)(emptyFetch).request.apiVersion;

    console.log(`Kafka 0.11 negotiated Produce v${produceVersion}, Fetch v${fetchVersion}`);
    expect(produceVersion).toBe(3);
    expect(fetchVersion).toBe(5);
    expect(versions[API_KEYS.SaslAuthenticate]).toBeUndefined();
    expect(versions[API_KEYS.CreatePartitions]).toBeUndefined();
    expect(versions[API_KEYS.DeleteGroups]).toBeUndefined();
  });

  testIfKafkaAtLeast_1_0('Kafka 1.0+ advertises SaslAuthenticate and CreatePartitions', () => {
    const versions = broker!.versions!;
    expect(versions[API_KEYS.SaslAuthenticate]).toEqual(expect.objectContaining({ maxVersion: expect.any(Number) }));
    expect(versions[API_KEYS.CreatePartitions]).toEqual(expect.objectContaining({ maxVersion: expect.any(Number) }));
  });

  testIfKafkaEquals_1_1('Kafka 1.1 negotiates Produce v5 and Fetch v7', () => {
    const versions = broker!.versions!;
    const produceVersion = lookup(versions)(API_KEYS.Produce, Produce)(emptyProduce).request.apiVersion;
    const fetchVersion = lookup(versions)(API_KEYS.Fetch, Fetch)(emptyFetch).request.apiVersion;

    console.log(`Kafka 1.1 negotiated Produce v${produceVersion}, Fetch v${fetchVersion}`);
    expect(produceVersion).toBe(5);
    expect(fetchVersion).toBe(7);
  });

  testIfKafkaAtLeast_1_1('Kafka 1.1+ advertises DeleteGroups and Fetch incremental sessions (v7+)', () => {
    const versions = broker!.versions!;
    const fetchVersion = lookup(versions)(API_KEYS.Fetch, Fetch)(emptyFetch).request.apiVersion;

    expect(versions[API_KEYS.DeleteGroups]).toEqual(expect.objectContaining({ maxVersion: expect.any(Number) }));
    expect(fetchVersion).toBeGreaterThanOrEqual(7);
    expect(versions[API_KEYS.DescribeConfigs]?.maxVersion).toBeGreaterThanOrEqual(1);
  });

  testIfKafkaAtLeast_2_4('Kafka 2.4+ negotiates Produce >= 3 and Fetch >= 8', () => {
    const versions = broker!.versions!;
    const produceVersion = lookup(versions)(API_KEYS.Produce, Produce)(emptyProduce).request.apiVersion;
    const fetchVersion = lookup(versions)(API_KEYS.Fetch, Fetch)(emptyFetch).request.apiVersion;

    console.log(`negotiated Produce v${produceVersion}, Fetch v${fetchVersion}`);
    expect(produceVersion).toBeGreaterThanOrEqual(3);
    expect(fetchVersion).toBeGreaterThanOrEqual(8);
  });

  testIfKafkaAtMost_3_6('Kafka 3.x advertises Produce minVersion 0; the client still uses RecordBatch v3+', () => {
    const produce = broker!.versions![API_KEYS.Produce];
    expect(produce?.minVersion).toBe(0);
    expect(produce?.maxVersion).toBeGreaterThanOrEqual(3);

    const negotiated = lookup(broker!.versions!)(API_KEYS.Produce, Produce)(emptyProduce).request.apiVersion;
    expect(negotiated).toBeGreaterThanOrEqual(3);
  });

  testIfKafkaAtLeast_4_0(
    'Kafka 4.0 still advertises Produce minVersion 0 (KAFKA-18659 shim); the client uses >= 3',
    () => {
      const produce = broker!.versions![API_KEYS.Produce];
      expect(produce?.minVersion).toBe(0);
      expect(produce?.maxVersion).toBeGreaterThanOrEqual(3);

      const negotiated = lookup(broker!.versions!)(API_KEYS.Produce, Produce)(emptyProduce).request.apiVersion;
      expect(negotiated).toBeGreaterThanOrEqual(3);
    },
  );
});
