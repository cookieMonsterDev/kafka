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
  testIfKafkaAtLeast_4_0,
  testIfKafkaAtMost_3_6,
} from '../../helpers/index';

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
    const produceVersion = lookup(versions)(API_KEYS.Produce, Produce)({ topicData: [] }).request.apiVersion;
    const fetchVersion = lookup(versions)(API_KEYS.Fetch, Fetch)({
      replicaId: -1,
      maxWaitTime: 100,
      minBytes: 1,
      maxBytes: 1_048_576,
      topics: [],
    }).request.apiVersion;

    expect(produceVersion).toBeGreaterThanOrEqual(3);
    expect(fetchVersion).toBeGreaterThanOrEqual(4);
  });

  testIfKafkaAtMost_3_6('Kafka 3.x advertises Produce minVersion 0; the client still uses RecordBatch v3+', () => {
    const produce = broker!.versions![API_KEYS.Produce];
    expect(produce?.minVersion).toBe(0);
    expect(produce?.maxVersion).toBeGreaterThanOrEqual(3);

    const negotiated = lookup(broker!.versions!)(API_KEYS.Produce, Produce)({ topicData: [] }).request.apiVersion;
    expect(negotiated).toBeGreaterThanOrEqual(3);
  });

  testIfKafkaAtLeast_4_0(
    'Kafka 4.0 still advertises Produce minVersion 0 (KAFKA-18659 shim); the client uses >= 3',
    () => {
      const produce = broker!.versions![API_KEYS.Produce];
      expect(produce?.minVersion).toBe(0);
      expect(produce?.maxVersion).toBeGreaterThanOrEqual(3);

      const negotiated = lookup(broker!.versions!)(API_KEYS.Produce, Produce)({ topicData: [] }).request.apiVersion;
      expect(negotiated).toBeGreaterThanOrEqual(3);
    },
  );
});
