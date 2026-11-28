import { describe, expect, it, vi } from 'vitest';
import { KafkaInvariantViolation, KafkaMemberIdRequired, KafkaServerDoesNotSupportApiKey } from '../../errors';
import { API_KEYS } from './api-keys';
import { ApiVersions } from './api-versions/index';
import {
  defineRequestFamily,
  highestVersionBelow,
  lazyRequestFamily,
  lookup,
  type BrokerVersions,
  type ProtocolResult,
  type RequestFamily,
} from './index';
import { CreateAcls } from './create-acls/index';
import { CreateTopics } from './create-topics/index';
import { DeleteAcls } from './delete-acls/index';
import { DeleteTopics } from './delete-topics/index';
import { DescribeAcls } from './describe-acls/index';
import { DescribeConfigs } from './describe-configs/index';
import { Fetch } from './fetch/index';
import { InitProducerId } from './init-producer-id/index';
import { JoinGroup } from './join-group/index';
import { KAFKA_APIS } from './kafka-apis';
import { ListOffsets } from './list-offsets/index';
import { OffsetCommit } from './offset-commit/index';
import { Produce } from './produce/index';

function expectUnsupportedApiKey(fn: () => void): KafkaServerDoesNotSupportApiKey {
  try {
    fn();
  } catch (error) {
    if (error instanceof KafkaServerDoesNotSupportApiKey) return error;
    throw error;
  }
  throw new Error('expected lookup to throw');
}

function fakeFamily(versions: readonly number[]): RequestFamily<Record<string, never>> {
  return {
    ...KAFKA_APIS.Produce,
    versions,
    protocol({ version }) {
      return () =>
        ({
          request: {
            apiKey: API_KEYS.Produce,
            apiVersion: version,
            apiName: 'Produce',
            encode: async () => {
              throw new Error('unused');
            },
          },
          response: {
            decode: async () => ({}),
            parse: async (data) => data,
          },
        }) satisfies ProtocolResult;
    },
  };
}

function negotiatedVersion<Options>(
  brokerVersions: BrokerVersions,
  family: RequestFamily<Options>,
  options: Options = {} as Options,
): number {
  return lookup(brokerVersions)(family)(options).request.apiVersion;
}

describe('protocol/requests', () => {
  it('picks min(highest version we implement, highest the broker advertised)', () => {
    const factory = lookup({ [API_KEYS.ApiVersions]: { maxVersion: 1 } })(ApiVersions);
    expect(factory({}).request.apiVersion).toBe(1);
  });

  it('negotiates each family once per lookup and reuses its factory', () => {
    const family = fakeFamily([3, 4, 5]);
    const protocol = vi.spyOn(family, 'protocol');
    const lookupRequest = lookup({ [API_KEYS.Produce]: { minVersion: 0, maxVersion: 4 } });

    const first = lookupRequest(family);
    const second = lookupRequest(family);

    expect(second).toBe(first);
    expect(protocol).toHaveBeenCalledExactlyOnceWith({ version: 4 });
  });

  it('caps at the highest version we implement even if the broker supports more', () => {
    const factory = lookup({ [API_KEYS.ApiVersions]: { maxVersion: 99 } })(ApiVersions);
    expect(factory({}).request.apiVersion).toBe(5);
  });

  it('throws KafkaServerDoesNotSupportApiKey when the broker never advertised the api', () => {
    expect(() => lookup({})(ApiVersions)).toThrow(KafkaServerDoesNotSupportApiKey);
  });

  it('picks the highest overlapping version when the broker is older than the client max', () => {
    expect(negotiatedVersion({ [API_KEYS.Produce]: { minVersion: 0, maxVersion: 5 } }, Produce)).toBe(5);
  });

  it('selects Produce v2 and Fetch v3 on a Kafka 0.10 ApiVersions map', () => {
    const kafka010: BrokerVersions = {
      [API_KEYS.Produce]: { minVersion: 0, maxVersion: 2 },
      [API_KEYS.Fetch]: { minVersion: 0, maxVersion: 3 },
    };

    expect(negotiatedVersion(kafka010, Produce)).toBe(2);
    expect(
      negotiatedVersion(kafka010, Fetch, {
        replicaId: -1,
        maxWaitTime: 100,
        minBytes: 1,
        maxBytes: 1024,
        topics: [],
      }),
    ).toBe(3);
  });

  it('throws KafkaServerDoesNotSupportApiKey when the broker is too old for implemented versions', () => {
    expect(() => lookup({ [API_KEYS.Produce]: { minVersion: 0, maxVersion: 2 } })(fakeFamily([8, 9]))).toThrow(
      KafkaServerDoesNotSupportApiKey,
    );
  });

  it('includes api name, broker range, and implemented range when there is no overlap', () => {
    const recordBatchOnly = fakeFamily([3, 4, 5, 6, 7]);
    const unsupported = expectUnsupportedApiKey(() =>
      lookup({ [API_KEYS.Produce]: { minVersion: 0, maxVersion: 2 } })(recordBatchOnly),
    );
    expect(unsupported.apiKey).toBe(API_KEYS.Produce);
    expect(unsupported.apiName).toBe('Produce');
    expect(unsupported.brokerMinVersion).toBe(0);
    expect(unsupported.brokerMaxVersion).toBe(2);
    expect(unsupported.implementedVersions).toEqual([3, 4, 5, 6, 7]);
    expect(unsupported.message).toContain('Broker advertised 0-2');
    expect(unsupported.message).toContain('client implements 3-7');
  });

  it('never throws Invariant violated for a Produce maxVersion 2 broker', () => {
    expect(() => lookup({ [API_KEYS.Produce]: { maxVersion: 2 } })(Produce)).not.toThrow(/Invariant violated/);
    expect(negotiatedVersion({ [API_KEYS.Produce]: { maxVersion: 2 } }, Produce)).toBe(2);
  });

  it('respects broker minVersion so Kafka 4.0 floors still pick the highest client version', () => {
    expect(negotiatedVersion({ [API_KEYS.Produce]: { minVersion: 3, maxVersion: 7 } }, Produce)).toBe(7);

    const clientWithLegacyProduce = fakeFamily([0, 1, 2, 3, 4, 5, 6, 7]);
    expect(negotiatedVersion({ [API_KEYS.Produce]: { minVersion: 3, maxVersion: 7 } }, clientWithLegacyProduce)).toBe(
      7,
    );
  });

  it('throws when the broker minVersion is above every implemented version', () => {
    expect(() => lookup({ [API_KEYS.Produce]: { minVersion: 14, maxVersion: 15 } })(Produce)).toThrow(
      KafkaServerDoesNotSupportApiKey,
    );
  });

  it('skips gaps in family.versions instead of calling protocol() with a missing version', () => {
    const gapped = fakeFamily([0, 1, 4]);
    expect(negotiatedVersion({ [API_KEYS.Produce]: { minVersion: 0, maxVersion: 3 } }, gapped)).toBe(1);
  });

  it('skips InitProducerId v2 so a Kafka 2.4 broker (max 2) gets v1', () => {
    expect(
      negotiatedVersion({ [API_KEYS.InitProducerId]: { minVersion: 0, maxVersion: 2 } }, InitProducerId, {
        transactionalId: null,
        transactionTimeout: 30_000,
      }),
    ).toBe(1);
  });

  it('selects InitProducerId v3+ on Kafka 2.5+ brokers that advertise KIP-360', () => {
    expect(
      negotiatedVersion({ [API_KEYS.InitProducerId]: { minVersion: 0, maxVersion: 3 } }, InitProducerId, {
        transactionalId: null,
        transactionTimeout: 30_000,
      }),
    ).toBe(3);
    expect(
      negotiatedVersion({ [API_KEYS.InitProducerId]: { minVersion: 0, maxVersion: 4 } }, InitProducerId, {
        transactionalId: null,
        transactionTimeout: 30_000,
      }),
    ).toBe(4);
    expect(
      negotiatedVersion({ [API_KEYS.InitProducerId]: { minVersion: 0, maxVersion: 6 } }, InitProducerId, {
        transactionalId: null,
        transactionTimeout: 30_000,
      }),
    ).toBe(5);
  });

  it('selects Kafka 0.11 admin and data-path versions from a recorded ApiVersions map', () => {
    const kafka011: BrokerVersions = {
      [API_KEYS.Produce]: { minVersion: 0, maxVersion: 3 },
      [API_KEYS.Fetch]: { minVersion: 0, maxVersion: 5 },
      [API_KEYS.CreateTopics]: { minVersion: 0, maxVersion: 1 },
      [API_KEYS.DeleteTopics]: { minVersion: 0, maxVersion: 0 },
      [API_KEYS.DescribeConfigs]: { minVersion: 0, maxVersion: 0 },
      [API_KEYS.DescribeAcls]: { minVersion: 0, maxVersion: 0 },
      [API_KEYS.CreateAcls]: { minVersion: 0, maxVersion: 0 },
      [API_KEYS.DeleteAcls]: { minVersion: 0, maxVersion: 0 },
      [API_KEYS.ListOffsets]: { minVersion: 0, maxVersion: 1 },
      [API_KEYS.OffsetCommit]: { minVersion: 0, maxVersion: 2 },
    };

    expect(negotiatedVersion(kafka011, Produce)).toBe(3);
    expect(
      negotiatedVersion(kafka011, Fetch, {
        replicaId: -1,
        maxWaitTime: 100,
        minBytes: 1,
        maxBytes: 1024,
        topics: [],
      }),
    ).toBe(5);
    expect(negotiatedVersion(kafka011, CreateTopics, { topics: [{ topic: 't' }] })).toBe(1);
    expect(negotiatedVersion(kafka011, DeleteTopics, { topics: ['t'] })).toBe(0);
    expect(
      negotiatedVersion(kafka011, DescribeConfigs, {
        resources: [{ type: 2, name: 't' }],
      }),
    ).toBe(0);
    expect(
      negotiatedVersion(kafka011, DescribeAcls, {
        resourceType: 2,
        resourceName: 't',
        resourcePatternType: 3,
        principal: null,
        host: '*',
        operation: 2,
        permissionType: 3,
      }),
    ).toBe(0);
    expect(
      negotiatedVersion(kafka011, CreateAcls, {
        creations: [
          {
            resourceType: 2,
            resourceName: 't',
            resourcePatternType: 3,
            principal: 'User:a',
            host: '*',
            operation: 2,
            permissionType: 3,
          },
        ],
      }),
    ).toBe(0);
    expect(
      negotiatedVersion(kafka011, DeleteAcls, {
        filters: [
          {
            resourceType: 2,
            resourceName: 't',
            resourcePatternType: 3,
            principal: null,
            host: '*',
            operation: 2,
            permissionType: 3,
          },
        ],
      }),
    ).toBe(0);
    expect(negotiatedVersion(kafka011, ListOffsets, { topics: [] })).toBe(1);
    expect(
      negotiatedVersion(kafka011, OffsetCommit, {
        groupId: 'g',
        groupGenerationId: 1,
        memberId: 'm',
        topics: [],
      }),
    ).toBe(2);
  });

  it('selects Fetch v18 on a Kafka 4.3 ApiVersions map', () => {
    expect(
      negotiatedVersion({ [API_KEYS.Fetch]: { minVersion: 4, maxVersion: 18 } }, Fetch, {
        replicaId: -1,
        maxWaitTime: 100,
        minBytes: 1,
        maxBytes: 1024,
        topics: [],
      }),
    ).toBe(18);
  });
});

describe('defineRequestFamily', () => {
  const versions = { 1: fakeFamily([1]).protocol({ version: 1 }), 3: fakeFamily([3]).protocol({ version: 3 }) };

  it('carries the API facts and the implemented versions', () => {
    const family = defineRequestFamily(KAFKA_APIS.Produce, versions);

    expect(family).toMatchObject({ apiKey: API_KEYS.Produce, apiName: 'Produce', firstFlexibleVersion: 9 });
    expect(family.versions).toEqual([1, 3]);
    expect(family.protocol({ version: 3 })).toBe(versions[3]);
  });

  it('throws for a version it does not implement', () => {
    const family = defineRequestFamily(KAFKA_APIS.Produce, versions);

    expect(() => family.protocol({ version: 2 })).toThrow('Invariant violated: no Produce protocol for version 2');
  });

  it('gives every family the same properties, hooks or not', () => {
    const plain = defineRequestFamily(KAFKA_APIS.Produce, versions);
    const hooked = defineRequestFamily(KAFKA_APIS.Produce, versions, { preferVersion: (_options, version) => version });

    expect(Object.keys(hooked)).toEqual(Object.keys(plain));
  });
});

describe('lazyRequestFamily', () => {
  it('carries the API facts before loading and loads the family once', async () => {
    const family = fakeFamily([0]);
    const load = vi.fn(() => Promise.resolve(family));
    const stub = lazyRequestFamily(KAFKA_APIS.CreateTopics, load);

    expect(stub).toMatchObject({ apiKey: API_KEYS.CreateTopics, apiName: 'CreateTopics', firstFlexibleVersion: 5 });
    expect(load).not.toHaveBeenCalled();

    await expect(stub.load()).resolves.toBe(family);
    await expect(stub.load()).resolves.toBe(family);
    expect(load).toHaveBeenCalledOnce();
  });
});

describe('highestVersionBelow', () => {
  it('picks the highest version below the limit within the advertised range', () => {
    expect(highestVersionBelow(KAFKA_APIS.Produce, [3, 11, 12, 13], 13, { minVersion: 3, maxVersion: 13 })).toBe(12);
  });

  it('throws when no version below the limit is advertised', () => {
    expect(() => highestVersionBelow(KAFKA_APIS.Produce, [12, 13], 13, { minVersion: 13, maxVersion: 13 })).toThrow(
      KafkaInvariantViolation,
    );
  });
});

describe('request family hooks', () => {
  const topicId = Buffer.alloc(16, 1);

  it('Produce prefers the highest name-based version when a topic has no id', () => {
    const advertised = { minVersion: 0, maxVersion: 13 };
    const nameOnly = { acks: 1, timeout: 1000, topicData: [{ topic: 't', partitions: [] }] };
    const withIds = { acks: 1, timeout: 1000, topicData: [{ topic: 't', topicId, partitions: [] }] };

    expect(Produce.preferVersion?.(nameOnly, 13, advertised)).toBe(12);
    expect(Produce.preferVersion?.(withIds, 13, advertised)).toBe(13);
    expect(Produce.preferVersion?.(nameOnly, 9, advertised)).toBe(9);
  });

  it('Produce stays below v12 in a transaction outside transaction version 2', () => {
    const advertised = { minVersion: 0, maxVersion: 13 };
    const transactional = {
      acks: -1,
      timeout: 1000,
      transactionalId: 'txn',
      topicData: [{ topic: 't', topicId, partitions: [] }],
    };

    expect(Produce.preferVersion?.(transactional, 13, advertised)).toBe(11);
    expect(Produce.preferVersion?.({ ...transactional, isTransactionV2Enabled: false }, 13, advertised)).toBe(11);
    expect(Produce.preferVersion?.({ ...transactional, isTransactionV2Enabled: true }, 13, advertised)).toBe(13);
    expect(Produce.preferVersion?.({ ...transactional, transactionalId: null }, 13, advertised)).toBe(13);
    expect(Produce.preferVersion?.(transactional, 9, advertised)).toBe(9);
  });

  it('Fetch merges the shuffled partitions of one topic into one entry', () => {
    const partition = (id: number) => ({ partition: id, fetchOffset: 0n, maxBytes: 1024 });
    const prepared = Fetch.prepareOptions?.({
      replicaId: -1,
      maxWaitTime: 100,
      minBytes: 1,
      maxBytes: 1024,
      topics: [{ topic: 't', topicId, partitions: [partition(0), partition(1), partition(2)] }],
    });

    expect(prepared?.topics).toHaveLength(1);
    expect(prepared?.topics[0]?.topicId).toBe(topicId);
    expect(prepared?.topics[0]?.partitions.map(({ partition: id }) => id).sort()).toEqual([0, 1, 2]);
  });

  it('JoinGroup retries once with the member id the broker assigned', () => {
    const options = {
      groupId: 'g',
      sessionTimeout: 10_000,
      memberId: '',
      protocolType: 'consumer',
      groupProtocols: [],
    };
    const memberIdRequired = new KafkaMemberIdRequired({ message: 'need a member id' }, { memberId: 'm-1' });

    expect(JoinGroup.retryOptions?.(memberIdRequired, options)).toEqual({ ...options, memberId: 'm-1' });
    expect(JoinGroup.retryOptions?.(new Error('other'), options)).toBeUndefined();
  });
});
