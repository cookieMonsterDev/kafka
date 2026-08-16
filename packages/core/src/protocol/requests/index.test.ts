import { describe, expect, it } from 'vitest';
import { KafkaServerDoesNotSupportApiKey } from '../../errors';
import { API_KEYS } from './api-keys';
import { ApiVersions } from './api-versions/index';
import {
  lookup,
  NOT_IMPLEMENTED_REQUEST_DEFINITIONS,
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
  apiKey: number,
  family: RequestFamily<Options>,
  options: Options = {} as Options,
): number {
  return lookup(brokerVersions)(apiKey, family)(options).request.apiVersion;
}

describe('protocol/requests', () => {
  it('picks min(highest version we implement, highest the broker advertised)', () => {
    const factory = lookup({ [API_KEYS.ApiVersions]: { maxVersion: 1 } })(API_KEYS.ApiVersions, ApiVersions);
    expect(factory({}).request.apiVersion).toBe(1);
  });

  it('caps at the highest version we implement even if the broker supports more', () => {
    const factory = lookup({ [API_KEYS.ApiVersions]: { maxVersion: 99 } })(API_KEYS.ApiVersions, ApiVersions);
    expect(factory({}).request.apiVersion).toBe(4);
  });

  it('throws KafkaServerDoesNotSupportApiKey when the broker never advertised the api', () => {
    expect(() => lookup({})(API_KEYS.ApiVersions, ApiVersions)).toThrow(KafkaServerDoesNotSupportApiKey);
  });

  it('picks the highest overlapping version when the broker is older than the client max', () => {
    expect(negotiatedVersion({ [API_KEYS.Produce]: { minVersion: 0, maxVersion: 5 } }, API_KEYS.Produce, Produce)).toBe(
      5,
    );
  });

  it('selects Produce v2 and Fetch v3 on a Kafka 0.10 ApiVersions map', () => {
    const kafka010: BrokerVersions = {
      [API_KEYS.Produce]: { minVersion: 0, maxVersion: 2 },
      [API_KEYS.Fetch]: { minVersion: 0, maxVersion: 3 },
    };

    expect(negotiatedVersion(kafka010, API_KEYS.Produce, Produce)).toBe(2);
    expect(
      negotiatedVersion(kafka010, API_KEYS.Fetch, Fetch, {
        replicaId: -1,
        maxWaitTime: 100,
        minBytes: 1,
        maxBytes: 1024,
        topics: [],
      }),
    ).toBe(3);
  });

  it('throws KafkaServerDoesNotSupportApiKey when the broker is too old for implemented versions', () => {
    expect(() =>
      lookup({ [API_KEYS.Produce]: { minVersion: 0, maxVersion: 2 } })(API_KEYS.Produce, fakeFamily([8, 9])),
    ).toThrow(KafkaServerDoesNotSupportApiKey);
  });

  it('includes api name, broker range, and implemented range when there is no overlap', () => {
    const recordBatchOnly = fakeFamily([3, 4, 5, 6, 7]);
    const unsupported = expectUnsupportedApiKey(() =>
      lookup({ [API_KEYS.Produce]: { minVersion: 0, maxVersion: 2 } })(API_KEYS.Produce, recordBatchOnly),
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
    expect(() => lookup({ [API_KEYS.Produce]: { maxVersion: 2 } })(API_KEYS.Produce, Produce)).not.toThrow(
      /Invariant violated/,
    );
    expect(negotiatedVersion({ [API_KEYS.Produce]: { maxVersion: 2 } }, API_KEYS.Produce, Produce)).toBe(2);
  });

  it('respects broker minVersion so Kafka 4.0 floors still pick the highest client version', () => {
    expect(negotiatedVersion({ [API_KEYS.Produce]: { minVersion: 3, maxVersion: 7 } }, API_KEYS.Produce, Produce)).toBe(
      7,
    );

    const clientWithLegacyProduce = fakeFamily([0, 1, 2, 3, 4, 5, 6, 7]);
    expect(
      negotiatedVersion(
        { [API_KEYS.Produce]: { minVersion: 3, maxVersion: 7 } },
        API_KEYS.Produce,
        clientWithLegacyProduce,
      ),
    ).toBe(7);
  });

  it('throws when the broker minVersion is above every implemented version', () => {
    expect(() => lookup({ [API_KEYS.Produce]: { minVersion: 11, maxVersion: 12 } })(API_KEYS.Produce, Produce)).toThrow(
      KafkaServerDoesNotSupportApiKey,
    );
  });

  it('skips gaps in family.versions instead of calling protocol() with a missing version', () => {
    const gapped = fakeFamily([0, 1, 4]);
    expect(negotiatedVersion({ [API_KEYS.Produce]: { minVersion: 0, maxVersion: 3 } }, API_KEYS.Produce, gapped)).toBe(
      1,
    );
  });

  it('the not-implemented marker always throws KafkaNotImplemented', () => {
    expect(() => NOT_IMPLEMENTED_REQUEST_DEFINITIONS.protocol({ version: 0 })).toThrow('This API is not implemented');
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

    expect(negotiatedVersion(kafka011, API_KEYS.Produce, Produce)).toBe(3);
    expect(
      negotiatedVersion(kafka011, API_KEYS.Fetch, Fetch, {
        replicaId: -1,
        maxWaitTime: 100,
        minBytes: 1,
        maxBytes: 1024,
        topics: [],
      }),
    ).toBe(5);
    expect(negotiatedVersion(kafka011, API_KEYS.CreateTopics, CreateTopics, { topics: [{ topic: 't' }] })).toBe(1);
    expect(negotiatedVersion(kafka011, API_KEYS.DeleteTopics, DeleteTopics, { topics: ['t'] })).toBe(0);
    expect(
      negotiatedVersion(kafka011, API_KEYS.DescribeConfigs, DescribeConfigs, {
        resources: [{ type: 2, name: 't' }],
      }),
    ).toBe(0);
    expect(
      negotiatedVersion(kafka011, API_KEYS.DescribeAcls, DescribeAcls, {
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
      negotiatedVersion(kafka011, API_KEYS.CreateAcls, CreateAcls, {
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
      negotiatedVersion(kafka011, API_KEYS.DeleteAcls, DeleteAcls, {
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
    expect(negotiatedVersion(kafka011, API_KEYS.ListOffsets, ListOffsets, { topics: [] })).toBe(1);
    expect(
      negotiatedVersion(kafka011, API_KEYS.OffsetCommit, OffsetCommit, {
        groupId: 'g',
        groupGenerationId: 1,
        memberId: 'm',
        topics: [],
      }),
    ).toBe(2);
  });
});
