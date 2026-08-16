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
import { Produce } from './produce/index';

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
    expect(factory({}).request.apiVersion).toBe(2);
  });

  it('throws KafkaServerDoesNotSupportApiKey when the broker never advertised the api', () => {
    expect(() => lookup({})(API_KEYS.ApiVersions, ApiVersions)).toThrow(KafkaServerDoesNotSupportApiKey);
  });

  it('picks the highest overlapping version when the broker is older than the client max', () => {
    expect(
      negotiatedVersion({ [API_KEYS.Produce]: { minVersion: 0, maxVersion: 5 } }, API_KEYS.Produce, Produce),
    ).toBe(5);
  });

  it('throws KafkaServerDoesNotSupportApiKey when the broker is too old for implemented versions', () => {
    expect(() =>
      lookup({ [API_KEYS.Produce]: { minVersion: 0, maxVersion: 2 } })(API_KEYS.Produce, Produce),
    ).toThrow(KafkaServerDoesNotSupportApiKey);
  });

  it('includes api name, broker range, and implemented range when there is no overlap', () => {
    try {
      lookup({ [API_KEYS.Produce]: { minVersion: 0, maxVersion: 2 } })(API_KEYS.Produce, Produce);
      throw new Error('expected lookup to throw');
    } catch (error) {
      expect(error).toBeInstanceOf(KafkaServerDoesNotSupportApiKey);
      const unsupported = error as KafkaServerDoesNotSupportApiKey;
      expect(unsupported.apiKey).toBe(API_KEYS.Produce);
      expect(unsupported.apiName).toBe('Produce');
      expect(unsupported.brokerMinVersion).toBe(0);
      expect(unsupported.brokerMaxVersion).toBe(2);
      expect(unsupported.implementedVersions).toEqual([3, 4, 5, 6, 7]);
      expect(unsupported.message).toContain('Broker advertised 0-2');
      expect(unsupported.message).toContain('client implements 3-7');
    }
  });

  it('never throws Invariant violated for a Produce maxVersion 2 gap', () => {
    expect(() =>
      lookup({ [API_KEYS.Produce]: { maxVersion: 2 } })(API_KEYS.Produce, Produce),
    ).not.toThrow(/Invariant violated/);
  });

  it('respects broker minVersion so Kafka 4.0 floors still pick the highest client version', () => {
    expect(
      negotiatedVersion({ [API_KEYS.Produce]: { minVersion: 3, maxVersion: 7 } }, API_KEYS.Produce, Produce),
    ).toBe(7);

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
    expect(() =>
      lookup({ [API_KEYS.Produce]: { minVersion: 8, maxVersion: 9 } })(API_KEYS.Produce, Produce),
    ).toThrow(KafkaServerDoesNotSupportApiKey);
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
});
