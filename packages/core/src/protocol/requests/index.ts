import { KafkaNotImplemented, KafkaServerDoesNotSupportApiKey } from '../../errors';
import type { Encoder } from '../encoder';
import { apiKeyName } from './api-keys';

export interface AnyRequestDefinition {
  apiKey: number;
  apiVersion: number;
  apiName: string;
  encode(): Promise<Encoder>;
  expectResponse?(): boolean;
}

export interface AnyResponseDefinition {
  decode(rawData: Buffer): Promise<unknown>;
  parse(data: unknown): Promise<unknown>;
}

export interface ProtocolResult {
  request: AnyRequestDefinition;
  response: AnyResponseDefinition;
  logResponseError?: boolean;
}

/**
 * Per-version factory returned by a family's `protocol({ version })`.
 * Each family takes one options object, e.g. `metadata({ topics, allowAutoTopicCreation })`.
 */
export type ProtocolFactory<Options> = (values: Options) => ProtocolResult;

export interface RequestFamily<Options = unknown> {
  readonly versions: readonly number[];
  protocol(options: { version: number }): ProtocolFactory<Options>;
}

export const NOT_IMPLEMENTED_REQUEST_DEFINITIONS: RequestFamily<never> = Object.freeze({
  versions: Object.freeze([]),
  protocol(): never {
    throw new KafkaNotImplemented('This API is not implemented');
  },
});

/**
 * The per-apiKey version range a broker advertised via `ApiVersions`, keyed by apiKey. Shared with
 * `network/connection.ts`, which threads it through from `setVersions()` to this same `lookup()`.
 */
export type BrokerVersions = Readonly<Record<number, { maxVersion: number } | undefined>>;

/**
 * `lookup(brokerVersions)(apiKey, family)` picks `min(highest version we implement, highest the
 * broker advertised)` and returns that version's factory, or throws if the broker never
 * advertised the API at all.
 */
export function lookup(
  brokerVersions: BrokerVersions,
): <Options>(apiKey: number, family: RequestFamily<Options>) => ProtocolFactory<Options> {
  return (apiKey, family) => {
    const version = brokerVersions[apiKey];
    if (version == null || version.maxVersion == null) {
      throw new KafkaServerDoesNotSupportApiKey('The Kafka server does not support the requested API version', {
        apiKey,
        apiName: apiKeyName(apiKey),
      });
    }

    const bestImplementedVersion = Math.max(...family.versions);
    const bestSupportedVersion = Math.min(bestImplementedVersion, version.maxVersion);
    return family.protocol({ version: bestSupportedVersion });
  };
}
