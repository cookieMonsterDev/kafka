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
  requestTimeout?: number;
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
 * The per-apiKey version range a broker advertised via `ApiVersions`. `minVersion` is optional so
 * older call sites can pass only `maxVersion`; lookup treats a missing floor as `0`.
 */
export interface BrokerApiVersion {
  minVersion?: number;
  maxVersion: number;
}

/**
 * The per-apiKey version range a broker advertised via `ApiVersions`, keyed by apiKey. Shared with
 * `network/connection.ts`, which threads it through from `setVersions()` to this same `lookup()`.
 */
export type BrokerVersions = Readonly<Record<number, BrokerApiVersion | undefined>>;

function formatVersionRange(versions: readonly number[]): string {
  if (versions.length === 0) {
    return '(none)';
  }
  const sorted = [...versions].sort((a, b) => a - b);
  const first = sorted[0];
  const last = sorted[sorted.length - 1];
  if (first === last) {
    return String(first);
  }
  return `${first}-${last}`;
}

function unsupportedApiKeyError(
  apiKey: number,
  extras: {
    brokerMinVersion?: number;
    brokerMaxVersion?: number;
    implementedVersions?: readonly number[];
  } = {},
): KafkaServerDoesNotSupportApiKey {
  const apiName = apiKeyName(apiKey);
  const label = apiName != null ? `${apiName} (${apiKey})` : `API key ${apiKey}`;
  const { brokerMinVersion, brokerMaxVersion, implementedVersions } = extras;

  let message = `The Kafka server does not support the requested API version`;
  if (brokerMaxVersion != null && implementedVersions != null) {
    message = `The Kafka server does not support ${label}. Broker advertised ${brokerMinVersion ?? 0}-${brokerMaxVersion}; client implements ${formatVersionRange(implementedVersions)}.`;
  }

  return new KafkaServerDoesNotSupportApiKey(message, {
    apiKey,
    apiName,
    brokerMinVersion,
    brokerMaxVersion,
    implementedVersions,
  });
}

/**
 * `lookup(brokerVersions)(apiKey, family)` picks the highest `v` in `family.versions` such that
 * `broker.minVersion <= v <= broker.maxVersion`. Throws `KafkaServerDoesNotSupportApiKey` when the
 * broker never advertised the API or when there is no overlap with the versions we implement.
 */
export function lookup(
  brokerVersions: BrokerVersions,
): <Options>(apiKey: number, family: RequestFamily<Options>) => ProtocolFactory<Options> {
  return (apiKey, family) => {
    const version = brokerVersions[apiKey];
    if (version == null || version.maxVersion == null) {
      throw unsupportedApiKeyError(apiKey);
    }

    const brokerMinVersion = version.minVersion ?? 0;
    const brokerMaxVersion = version.maxVersion;
    const overlapping = family.versions.filter((v) => v >= brokerMinVersion && v <= brokerMaxVersion);

    if (overlapping.length === 0) {
      throw unsupportedApiKeyError(apiKey, {
        brokerMinVersion,
        brokerMaxVersion,
        implementedVersions: family.versions,
      });
    }

    const bestSupportedVersion = Math.max(...overlapping);
    return family.protocol({ version: bestSupportedVersion });
  };
}
