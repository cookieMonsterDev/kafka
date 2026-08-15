import { KafkaJSNotImplemented, KafkaJSServerDoesNotSupportApiKey } from '../../errors.js';
import type { Encoder } from '../encoder.js';
import { apiKeyName } from './api-keys.js';

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
 * The per-version factory a family's `protocol({version})` resolves to, e.g. SaslHandshake's is
 * `(values: {mechanism: string}) => ProtocolResult`, ApiVersions's is `(values: {}) => ProtocolResult`
 * — every family takes exactly one options object, mirroring kafkajs's own calling convention
 * (`metadata({topics, allowAutoTopicCreation})`). `Options` carries the real per-family field
 * shape through `RequestFamily<Options>`, so a concrete family export (e.g. `SaslHandshake`) is
 * fully typed at its call site; only `NOT_IMPLEMENTED_REQUEST_DEFINITIONS` and other
 * apiKey-generic code default it to `unknown`.
 */
export type ProtocolFactory<Options> = (values: Options) => ProtocolResult;

export interface RequestFamily<Options = unknown> {
  readonly versions: readonly number[];
  protocol(options: { version: number }): ProtocolFactory<Options>;
}

export const NOT_IMPLEMENTED_REQUEST_DEFINITIONS: RequestFamily<never> = Object.freeze({
  versions: Object.freeze([]),
  protocol(): never {
    throw new KafkaJSNotImplemented('This API is not implemented');
  },
});

/**
 * `lookup(brokerVersions)(apiKey, family)` picks `min(highest version we implement, highest the
 * broker advertised)` and returns that version's factory, or throws if the broker never
 * advertised the API at all.
 */
export function lookup(
  brokerVersions: Readonly<Record<number, { maxVersion: number } | undefined>>,
): <Options>(apiKey: number, family: RequestFamily<Options>) => ProtocolFactory<Options> {
  return (apiKey, family) => {
    const version = brokerVersions[apiKey];
    if (version == null || version.maxVersion == null) {
      throw new KafkaJSServerDoesNotSupportApiKey('The Kafka server does not support the requested API version', {
        apiKey,
        apiName: apiKeyName(apiKey),
      });
    }

    const bestImplementedVersion = Math.max(...family.versions);
    const bestSupportedVersion = Math.min(bestImplementedVersion, version.maxVersion);
    return family.protocol({ version: bestSupportedVersion });
  };
}
