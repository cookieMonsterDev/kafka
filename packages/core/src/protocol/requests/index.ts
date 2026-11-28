import { KafkaInvariantViolation, KafkaServerDoesNotSupportApiKey } from '../../errors';
import type { Encoder } from '../encoder';
import type { KafkaApi } from './kafka-apis';

export type { KafkaApi } from './kafka-apis';

export interface AnyRequestDefinition {
  apiKey: number;
  apiVersion: number;
  apiName: string;
  encode(): Encoder | Promise<Encoder>;
  expectResponse?(): boolean;
}

export interface AnyResponseDefinition {
  decode(rawData: Buffer): unknown;
  parse(data: unknown): unknown;
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

/** A family's factories keyed by version, as `defineRequestFamily` takes them. */
export type VersionFactories<Options> = Readonly<Record<number, ProtocolFactory<Options>>>;

/**
 * Optional static behaviour a family adds around the plain exchange `Broker#send` runs: pick the
 * negotiated version, build the request, and parse the response with that version's `parse`.
 */
export interface RequestFamilyHooks<Options, Response> {
  /** Normalizes the caller's options before the request is built, e.g. Fetch's KIP-74 partition shuffle. */
  readonly prepareOptions?: (options: Options) => Options;
  /**
   * Version preference: the version to send for these options instead of the negotiated one, which
   * is the highest version both sides support. It must be one of the family's versions.
   */
  readonly preferVersion?: (options: Options, negotiatedVersion: number, advertised: BrokerApiVersion) => number;
  /** Post-processing: parses the decoded body in place of the version's own `parse`, which it may call. */
  readonly parseResponse?: (
    body: unknown,
    options: Options,
    parse: (body: unknown) => unknown,
  ) => Response | Promise<Response>;
  /** Post-processing: the options to send once more after `error`, or `undefined` to fail with it. */
  readonly retryOptions?: (error: unknown, options: Options) => Options | undefined;
}

/**
 * One Kafka API's request family: the facts about the API, the versions this client implements, a
 * factory per version, and the response type `Broker#send` resolves to for every version.
 */
export interface RequestFamily<Options = unknown, Response = unknown>
  extends KafkaApi, RequestFamilyHooks<Options, Response> {
  readonly versions: readonly number[];
  protocol(options: { version: number }): ProtocolFactory<Options>;
}

/**
 * A family whose codecs load on first use, so applications that never send it never load them.
 * It carries the API facts up front; `load` imports the family once and returns the same promise
 * on every later call.
 */
export interface LazyRequestFamily<Options = unknown, Response = unknown> extends KafkaApi {
  load(): Promise<RequestFamily<Options, Response>>;
}

type AnyVersionFactories = Readonly<Record<number, (options: never) => ProtocolResult>>;

/** The options every factory in a version table takes. */
type VersionOptions<Versions extends AnyVersionFactories> = Versions[keyof Versions] extends (
  options: infer Options,
) => ProtocolResult
  ? Options
  : never;

/** What one version's factory resolves to once its response is parsed. */
type ParsedBody<Factory> = Factory extends (options: never) => { response: { parse(data: never): infer Parsed } }
  ? Awaited<Parsed>
  : never;

/** The response type a version table resolves to: the union of every version's parsed body. */
export type VersionResponse<Versions extends AnyVersionFactories> = {
  [Version in keyof Versions]: ParsedBody<Versions[Version]>;
}[keyof Versions];

/**
 * Builds a family from its API facts and version table. Without a `parseResponse` hook the
 * family's response type is the union of every version's parsed body, so callers handle the
 * fields an older version leaves out.
 */
export function defineRequestFamily<Versions extends AnyVersionFactories, Response = VersionResponse<Versions>>(
  api: KafkaApi,
  versions: Versions,
  hooks: RequestFamilyHooks<VersionOptions<Versions>, Response> = {},
): RequestFamily<VersionOptions<Versions>, Response> {
  // Every family has the same properties, hooks or not, so `Broker#send` reads one object shape.
  return Object.freeze({
    apiKey: api.apiKey,
    apiName: api.apiName,
    firstFlexibleVersion: api.firstFlexibleVersion,
    versions: Object.freeze(Object.keys(versions).map(Number)),
    protocol({ version }: { version: number }) {
      const factory: Versions[number] | undefined = versions[version];
      if (!factory) throw new Error(`Invariant violated: no ${api.apiName} protocol for version ${version}`);
      // Every factory in the table takes `VersionOptions<Versions>`; the constraint only says `never`.
      return factory as ProtocolFactory<VersionOptions<Versions>>;
    },
    prepareOptions: hooks.prepareOptions,
    preferVersion: hooks.preferVersion,
    parseResponse: hooks.parseResponse,
    retryOptions: hooks.retryOptions,
  });
}

/** A stub for a family that loads on first use; `load` imports it once. */
export function lazyRequestFamily<Options, Response>(
  api: KafkaApi,
  load: () => Promise<RequestFamily<Options, Response>>,
): LazyRequestFamily<Options, Response> {
  let loaded: Promise<RequestFamily<Options, Response>> | undefined;
  return Object.freeze({
    apiKey: api.apiKey,
    apiName: api.apiName,
    firstFlexibleVersion: api.firstFlexibleVersion,
    load: () => (loaded ??= load()),
  });
}

/**
 * Picks the highest of `versions` below `limit` that the broker accepts, for a family whose newer
 * versions need input the options lack (such as topic ids from Produce and Fetch v13).
 */
export function highestVersionBelow(
  api: KafkaApi,
  versions: readonly number[],
  limit: number,
  advertised: BrokerApiVersion,
): number {
  const brokerMin = advertised.minVersion ?? 0;
  const candidates = versions.filter(
    (version) => version < limit && version >= brokerMin && version <= advertised.maxVersion,
  );
  if (candidates.length === 0) {
    throw new KafkaInvariantViolation(`no ${api.apiName} version below ${limit} for this broker`);
  }
  return Math.max(...candidates);
}

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
  { apiKey, apiName }: KafkaApi,
  extras: {
    brokerMinVersion?: number;
    brokerMaxVersion?: number;
    implementedVersions?: readonly number[];
  } = {},
): KafkaServerDoesNotSupportApiKey {
  const { brokerMinVersion, brokerMaxVersion, implementedVersions } = extras;

  let message = `The Kafka server does not support the requested API version`;
  if (brokerMaxVersion != null && implementedVersions != null) {
    message = `The Kafka server does not support ${apiName} (${apiKey}). Broker advertised ${brokerMinVersion ?? 0}-${brokerMaxVersion}; client implements ${formatVersionRange(implementedVersions)}.`;
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
 * The highest of `versions` within the range the broker advertised, which is the version `lookup`
 * negotiates, or `null` when the two share none.
 */
export function highestCommonVersion(versions: readonly number[], advertised: BrokerApiVersion): number | null {
  const brokerMinVersion = advertised.minVersion ?? 0;
  const overlapping = versions.filter((v) => v >= brokerMinVersion && v <= advertised.maxVersion);
  return overlapping.length === 0 ? null : Math.max(...overlapping);
}

/**
 * `lookup(brokerVersions)(family)` picks the highest `v` in `family.versions` such that
 * `broker.minVersion <= v <= broker.maxVersion`. Throws `KafkaServerDoesNotSupportApiKey` when the
 * broker never advertised the API or when there is no overlap with the versions we implement.
 * The negotiated factory is cached per family, since `brokerVersions` is fixed for one lookup.
 */
export function lookup(
  brokerVersions: BrokerVersions,
): <Options, Response>(family: RequestFamily<Options, Response>) => ProtocolFactory<Options> {
  const negotiatedFactories = new Map<object, ProtocolFactory<never>>();

  return <Options, Response>(family: RequestFamily<Options, Response>): ProtocolFactory<Options> => {
    const cached = negotiatedFactories.get(family) as ProtocolFactory<Options> | undefined;
    if (cached !== undefined) {
      return cached;
    }

    const version = brokerVersions[family.apiKey];
    if (version == null || version.maxVersion == null) {
      throw unsupportedApiKeyError(family);
    }

    const bestSupportedVersion = highestCommonVersion(family.versions, version);
    if (bestSupportedVersion === null) {
      throw unsupportedApiKeyError(family, {
        brokerMinVersion: version.minVersion ?? 0,
        brokerMaxVersion: version.maxVersion,
        implementedVersions: family.versions,
      });
    }

    const factory = family.protocol({ version: bestSupportedVersion });
    negotiatedFactories.set(family, factory);
    return factory;
  };
}
