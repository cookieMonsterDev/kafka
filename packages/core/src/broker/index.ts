import {
  KafkaConnectionClosedError,
  KafkaInvalidVarIntError,
  KafkaInvariantViolation,
  KafkaProtocolError,
} from '../errors';
import type { Logger } from '../loggers/index';
import type { ConnectionPool } from '../network/connection-pool';
import { asTypedSend } from '../network/connection';
import { ApiVersions } from '../protocol/requests/api-versions/index';
import type { ApiVersionsResponseV1Body } from '../protocol/requests/api-versions/v1/response';
import type { ApiVersionsResponseV3Body } from '../protocol/requests/api-versions/v3/response';
import { lookup } from '../protocol/requests/index';
import type {
  BrokerVersions,
  LazyRequestFamily,
  ProtocolFactory,
  ProtocolResult,
  RequestFamily,
} from '../protocol/requests/index';
import { SaslAuthenticate } from '../protocol/requests/sasl-authenticate/index';
import { Lock } from '../utils/lock';

type LookupRequest = ReturnType<typeof lookup>;

const notInitializedLookup: LookupRequest = () => {
  throw new Error('Broker not connected');
};

/** Pre-flexible brokers answer ApiVersions v3+ with a v0/v1 body instead of UNSUPPORTED_VERSION. */
function isApiVersionsNegotiationFallback(error: unknown): boolean {
  if (error instanceof KafkaProtocolError) return error.type === 'UNSUPPORTED_VERSION';
  return error instanceof RangeError || error instanceof KafkaInvalidVarIntError;
}

export interface BrokerOptions {
  connectionPool: ConnectionPool;
  logger: Logger;
  nodeId?: number | null;
  versions?: BrokerVersions | null;
  authenticationTimeout?: number;
  allowAutoTopicCreation?: boolean;
}

/**
 * One connection to a broker: `connect()` negotiates `ApiVersions` and authenticates, and `send`
 * runs one request of any family with the version negotiated through `lookupRequest`.
 */
export class Broker {
  connectionPool: ConnectionPool;
  nodeId: number | null;
  rootLogger: Logger;
  logger: Logger;
  versions: BrokerVersions | null;
  authenticationTimeout: number;
  allowAutoTopicCreation: boolean;
  brokerAddress: string;
  lock: Lock;
  lookupRequest: LookupRequest;

  constructor({
    connectionPool,
    logger,
    nodeId = null,
    versions = null,
    authenticationTimeout = 10_000,
    allowAutoTopicCreation = true,
  }: BrokerOptions) {
    this.connectionPool = connectionPool;
    this.nodeId = nodeId;
    this.rootLogger = logger;
    this.logger = logger.namespace('Broker');
    this.versions = versions;
    this.authenticationTimeout = authenticationTimeout;
    this.allowAutoTopicCreation = allowAutoTopicCreation;

    // The lock timeout has twice the connectionTimeout because the same timeout is used for the
    // first ApiVersions call.
    const lockTimeout = 2 * this.connectionPool.connectionTimeout + this.authenticationTimeout;
    this.brokerAddress = `${this.connectionPool.host}:${this.connectionPool.port}`;

    this.lock = new Lock({ timeout: lockTimeout, description: `connect to broker ${this.brokerAddress}` });
    this.lookupRequest = notInitializedLookup;
  }

  isConnected(): boolean {
    return this.connectionPool.sasl
      ? this.connectionPool.isConnected() && this.connectionPool.isAuthenticated()
      : this.connectionPool.isConnected();
  }

  async connect(): Promise<void> {
    await this.lock.acquire();
    try {
      if (this.isConnected()) return;

      const connection = await this.connectionPool.getConnection();

      if (!this.versions) {
        this.versions = await this.apiVersions();
      }
      this.connectionPool.setVersions(this.versions);

      this.lookupRequest = lookup(this.versions);

      if (connection.getSupportAuthenticationProtocol() === null) {
        let supportAuthenticationProtocol = false;
        try {
          this.lookupRequest(SaslAuthenticate);
          supportAuthenticationProtocol = true;
        } catch {
          supportAuthenticationProtocol = false;
        }
        this.connectionPool.setSupportAuthenticationProtocol(supportAuthenticationProtocol);

        this.logger.debug('Verified support for SaslAuthenticate', {
          broker: this.brokerAddress,
          supportAuthenticationProtocol,
        });
      }

      await connection.authenticate();
    } finally {
      await this.lock.release();
    }
  }

  async disconnect(): Promise<void> {
    await this.connectionPool.destroy();
  }

  async apiVersions(): Promise<BrokerVersions> {
    let response: ApiVersionsResponseV1Body | undefined;
    const availableVersions = [...ApiVersions.versions].sort((a, b) => b - a);
    const oldestVersion = availableVersions[availableVersions.length - 1];

    // Find the best version implemented by the server. Flexible ApiVersions (v3+) uses a v2
    // request header that pre-KIP-482 brokers cannot parse; they reply with a non-flexible body
    // which throws RangeError on tagged-field decode instead of UNSUPPORTED_VERSION.
    for (const candidateVersion of availableVersions) {
      try {
        const apiVersions = ApiVersions.protocol({ version: candidateVersion });
        response = await this.#sendRequest<ApiVersionsResponseV1Body>({
          ...apiVersions({}),
          requestTimeout: this.connectionPool.connectionTimeout,
        });
        break;
      } catch (e) {
        if (candidateVersion !== oldestVersion && isApiVersionsNegotiationFallback(e)) {
          this.logger.debug('ApiVersions negotiation falling back', {
            broker: this.brokerAddress,
            candidateVersion,
            error: (e as Error).message,
          });
          continue;
        }
        throw e;
      }
    }

    if (!response) {
      throw new KafkaProtocolError({ message: 'API Versions not supported', retriable: false });
    }

    const versions: Record<number, { minVersion: number; maxVersion: number }> = {};
    for (const version of response.apiVersions) {
      versions[version.apiKey] = { minVersion: version.minVersion, maxVersion: version.maxVersion };
    }
    return versions;
  }

  /** ApiVersions v3+ with KIP-584 supported and finalized feature metadata. */
  async describeFeatures(): Promise<ApiVersionsResponseV3Body> {
    const advertised = this.versions?.[ApiVersions.apiKey];
    const brokerMax = advertised?.maxVersion ?? 0;
    const featureVersions = ApiVersions.versions.filter((version) => version >= 3 && version <= brokerMax);
    if (featureVersions.length === 0) {
      throw new KafkaProtocolError({
        message: 'DescribeFeatures requires ApiVersions v3 or newer on the broker',
        retriable: false,
      });
    }

    const version = Math.max(...featureVersions);
    const apiVersions = ApiVersions.protocol({ version });
    const response = await this.#sendRequest<ApiVersionsResponseV3Body>({
      ...apiVersions({}),
      requestTimeout: this.connectionPool.connectionTimeout,
    });
    return requireResponse(response);
  }

  /**
   * Sends one `family` request with the version negotiated for this broker and resolves to the
   * family's response. A family that loads on demand is imported first. Any other family builds and
   * frames its request synchronously, without a promise hop, and a synchronous throw still rejects.
   */
  send<Options, Response>(
    family: RequestFamily<Options, Response> | LazyRequestFamily<Options, Response>,
    options: NoInfer<Options>,
  ): Promise<Response> {
    if ('load' in family) {
      return family.load().then((loaded) => this.send(loaded, options));
    }

    try {
      const response = this.#sendOnce(family, options);
      if (family.retryOptions === undefined) return response;

      return response.catch((error: unknown) => {
        const retryOptions = family.retryOptions?.(error, options);
        if (retryOptions === undefined) throw error;
        return this.#sendOnce(family, retryOptions);
      });
    } catch (e) {
      // eslint-disable-next-line @typescript-eslint/prefer-promise-reject-errors -- forward the failure as-is
      return Promise.reject(e);
    }
  }

  #sendOnce<Options, Response>(family: RequestFamily<Options, Response>, callerOptions: Options): Promise<Response> {
    const options = family.prepareOptions === undefined ? callerOptions : family.prepareOptions(callerOptions);
    let protocol = this.lookupRequest(family)(options);

    if (family.preferVersion !== undefined) {
      const negotiatedVersion = protocol.request.apiVersion;
      const advertised = this.versions?.[family.apiKey];
      if (advertised === undefined) {
        throw new KafkaInvariantViolation(`no advertised ${family.apiName} versions after negotiating one`);
      }
      const version = family.preferVersion(options, negotiatedVersion, advertised);
      if (version !== negotiatedVersion) protocol = family.protocol({ version })(options);
    }

    if (family.parseResponse !== undefined) protocol = withParseResponse(protocol, family, options);

    const response = this.#sendRequest<Response>(protocol);
    // Only a request that may expect no response (Produce, with `acks: 0`) resolves without one; its
    // family's response type includes `undefined`. Every other response is checked here once.
    if (protocol.request.expectResponse !== undefined) return response as Promise<Response>;
    return response.then(requireResponse);
  }

  #sendRequest<T>(protocolResult: ProtocolResult): Promise<T | undefined> {
    return this.connectionPool.send<T>(asTypedSend<T>(protocolResult)).catch(async (e: unknown) => {
      if (e instanceof KafkaConnectionClosedError) {
        await this.disconnect();
      }
      throw e;
    });
  }
}

/** Parses the response with the family's `parseResponse` hook, which may call the version's own `parse`. */
function withParseResponse<Options, Response>(
  protocol: ProtocolResult,
  family: RequestFamily<Options, Response>,
  options: Options,
): ProtocolResult {
  const { response } = protocol;
  return {
    ...protocol,
    response: {
      decode: (rawData: Buffer) => response.decode(rawData),
      parse: (body: unknown) => family.parseResponse?.(body, options, (versionBody) => response.parse(versionBody)),
    },
  };
}

function requireResponse<Response>(response: Response | undefined): Response {
  if (response === undefined) {
    throw new KafkaInvariantViolation('Broker request unexpectedly returned no response');
  }
  return response;
}

export type { ProtocolFactory };
