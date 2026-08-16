import type { Socket } from 'node:net';
import type { ConnectionOptions as TlsConnectionOptions } from 'node:tls';
import { INT_32_MAX_VALUE } from '../constants.js';
import { KafkaJSConnectionClosedError, KafkaJSConnectionError } from '../errors.js';
import type { InstrumentationEventEmitter } from '../instrumentation/emitter.js';
import type { Logger } from '../loggers/index.js';
import { createRequest } from '../protocol/request.js';
import { Decoder } from '../protocol/decoder.js';
import { API_KEYS } from '../protocol/requests/api-keys.js';
import type { AnyRequestDefinition, BrokerVersions } from '../protocol/requests/index.js';
import { sharedPromiseTo } from '../utils/shared-promise-to.js';
import { CONNECTED_STATUS, CONNECTION_STATUS } from './connection-status.js';
import type { ConnectionStatus } from './connection-status.js';
import type { NetworkEventMap } from './instrumentation-events.js';
import { RequestQueue } from './request-queue/index.js';
import type { RequestEntry } from './request-queue/socket-request.js';
import { createSocket } from './socket.js';
import type { SocketFactory } from './socket-factory.js';

const requestInfo = ({
  apiName,
  apiKey,
  apiVersion,
}: {
  apiName: string;
  apiKey: number;
  apiVersion: number;
}): string => `${apiName}(key: ${apiKey}, version: ${apiVersion})`;

const NON_AUTHENTICATED_API_KEYS: readonly number[] = [
  API_KEYS.ApiVersions,
  API_KEYS.SaslHandshake,
  API_KEYS.SaslAuthenticate,
];

const isAuthenticatedRequest = (request: Pick<AnyRequestDefinition, 'apiKey'>): boolean =>
  !NON_AUTHENTICATED_API_KEYS.includes(request.apiKey);

/**
 * The minimal shape `Connection` needs from a SASL config: a mechanism name, and (once resolved,
 * either by the caller or by a built-in provider) a way to run the actual exchange. Left generic
 * enough that a broker layer's concrete mechanism config is directly assignable here.
 */
export interface SaslConfig {
  mechanism: string;
  authenticationProvider?: (args: AuthenticationProviderArgs) => SaslAuthenticationProvider;
  [key: string]: unknown;
}

export interface AuthenticationProviderArgs {
  host: string;
  port: number;
  logger: Logger;
  saslAuthenticate<ParseResult>(args: {
    request: { encode(): Buffer | Promise<Buffer> };
    response?: { decode(rawResponse: Buffer): Buffer | Promise<Buffer>; parse(data: Buffer): ParseResult };
  }): Promise<ParseResult | void>;
}

export interface SaslAuthenticationProvider {
  authenticate(): Promise<void>;
}

export interface SaslAuthenticator {
  readonly sessionLifetime: bigint;
  authenticate(): Promise<void>;
}

/**
 * Builds the object that runs the actual SASL handshake/exchange for this connection. Kept as an
 * injected dependency (rather than an import of a concrete implementation) so this module has no
 * hard dependency on the broker layer's mechanism implementations.
 */
export type CreateSaslAuthenticator = (
  connection: Connection,
  logger: Logger,
  versions: BrokerVersions | null,
  supportAuthenticationProtocol: boolean | null,
) => SaslAuthenticator;

export interface ConnectionOptions {
  host: string;
  port: number;
  logger: Logger;
  socketFactory: SocketFactory;
  requestTimeout: number;
  connectionTimeout: number;
  reauthenticationThreshold?: number;
  rack?: string | null;
  ssl?: TlsConnectionOptions | null;
  sasl?: SaslConfig | null;
  clientId?: string;
  enforceRequestTimeout?: boolean;
  maxInFlightRequests?: number | null;
  instrumentationEmitter?: InstrumentationEventEmitter<NetworkEventMap> | null;
  createSaslAuthenticator?: CreateSaslAuthenticator;
}

interface AuthHandlers {
  onSuccess: (rawData: Buffer) => void;
  onError: () => void;
}

/**
 * The optional KIP-219 client-side throttle hint `Connection#send` reads off the decoded response
 * before handing it to `parse`. Read via a cast rather than a generic constraint on `T` — a
 * constraint here would make every concrete response type "weak" (all-optional) against
 * TypeScript's own eyes and defeat inference at call sites that don't carry this field at all.
 */
interface ThrottleableResponseData {
  clientSideThrottleTime?: number;
}

export interface ConnectionResponseDefinition<T> {
  decode(rawData: Buffer): Promise<T>;
  parse(data: T): Promise<T>;
}

export interface SendOptions<T> {
  request: AnyRequestDefinition;
  response: ConnectionResponseDefinition<T>;
  requestTimeout?: number | null;
  logResponseError?: boolean;
}

/**
 * One TCP (or TLS) connection to a single broker: frames requests onto the wire, demultiplexes
 * responses by correlation id via `RequestQueue`, and lazily authenticates over SASL before the
 * first non-bootstrap request.
 */
export class Connection {
  readonly host: string;
  readonly port: number;
  readonly rack: string | null;
  readonly clientId: string;
  readonly broker: string;
  readonly logger: Logger;
  readonly requestQueue: RequestQueue;
  readonly sasl: SaslConfig | null;

  readonly #socketFactory: SocketFactory;
  readonly #ssl: TlsConnectionOptions | null;
  readonly #connectionTimeout: number;
  readonly #reauthenticationThreshold: number;
  readonly #shouldLogBuffers: boolean;
  readonly #shouldLogFetchBuffer: boolean;
  readonly #createSaslAuthenticator: CreateSaslAuthenticator | undefined;

  #socket: Socket | undefined;
  #connectionStatus: ConnectionStatus = CONNECTION_STATUS.DISCONNECTED;
  #correlationId = 0;
  #versions: BrokerVersions | null = null;
  #supportAuthenticationProtocol: boolean | null = null;
  #authHandlers: AuthHandlers | null = null;
  #authExpectResponse = false;
  #authenticatedAt: [number, number] | null = null;
  #sessionLifetime = 0n;

  #bytesBuffered = 0;
  #bytesNeeded = Decoder.int32Size();
  #chunks: Buffer[] = [];

  #authenticate = sharedPromiseTo(async (): Promise<void> => {
    if (this.sasl && !this.isAuthenticated()) {
      if (!this.#createSaslAuthenticator) {
        throw new KafkaJSConnectionError('SASL is configured but no SASL authenticator was provided', {
          broker: this.broker,
        });
      }

      const authenticator = this.#createSaslAuthenticator(
        this,
        this.logger,
        this.#versions,
        this.#supportAuthenticationProtocol,
      );

      await authenticator.authenticate();
      this.#authenticatedAt = process.hrtime();
      this.#sessionLifetime = authenticator.sessionLifetime;
    }
  });

  constructor(options: ConnectionOptions) {
    const {
      host,
      port,
      logger,
      socketFactory,
      requestTimeout,
      reauthenticationThreshold = 10_000,
      rack = null,
      ssl = null,
      sasl = null,
      clientId = 'kafkajs',
      connectionTimeout,
      enforceRequestTimeout = true,
      maxInFlightRequests = null,
      instrumentationEmitter = null,
      createSaslAuthenticator,
    } = options;

    this.host = host;
    this.port = port;
    this.rack = rack;
    this.clientId = clientId;
    this.broker = `${host}:${port}`;
    this.logger = logger.namespace('Connection');

    this.#socketFactory = socketFactory;
    this.#ssl = ssl;
    this.sasl = sasl;
    this.#createSaslAuthenticator = createSaslAuthenticator;

    this.#connectionTimeout = connectionTimeout;
    this.#reauthenticationThreshold = reauthenticationThreshold;

    this.requestQueue = new RequestQueue({
      instrumentationEmitter,
      maxInFlightRequests,
      requestTimeout,
      enforceRequestTimeout,
      clientId,
      broker: this.broker,
      logger: logger.namespace('RequestQueue'),
      isConnected: () => this.isConnected(),
    });

    this.#shouldLogBuffers = process.env.KAFKAJS_DEBUG_PROTOCOL_BUFFERS === '1';
    this.#shouldLogFetchBuffer = this.#shouldLogBuffers && process.env.KAFKAJS_DEBUG_EXTENDED_PROTOCOL_BUFFERS === '1';
  }

  #logDebug(message: string, extra: Record<string, unknown> = {}): void {
    this.logger.debug(message, { broker: this.broker, clientId: this.clientId, ...extra });
  }

  #logError(message: string, extra: Record<string, unknown> = {}): void {
    this.logger.error(message, { broker: this.broker, clientId: this.clientId, ...extra });
  }

  getSupportAuthenticationProtocol(): boolean | null {
    return this.#supportAuthenticationProtocol;
  }

  setSupportAuthenticationProtocol(isSupported: boolean | null): void {
    this.#supportAuthenticationProtocol = isSupported;
  }

  setVersions(versions: BrokerVersions | null): void {
    this.#versions = versions;
  }

  isConnected(): boolean {
    return CONNECTED_STATUS.includes(this.#connectionStatus);
  }

  connect(): Promise<boolean> {
    return new Promise((resolve, reject) => {
      if (this.isConnected()) {
        resolve(true);
        return;
      }

      this.#authenticatedAt = null;

      let timeoutId: NodeJS.Timeout;

      const onConnect = (): void => {
        clearTimeout(timeoutId);
        this.#connectionStatus = CONNECTION_STATUS.CONNECTED;
        resolve(true);
      };

      const onData = (data: Buffer): void => {
        this.#processData(data);
      };

      const onEnd = (): void => {
        void (async () => {
          clearTimeout(timeoutId);

          const wasConnected = this.isConnected();

          if (this.#authHandlers) {
            this.#authHandlers.onError();
          } else if (wasConnected) {
            this.#logDebug('Kafka server has closed connection');
            this.#rejectRequests(
              new KafkaJSConnectionClosedError('Closed connection', { host: this.host, port: this.port }),
            );
          }

          await this.disconnect();
        })();
      };

      const onError = (e: Error & { code?: string }): void => {
        void (async () => {
          clearTimeout(timeoutId);

          const error = new KafkaJSConnectionError(`Connection error: ${e.message}`, {
            broker: `${this.host}:${this.port}`,
            code: e.code,
          });

          this.#logError(error.message, { stack: e.stack });
          this.#rejectRequests(error);
          await this.disconnect();

          reject(error);
        })();
      };

      const onTimeout = (): void => {
        void (async () => {
          const error = new KafkaJSConnectionError('Connection timeout', {
            broker: `${this.host}:${this.port}`,
          });

          this.#logError(error.message);
          this.#rejectRequests(error);
          await this.disconnect();
          reject(error);
        })();
      };

      this.#logDebug('Connecting', { ssl: !!this.#ssl, sasl: !!this.sasl });

      try {
        timeoutId = setTimeout(onTimeout, this.#connectionTimeout);
        this.#socket = createSocket({
          socketFactory: this.#socketFactory,
          host: this.host,
          port: this.port,
          ssl: this.#ssl,
          onConnect,
          onData,
          onEnd,
          onError,
          onTimeout,
        });
      } catch (e) {
        clearTimeout(timeoutId!);
        reject(
          new KafkaJSConnectionError(`Failed to connect: ${(e as Error).message}`, {
            broker: `${this.host}:${this.port}`,
          }),
        );
      }
    });
  }

  async disconnect(): Promise<boolean> {
    this.#authenticatedAt = null;
    this.#connectionStatus = CONNECTION_STATUS.DISCONNECTING;
    this.#logDebug('disconnecting...');

    await this.requestQueue.waitForPendingRequests();
    this.requestQueue.destroy();

    if (this.#socket) {
      this.#socket.end();
      this.#socket.unref();
    }

    this.#connectionStatus = CONNECTION_STATUS.DISCONNECTED;
    this.#logDebug('disconnected');
    return true;
  }

  isAuthenticated(): boolean {
    return this.#authenticatedAt != null && !this.#shouldReauthenticate();
  }

  #shouldReauthenticate(): boolean {
    if (this.#sessionLifetime === 0n) {
      return false;
    }

    if (this.#authenticatedAt == null) {
      return true;
    }

    const [secondsSince, remainingNanosSince] = process.hrtime(this.#authenticatedAt);
    const millisSince = BigInt(secondsSince) * 1000n + BigInt(remainingNanosSince) / 1_000_000n;

    const reauthenticateAt = millisSince + BigInt(this.#reauthenticationThreshold);
    return reauthenticateAt >= this.#sessionLifetime;
  }

  async authenticate(): Promise<void> {
    await this.#authenticate();
  }

  sendAuthRequest<T>({
    request,
    response,
  }: {
    request: Pick<AnyRequestDefinition, 'encode'>;
    response: { decode(rawData: Buffer): Promise<T>; parse(data: T): Promise<T> };
  }): Promise<T> {
    this.#authExpectResponse = true;

    return new Promise((resolve, reject) => {
      this.#authHandlers = {
        onSuccess: (rawData) => {
          this.#authHandlers = null;
          this.#authExpectResponse = false;

          response
            .decode(rawData)
            .then((data) => response.parse(data))
            .then(resolve)
            // eslint-disable-next-line @typescript-eslint/prefer-promise-reject-errors -- forward the decode/parse rejection reason as-is
            .catch((e: unknown) => reject(e));
        },
        onError: () => {
          this.#authHandlers = null;
          this.#authExpectResponse = false;

          reject(
            new KafkaJSConnectionError('Connection closed by the server', {
              broker: `${this.host}:${this.port}`,
            }),
          );
        },
      };

      void (async () => {
        try {
          const requestPayload = await request.encode();
          this.#failIfNotConnected();
          this.#socket!.write(requestPayload.buffer, 'binary');
        } catch (e) {
          // eslint-disable-next-line @typescript-eslint/prefer-promise-reject-errors -- forward the encode/write failure as-is
          reject(e);
        }
      })();
    });
  }

  async send<T>({
    request,
    response,
    requestTimeout = null,
    logResponseError = true,
  }: SendOptions<T>): Promise<T | undefined> {
    if (!this.isAuthenticated() && isAuthenticatedRequest(request)) {
      await this.#authenticate();
    }

    this.#failIfNotConnected();

    const expectResponse = !request.expectResponse || request.expectResponse();

    const sendRequest = async (): Promise<{
      correlationId: number;
      size: number;
      entry: RequestEntry;
      payload: Buffer | null;
    }> => {
      const { clientId } = this;
      const correlationId = this.#nextCorrelationId();

      const requestPayload = await createRequest({ request, correlationId, clientId });
      const { apiKey, apiName, apiVersion } = request;
      this.#logDebug(`Request ${requestInfo(request)}`, {
        correlationId,
        expectResponse,
        size: Buffer.byteLength(requestPayload.buffer),
      });

      return new Promise((resolve, reject) => {
        try {
          this.#failIfNotConnected();
          const entry: RequestEntry = { apiKey, apiName, apiVersion, correlationId, resolve, reject };

          this.requestQueue.push({
            entry,
            expectResponse,
            requestTimeout,
            sendRequest: () => {
              this.#socket!.write(requestPayload.buffer, 'binary');
            },
          });
        } catch (e) {
          // eslint-disable-next-line @typescript-eslint/prefer-promise-reject-errors -- forward the enqueue failure as-is
          reject(e);
        }
      });
    };

    const { correlationId, size, entry, payload } = await sendRequest();

    if (!expectResponse) {
      return undefined;
    }

    try {
      const payloadDecoded = await response.decode(payload!);

      // KIP-219: if the response says to throttle client-side, do that.
      this.requestQueue.maybeThrottle((payloadDecoded as ThrottleableResponseData).clientSideThrottleTime);

      const data = await response.parse(payloadDecoded);
      const isFetchApi = entry.apiName === 'Fetch';
      this.#logDebug(`Response ${requestInfo(entry)}`, {
        correlationId,
        size,
        data: isFetchApi && !this.#shouldLogFetchBuffer ? '[filtered]' : data,
      });

      return data;
    } catch (e) {
      const error = e as Error;

      if (logResponseError) {
        this.#logError(`Response ${requestInfo(entry)}`, {
          error: error.message,
          correlationId,
          size,
        });
      }

      const isBuffer = Buffer.isBuffer(payload);
      this.#logDebug(`Response ${requestInfo(entry)}`, {
        error: error.message,
        correlationId,
        payload: isBuffer && !this.#shouldLogBuffers ? { type: 'Buffer', data: '[filtered]' } : payload,
      });

      throw error;
    }
  }

  #failIfNotConnected(): void {
    if (!this.isConnected()) {
      throw new KafkaJSConnectionError('Not connected', { broker: `${this.host}:${this.port}` });
    }
  }

  #nextCorrelationId(): number {
    if (this.#correlationId >= INT_32_MAX_VALUE) {
      this.#correlationId = 0;
    }

    return this.#correlationId++;
  }

  #processData(rawData: Buffer): void {
    if (this.#authHandlers && !this.#authExpectResponse) {
      this.#authHandlers.onSuccess(rawData);
      return;
    }

    // Accumulate the new chunk.
    this.#chunks.push(rawData);
    this.#bytesBuffered += Buffer.byteLength(rawData);

    // Process data if there are enough bytes to read the expected response size,
    // otherwise keep buffering.
    while (this.#bytesNeeded <= this.#bytesBuffered) {
      const buffer = this.#chunks.length > 1 ? Buffer.concat(this.#chunks) : this.#chunks[0]!;
      const decoder = new Decoder(buffer);
      const expectedResponseSize = decoder.readInt32();

      // Return early if not enough bytes to read the full response.
      if (!decoder.canReadBytes(expectedResponseSize)) {
        this.#chunks = [buffer];
        this.#bytesBuffered = Buffer.byteLength(buffer);
        this.#bytesNeeded = Decoder.int32Size() + expectedResponseSize;
        return;
      }

      const response = new Decoder(decoder.readBytes(expectedResponseSize)!);

      // Reset the buffered chunks to whatever bytes remain.
      const remainderBuffer = decoder.readAll();
      this.#chunks = [remainderBuffer];
      this.#bytesBuffered = Buffer.byteLength(remainderBuffer);
      this.#bytesNeeded = Decoder.int32Size();

      if (this.#authHandlers) {
        const rawResponseSize = Decoder.int32Size() + expectedResponseSize;
        const rawResponseBuffer = buffer.subarray(0, rawResponseSize);
        this.#authHandlers.onSuccess(rawResponseBuffer);
        return;
      }

      const correlationId = response.readInt32();
      const payload = response.readAll();

      this.requestQueue.fulfillRequest({ size: expectedResponseSize, correlationId, payload });
    }
  }

  #rejectRequests(error: unknown): void {
    this.requestQueue.rejectAll(error);
  }
}
