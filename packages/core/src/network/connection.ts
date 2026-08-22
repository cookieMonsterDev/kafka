import type { Socket } from 'node:net';
import type { ConnectionOptions as TlsConnectionOptions } from 'node:tls';
import { INT_32_MAX_VALUE } from '../constants';
import { KafkaConnectionClosedError, KafkaConnectionError } from '../errors';
import type { InstrumentationEventEmitter } from '../instrumentation/emitter';
import type { Logger } from '../loggers/index';
import { createRequest } from '../protocol/request';
import { Decoder } from '../protocol/decoder';
import { usesFlexibleResponseHeader } from '../protocol/flexible';
import { API_KEYS } from '../protocol/requests/api-keys';
import type { AnyRequestDefinition, BrokerVersions, ProtocolResult } from '../protocol/requests/index';
import { sharedPromiseTo } from '../utils/shared-promise-to';
import { CONNECTED_STATUS, CONNECTION_STATUS } from './connection-status';
import type { ConnectionStatus } from './connection-status';
import type { NetworkEventMap } from './instrumentation-events';
import { RequestQueue } from './request-queue/index';
import type { RequestEntry } from './request-queue/socket-request';
import { createSocket } from './socket';
import type { SocketFactory } from './socket-factory';

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
  /**
   * `Decoded` and `ParseResult` are independent type parameters (not one type reused for both)
   * because a mechanism's `decode` and `parse` steps can genuinely differ - SCRAM's `decode`
   * yields a raw `Buffer` that `parse` only then turns into `{r, s, i, ...}`, while PLAIN's both
   * steps just resolve `true` without ever looking at the bytes.
   */
  saslAuthenticate: <Decoded, ParseResult = Decoded>(args: {
    request: { encode(): Buffer | Promise<Buffer> };
    response?: { decode(rawResponse: Buffer): Promise<Decoded>; parse(data: Decoded): Promise<ParseResult> };
  }) => Promise<ParseResult | undefined>;
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
 * A version-dispatched family's `protocol({version})(values)` result (`ProtocolResult` from
 * `protocol/requests/index.ts`) has an untyped `response` by design - the version picked is a
 * runtime decision, so its shape can't be known at the dispatch layer. Callers that know which
 * concrete response type a family resolves to (broker methods, the SASL authenticator) use this to
 * recover that type for `Connection#send`/`ConnectionPool#send`, in one clearly-labeled place
 * rather than casting ad hoc at every call site.
 */
export function asTypedSend<T>(protocolResult: ProtocolResult): SendOptions<T> {
  return {
    request: protocolResult.request,
    response: protocolResult.response as ConnectionResponseDefinition<T>,
    logResponseError: protocolResult.logResponseError,
    requestTimeout: protocolResult.requestTimeout,
  };
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

  #bytesNeeded = Decoder.int32Size();
  #receiveBuffer = Buffer.allocUnsafe(0);
  #receiveReadOffset = 0;
  #receiveWriteOffset = 0;

  #authenticate = sharedPromiseTo(async (): Promise<void> => {
    if (this.sasl && !this.isAuthenticated()) {
      if (!this.#createSaslAuthenticator) {
        throw new KafkaConnectionError('SASL is configured but no SASL authenticator was provided', {
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
      clientId = 'kafka',
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

    this.#shouldLogBuffers = process.env.KAFKA_DEBUG_PROTOCOL_BUFFERS === '1';
    this.#shouldLogFetchBuffer = this.#shouldLogBuffers && process.env.KAFKA_DEBUG_EXTENDED_PROTOCOL_BUFFERS === '1';
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
      this.#resetReceiveBuffer();

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
              new KafkaConnectionClosedError('Closed connection', { host: this.host, port: this.port }),
            );
          }

          await this.disconnect();
        })();
      };

      const onError = (e: Error & { code?: string }): void => {
        void (async () => {
          clearTimeout(timeoutId);

          const error = new KafkaConnectionError(`Connection error: ${e.message}`, {
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
          const error = new KafkaConnectionError('Connection timeout', {
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
          new KafkaConnectionError(`Failed to connect: ${(e as Error).message}`, {
            broker: `${this.host}:${this.port}`,
          }),
        );
      }
    });
  }

  async disconnect(): Promise<boolean> {
    this.#authenticatedAt = null;
    this.#connectionStatus = CONNECTION_STATUS.DISCONNECTING;
    this.#resetReceiveBuffer();
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

  /**
   * The pre-KIP-152 raw SASL exchange path: writes a mechanism's raw bytes directly to the socket
   * with no Kafka request framing, and (optionally) decodes the broker's raw reply the same way.
   * `response` is omitted for exchange steps that don't care about the reply's content — only that
   * one arrives — since every broker in scope for this port speaks `SaslAuthenticate` (KIP-152) and
   * never actually exercises this path; it exists for parity with the public `Authenticator` shape.
   */
  sendAuthRequest<Decoded, ParseResult = Decoded>({
    request,
    response,
  }: {
    request: { encode(): Buffer | Promise<Buffer> };
    response?: { decode(rawData: Buffer): Promise<Decoded>; parse(data: Decoded): Promise<ParseResult> };
  }): Promise<ParseResult | undefined> {
    this.#authExpectResponse = !!response;

    return new Promise((resolve, reject) => {
      this.#authHandlers = {
        onSuccess: (rawData) => {
          this.#authHandlers = null;
          this.#authExpectResponse = false;

          if (!response) {
            resolve(undefined);
            return;
          }

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
            new KafkaConnectionError('Connection closed by the server', {
              broker: `${this.host}:${this.port}`,
            }),
          );
        },
      };

      void (async () => {
        try {
          const requestPayload = await request.encode();
          this.#failIfNotConnected();
          this.#socket!.write(requestPayload, 'binary');
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
      throw new KafkaConnectionError('Not connected', { broker: `${this.host}:${this.port}` });
    }
  }

  #nextCorrelationId(): number {
    if (this.#correlationId >= INT_32_MAX_VALUE) {
      this.#correlationId = 0;
    }

    return this.#correlationId++;
  }

  #resetReceiveBuffer(): void {
    this.#receiveBuffer = Buffer.allocUnsafe(0);
    this.#receiveReadOffset = 0;
    this.#receiveWriteOffset = 0;
    this.#bytesNeeded = Decoder.int32Size();
  }

  /**
   * Copy `additional` bytes into the receive buffer. Compacts unparsed bytes to offset 0
   * (onto a fresh Buffer so in-flight payload views keep the previous backing store).
   */
  #ensureReceiveCapacity(additional: number): void {
    const remaining = this.#receiveWriteOffset - this.#receiveReadOffset;
    const needed = remaining + additional;
    if (this.#receiveReadOffset === 0 && needed <= this.#receiveBuffer.length) {
      return;
    }

    let capacity = this.#receiveBuffer.length;
    if (capacity < needed) {
      capacity = Math.max(capacity * 2, needed);
    }

    const next = Buffer.allocUnsafe(capacity);
    if (remaining > 0) {
      this.#receiveBuffer.copy(next, 0, this.#receiveReadOffset, this.#receiveWriteOffset);
    }
    this.#receiveBuffer = next;
    this.#receiveReadOffset = 0;
    this.#receiveWriteOffset = remaining;
  }

  #compactReceiveBuffer(): void {
    if (this.#receiveReadOffset === 0) {
      return;
    }
    this.#ensureReceiveCapacity(0);
  }

  #processData(rawData: Buffer): void {
    if (this.#authHandlers && !this.#authExpectResponse) {
      this.#authHandlers.onSuccess(rawData);
      return;
    }

    this.#ensureReceiveCapacity(rawData.length);
    rawData.copy(this.#receiveBuffer, this.#receiveWriteOffset);
    this.#receiveWriteOffset += rawData.length;

    while (this.#receiveWriteOffset - this.#receiveReadOffset >= this.#bytesNeeded) {
      const expectedResponseSize = this.#receiveBuffer.readInt32BE(this.#receiveReadOffset);
      const frameSize = Decoder.int32Size() + expectedResponseSize;

      if (this.#receiveWriteOffset - this.#receiveReadOffset < frameSize) {
        this.#bytesNeeded = frameSize;
        this.#compactReceiveBuffer();
        return;
      }

      const responseStart = this.#receiveReadOffset + Decoder.int32Size();
      const responseEnd = this.#receiveReadOffset + frameSize;

      if (this.#authHandlers) {
        const rawResponseBuffer = this.#receiveBuffer.subarray(this.#receiveReadOffset, responseEnd);
        this.#receiveReadOffset = responseEnd;
        this.#bytesNeeded = Decoder.int32Size();
        this.#compactReceiveBuffer();
        this.#authHandlers.onSuccess(rawResponseBuffer);
        return;
      }

      const response = new Decoder(this.#receiveBuffer.subarray(responseStart, responseEnd));
      this.#receiveReadOffset = responseEnd;
      this.#bytesNeeded = Decoder.int32Size();

      const correlationId = response.readInt32();
      const inflight = this.requestQueue.inflight.get(correlationId);
      if (inflight && usesFlexibleResponseHeader(inflight.entry.apiKey, inflight.entry.apiVersion)) {
        response.readTaggedFields();
      }
      const payload = response.readAll();

      this.requestQueue.fulfillRequest({ size: expectedResponseSize, correlationId, payload });
    }

    this.#compactReceiveBuffer();
  }

  #rejectRequests(error: unknown): void {
    this.requestQueue.rejectAll(error);
  }
}
