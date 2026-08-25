import { API_KEYS } from '../protocol/requests/api-keys';
import type { BrokerVersions } from '../protocol/requests/index';
import { Connection } from './connection';
import type { ConnectionOptions, SendOptions } from './connection';
import {
  DEFAULT_RECONNECT_BACKOFF_MAX_MS,
  DEFAULT_RECONNECT_BACKOFF_MS,
  DEFAULT_SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS,
  exponentialBackoffMs,
} from './defaults';

const POOL_SIZE = 2;
/** `Fetch` gets its own dedicated connection so a long poll never head-of-line blocks metadata/heartbeats/produce. */
const FETCH_CONNECTION_INDEX = 1;

/**
 * A small fixed-size pool of `Connection`s to one broker. Every non-`Fetch` request shares
 * connection 0; `Fetch` always uses connection 1, so a long-poll fetch never blocks other
 * requests to the same broker on the same socket.
 *
 * Mirrors the constructor options as public fields (`host`/`port`/`rack`/`sasl`/`connectionTimeout`)
 * because the cluster/broker layer above reads (and, for `rack`, writes back) them directly - e.g.
 * comparing a cached broker's address against fresh metadata, or recording a broker's rack once
 * metadata reveals it.
 */
export class ConnectionPool {
  readonly host: string;
  readonly port: number;
  rack: string | null;
  readonly sasl: ConnectionOptions['sasl'];
  readonly connectionTimeout: number;
  readonly clientId: string;
  readonly pool: readonly Connection[];
  readonly #reconnectBackoffMs: number;
  readonly #reconnectBackoffMaxMs: number;
  readonly #socketConnectionSetupTimeoutMs: number;
  readonly #socketConnectionSetupTimeoutMaxMs: number;
  #reconnectFailures = 0;

  constructor(options: ConnectionOptions) {
    this.host = options.host;
    this.port = options.port;
    this.rack = options.rack ?? null;
    this.sasl = options.sasl ?? null;
    this.connectionTimeout = options.connectionTimeout;
    this.clientId = options.clientId ?? 'kafka';
    this.#socketConnectionSetupTimeoutMs = options.connectionTimeout;
    this.#socketConnectionSetupTimeoutMaxMs =
      options.socketConnectionSetupTimeoutMaxMs ?? DEFAULT_SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS;
    this.#reconnectBackoffMs = options.reconnectBackoffMs ?? DEFAULT_RECONNECT_BACKOFF_MS;
    this.#reconnectBackoffMaxMs = options.reconnectBackoffMaxMs ?? DEFAULT_RECONNECT_BACKOFF_MAX_MS;
    this.pool = Array.from({ length: POOL_SIZE }, () => new Connection(options));
  }

  isConnected(): boolean {
    return this.pool.some((c) => c.isConnected());
  }

  isAuthenticated(): boolean {
    return this.pool.some((c) => c.isAuthenticated());
  }

  setSupportAuthenticationProtocol(isSupported: boolean | null): void {
    this.#map((c) => c.setSupportAuthenticationProtocol(isSupported));
  }

  setVersions(versions: BrokerVersions | null): void {
    this.#map((c) => c.setVersions(versions));
  }

  #map<T>(callback: (connection: Connection) => T): T[] {
    return this.pool.map((c) => callback(c));
  }

  async send<T>(protocolRequest: SendOptions<T>): Promise<T | undefined> {
    const connection = await this.getConnectionByRequest(protocolRequest);
    return connection.send(protocolRequest);
  }

  getConnectionByRequest({ request: { apiKey } }: Pick<SendOptions<unknown>, 'request'>): Promise<Connection> {
    const index = apiKey === API_KEYS.Fetch ? FETCH_CONNECTION_INDEX : 0;
    return this.getConnection(index);
  }

  async getConnection(index = 0): Promise<Connection> {
    const connection = this.pool[index]!;

    if (!connection.isConnected()) {
      const delay = exponentialBackoffMs(
        this.#reconnectFailures,
        this.#reconnectBackoffMs,
        this.#reconnectBackoffMaxMs,
      );
      if (delay > 0) {
        await new Promise<void>((resolve) => {
          setTimeout(resolve, delay);
        });
      }

      const setupTimeout = Math.max(
        this.#socketConnectionSetupTimeoutMs,
        exponentialBackoffMs(
          this.#reconnectFailures,
          this.#socketConnectionSetupTimeoutMs,
          this.#socketConnectionSetupTimeoutMaxMs,
        ) || this.#socketConnectionSetupTimeoutMs,
      );

      try {
        await connection.connect(setupTimeout);
        this.#reconnectFailures = 0;
      } catch (error) {
        this.#reconnectFailures += 1;
        throw error;
      }
    }

    return connection;
  }

  async destroy(): Promise<void> {
    await Promise.all(this.#map((c) => c.disconnect()));
  }
}
