import { API_KEYS } from '../protocol/requests/api-keys.js';
import type { BrokerVersions } from '../protocol/requests/index.js';
import { Connection } from './connection.js';
import type { ConnectionOptions, SendOptions } from './connection.js';

const POOL_SIZE = 2;
/** `Fetch` gets its own dedicated connection so a long poll never head-of-line blocks metadata/heartbeats/produce. */
const FETCH_CONNECTION_INDEX = 1;

/**
 * A small fixed-size pool of `Connection`s to one broker. Every non-`Fetch` request shares
 * connection 0; `Fetch` always uses connection 1, so a long-poll fetch never blocks other
 * requests to the same broker on the same socket.
 */
export class ConnectionPool {
  readonly pool: readonly Connection[];

  constructor(options: ConnectionOptions) {
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
      await connection.connect();
    }

    return connection;
  }

  async destroy(): Promise<void> {
    await Promise.all(this.#map((c) => c.disconnect()));
  }
}
