import { EventEmitter } from 'node:events';
import net from 'node:net';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { KafkaJSConnectionClosedError, KafkaJSConnectionError } from '../errors.js';
import { createLogger, LOG_LEVELS } from '../loggers/index.js';
import { Decoder } from '../protocol/decoder.js';
import { Encoder } from '../protocol/encoder.js';
import { API_KEYS } from '../protocol/requests/api-keys.js';
import { Connection } from './connection.js';
import type { ConnectionOptions, CreateSaslAuthenticator } from './connection.js';
import { createDefaultSocketFactory } from './socket-factory.js';

const silentLogger = createLogger({ level: LOG_LEVELS.NOTHING, logCreator: () => () => {} });

interface DecodedRequest {
  apiKey: number;
  apiVersion: number;
  correlationId: number;
  clientId: string;
}

/** A minimal fake broker: decodes the standard Kafka request header, ignores the body. */
function startFakeBroker(
  onRequest: (request: DecodedRequest, socket: net.Socket) => void,
): Promise<{ server: net.Server; port: number }> {
  return new Promise((resolve) => {
    const server = net.createServer((socket) => {
      let buffered = Buffer.alloc(0);

      socket.on('data', (chunk: Buffer) => {
        buffered = Buffer.concat([buffered, chunk]);

        while (buffered.length >= 4) {
          const size = buffered.readInt32BE(0);
          if (buffered.length < 4 + size) break;

          const frame = buffered.subarray(4, 4 + size);
          buffered = buffered.subarray(4 + size);

          const decoder = new Decoder(frame);
          const apiKey = decoder.readInt16();
          const apiVersion = decoder.readInt16();
          const correlationId = decoder.readInt32();
          const clientId = decoder.readString() ?? '';

          onRequest({ apiKey, apiVersion, correlationId, clientId }, socket);
        }
      });
    });

    server.listen(0, '127.0.0.1', () => {
      const { port } = server.address() as net.AddressInfo;
      resolve({ server, port });
    });
  });
}

function writeResponseFrame(socket: net.Socket, correlationId: number, body: Encoder): void {
  const responseHeader = new Encoder().writeInt32(correlationId).writeEncoder(body);
  const framed = new Encoder().writeInt32(responseHeader.size()).writeEncoder(responseHeader);
  socket.write(framed.buffer);
}

const metadataRequest = () => ({
  apiKey: API_KEYS.Metadata,
  apiVersion: 0,
  apiName: 'Metadata',
  encode: () => Promise.resolve(new Encoder()),
});

describe('network/Connection', () => {
  let servers: net.Server[] = [];
  let connections: Connection[] = [];

  afterEach(async () => {
    await Promise.all(connections.map((c) => c.disconnect()));
    connections = [];
    await Promise.all(servers.map((s) => new Promise<void>((resolve) => s.close(() => resolve()))));
    servers = [];
  });

  const createConnection = (port: number, overrides: Partial<ConnectionOptions> = {}): Connection => {
    const connection = new Connection({
      host: '127.0.0.1',
      port,
      logger: silentLogger,
      socketFactory: createDefaultSocketFactory(),
      requestTimeout: 1000,
      connectionTimeout: 1000,
      ...overrides,
    });
    connections.push(connection);
    return connection;
  };

  describe('connect', () => {
    it('resolves true and marks the connection as connected', async () => {
      const { server, port } = await startFakeBroker(() => {});
      servers.push(server);

      const connection = createConnection(port);
      await expect(connection.connect()).resolves.toBe(true);
      expect(connection.isConnected()).toBe(true);
    });

    it('resolves immediately without reconnecting when already connected', async () => {
      const { server, port } = await startFakeBroker(() => {});
      servers.push(server);

      const connection = createConnection(port);
      await connection.connect();
      await expect(connection.connect()).resolves.toBe(true);
    });

    it('rejects with a connection error when the broker is unreachable', async () => {
      const temp = net.createServer();
      const port = await new Promise<number>((resolve) => {
        temp.listen(0, '127.0.0.1', () => resolve((temp.address() as net.AddressInfo).port));
      });
      await new Promise<void>((resolve) => temp.close(() => resolve()));

      const connection = createConnection(port);
      await expect(connection.connect()).rejects.toThrow(KafkaJSConnectionError);
      expect(connection.isConnected()).toBe(false);
    });

    it('rejects with a timeout error when the socket never connects in time', async () => {
      const socketFactory = () => {
        const socket = new EventEmitter() as unknown as net.Socket;
        socket.end = vi.fn().mockReturnThis();
        socket.unref = vi.fn().mockReturnThis();
        socket.write = vi.fn().mockReturnValue(true);
        return socket;
      };

      const connection = createConnection(9999, { socketFactory, connectionTimeout: 1 });
      await expect(connection.connect()).rejects.toThrow('Connection timeout');
      expect(connection.isConnected()).toBe(false);
    });
  });

  describe('disconnect', () => {
    it('flips the connection status back to disconnected', async () => {
      const { server, port } = await startFakeBroker(() => {});
      servers.push(server);

      const connection = createConnection(port);
      await connection.connect();
      await expect(connection.disconnect()).resolves.toBe(true);
      expect(connection.isConnected()).toBe(false);
    });
  });

  describe('send', () => {
    it('writes a framed request and resolves with the parsed response', async () => {
      const { server, port } = await startFakeBroker((request, socket) => {
        writeResponseFrame(socket, request.correlationId, new Encoder().writeString('hello'));
      });
      servers.push(server);

      const connection = createConnection(port);
      await connection.connect();

      const response = {
        decode: async (rawData: Buffer) => ({ greeting: new Decoder(rawData).readString() }),
        parse: async (data: { greeting: string | null }) => data,
      };

      await expect(connection.send({ request: metadataRequest(), response })).resolves.toEqual({ greeting: 'hello' });
    });

    it('reassembles a response split across multiple TCP chunks', async () => {
      const { server, port } = await startFakeBroker((request, socket) => {
        const body = new Encoder().writeString('x'.repeat(5000));
        const responseHeader = new Encoder().writeInt32(request.correlationId).writeEncoder(body);
        const framed = new Encoder().writeInt32(responseHeader.size()).writeEncoder(responseHeader);

        const full = framed.buffer;
        let offset = 0;
        const sendChunk = (): void => {
          if (offset >= full.length) return;
          const end = Math.min(offset + 97, full.length);
          socket.write(full.subarray(offset, end));
          offset = end;
          setImmediate(sendChunk);
        };
        sendChunk();
      });
      servers.push(server);

      const connection = createConnection(port);
      await connection.connect();

      const response = {
        decode: async (rawData: Buffer) => ({ greeting: new Decoder(rawData).readString() }),
        parse: async (data: { greeting: string | null }) => data,
      };

      const result = await connection.send({ request: metadataRequest(), response });
      expect(result?.greeting).toHaveLength(5000);
    });

    it('rejects in-flight requests when the broker closes the connection', async () => {
      const { server, port } = await startFakeBroker((_request, socket) => {
        socket.end();
      });
      servers.push(server);

      const connection = createConnection(port);
      await connection.connect();

      const response = {
        decode: async (rawData: Buffer) => new Decoder(rawData),
        parse: async (data: Decoder) => data,
      };

      await expect(connection.send({ request: metadataRequest(), response })).rejects.toThrow(
        KafkaJSConnectionClosedError,
      );
    });

    it('applies KIP-219 client-side throttling from the decoded response', async () => {
      const { server, port } = await startFakeBroker((request, socket) => {
        writeResponseFrame(socket, request.correlationId, new Encoder());
      });
      servers.push(server);

      const connection = createConnection(port);
      await connection.connect();

      const response = {
        decode: async () => ({ clientSideThrottleTime: 5000 }),
        parse: async (data: { clientSideThrottleTime: number }) => data,
      };

      const before = Date.now();
      await connection.send({ request: metadataRequest(), response });
      expect(connection.requestQueue.throttledUntil).toBeGreaterThanOrEqual(before + 5000);
    });
  });

  describe('SASL authentication', () => {
    const noThrottleResponse = { decode: async () => ({}), parse: async (data: object) => data };

    it('authenticates lazily on the first non-bootstrap request', async () => {
      const { server, port } = await startFakeBroker((request, socket) => {
        writeResponseFrame(socket, request.correlationId, new Encoder());
      });
      servers.push(server);

      const authenticate = vi.fn().mockResolvedValue(undefined);
      const createSaslAuthenticator: CreateSaslAuthenticator = () => ({ sessionLifetime: 0n, authenticate });

      const connection = createConnection(port, { sasl: { mechanism: 'plain' }, createSaslAuthenticator });
      await connection.connect();
      expect(authenticate).not.toHaveBeenCalled();

      await connection.send({ request: metadataRequest(), response: noThrottleResponse });
      expect(authenticate).toHaveBeenCalledOnce();
    });

    it('does not authenticate before ApiVersions/SaslHandshake/SaslAuthenticate', async () => {
      const { server, port } = await startFakeBroker((request, socket) => {
        writeResponseFrame(socket, request.correlationId, new Encoder());
      });
      servers.push(server);

      const authenticate = vi.fn().mockResolvedValue(undefined);
      const createSaslAuthenticator: CreateSaslAuthenticator = () => ({ sessionLifetime: 0n, authenticate });

      const connection = createConnection(port, { sasl: { mechanism: 'plain' }, createSaslAuthenticator });
      await connection.connect();

      const request = {
        apiKey: API_KEYS.ApiVersions,
        apiVersion: 0,
        apiName: 'ApiVersions',
        encode: () => Promise.resolve(new Encoder()),
      };
      await connection.send({ request, response: noThrottleResponse });

      expect(authenticate).not.toHaveBeenCalled();
    });

    it('throws when sasl is configured but no authenticator factory was provided', async () => {
      const { server, port } = await startFakeBroker(() => {});
      servers.push(server);

      const connection = createConnection(port, { sasl: { mechanism: 'plain' } });
      await connection.connect();

      await expect(connection.send({ request: metadataRequest(), response: noThrottleResponse })).rejects.toThrow(
        'SASL is configured but no SASL authenticator was provided',
      );
    });
  });
});
