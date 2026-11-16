import { EventEmitter } from 'node:events';
import net from 'node:net';
import tls from 'node:tls';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { isBun } from '../runtime/runtime';
import { createDefaultSocketFactory } from './socket-factory';
import type { SocketFactoryArgs } from './socket-factory';

/** TLS without certificates: both sides share a pre-shared key. */
const PSK_TLS_OPTIONS = { ciphers: 'PSK-AES128-GCM-SHA256', maxVersion: 'TLSv1.2' } as const;
const PSK = Buffer.from('0123456789abcdef');

describe('network/createDefaultSocketFactory', () => {
  let server: net.Server | undefined;

  afterEach(async () => {
    if (server) {
      await new Promise<void>((resolve) => server!.close(() => resolve()));
      server = undefined;
    }
  });

  it('connects a plain TCP socket and invokes onConnect', async () => {
    server = net.createServer((socket) => socket.end());
    await new Promise<void>((resolve) => server!.listen(0, '127.0.0.1', resolve));
    const { port } = server.address() as net.AddressInfo;

    const factory = createDefaultSocketFactory();
    const connected = await new Promise<boolean>((resolve) => {
      const socket = factory({ host: '127.0.0.1', port, onConnect: () => resolve(true) });
      socket.on('error', () => resolve(false));
    });

    expect(connected).toBe(true);
  });

  it('enables keep-alive with a 60s delay', async () => {
    server = net.createServer();
    await new Promise<void>((resolve) => server!.listen(0, '127.0.0.1', resolve));
    const { port } = server.address() as net.AddressInfo;

    const setKeepAliveSpy = vi.spyOn(net.Socket.prototype, 'setKeepAlive');
    const setNoDelaySpy = vi.spyOn(net.Socket.prototype, 'setNoDelay');
    const factory = createDefaultSocketFactory();
    const socket = await new Promise<net.Socket>((resolve) => {
      const created = factory({ host: '127.0.0.1', port, onConnect: () => resolve(created) });
    });

    expect(setKeepAliveSpy).toHaveBeenCalledWith(true, 60_000);
    expect(setNoDelaySpy).toHaveBeenCalledWith(true);
    setKeepAliveSpy.mockRestore();
    setNoDelaySpy.mockRestore();
    socket.destroy();
  });

  it('adds servername (SNI) for non-IP hosts over TLS, but not for IP hosts', () => {
    const fakeSocket = () => {
      const socket = new EventEmitter() as unknown as net.Socket;
      socket.setKeepAlive = vi.fn().mockReturnThis();
      socket.setNoDelay = vi.fn().mockReturnThis();
      return socket;
    };

    const connectSpy = vi.spyOn(tls, 'connect').mockImplementation((() => fakeSocket()) as never);

    const factory = createDefaultSocketFactory();
    factory({ host: 'broker.example.com', port: 9093, ssl: {}, onConnect: () => {} });
    factory({ host: '127.0.0.1', port: 9093, ssl: {}, onConnect: () => {} });

    expect(connectSpy.mock.calls[0]?.[0]).toMatchObject({
      host: 'broker.example.com',
      port: 9093,
      servername: 'broker.example.com',
    });
    expect(connectSpy.mock.calls[1]?.[0]).not.toHaveProperty('servername');

    connectSpy.mockRestore();
  });

  it('uses an explicit servername for SNI when connecting to an IP over TLS', () => {
    const fakeSocket = () => {
      const socket = new EventEmitter() as unknown as net.Socket;
      socket.setKeepAlive = vi.fn().mockReturnThis();
      socket.setNoDelay = vi.fn().mockReturnThis();
      return socket;
    };

    const connectSpy = vi.spyOn(tls, 'connect').mockImplementation((() => fakeSocket()) as never);
    const factory = createDefaultSocketFactory();
    factory({
      host: '10.0.0.1',
      port: 9093,
      ssl: {},
      servername: 'broker.example.com',
      onConnect: () => {},
    });

    expect(connectSpy.mock.calls[0]?.[0]).toMatchObject({
      host: '10.0.0.1',
      port: 9093,
      servername: 'broker.example.com',
    });
    connectSpy.mockRestore();
  });

  // Bun ignores `onread` and emits 'data'; its TLS also lacks the PSK cipher used here.
  describe.skipIf(isBun())('onread', () => {
    const payload = Buffer.from(Array.from({ length: 3_000 }, (_, i) => i % 251));

    /** Connects with `onread` reading into a 1000-byte buffer, and collects everything it receives. */
    const receiveAll = (args: Omit<SocketFactoryArgs, 'onConnect' | 'onread'>) =>
      new Promise<{ received: Buffer; maxRead: number; dataEvents: number }>((resolve, reject) => {
        const readBuffer = Buffer.alloc(1_000);
        const reads: Buffer[] = [];
        let dataEvents = 0;
        const socket = createDefaultSocketFactory()({
          ...args,
          onConnect: () => {},
          onread: {
            buffer: () => readBuffer,
            callback: (bytesRead, target) => {
              reads.push(Buffer.from(target.subarray(0, bytesRead)));
              return true;
            },
          },
        });
        socket.on('data', () => (dataEvents += 1));
        socket.on('error', reject);
        socket.on('end', () => {
          socket.destroy();
          resolve({
            received: Buffer.concat(reads),
            maxRead: Math.max(...reads.map((read) => read.length)),
            dataEvents,
          });
        });
      });

    it('passes onread to a plain TCP socket', async () => {
      server = net.createServer((socket) => socket.end(payload));
      await new Promise<void>((resolve) => server!.listen(0, '127.0.0.1', resolve));
      const { port } = server.address() as net.AddressInfo;

      const { received, maxRead, dataEvents } = await receiveAll({ host: '127.0.0.1', port });

      expect(received.equals(payload)).toBe(true);
      expect(maxRead).toBeLessThanOrEqual(1_000);
      expect(dataEvents).toBe(0);
    });

    it('passes onread to a TLS socket, which reads decrypted bytes into it', async () => {
      server = tls.createServer({ ...PSK_TLS_OPTIONS, pskCallback: () => PSK }, (socket) => socket.end(payload));
      await new Promise<void>((resolve) => server!.listen(0, '127.0.0.1', resolve));
      const { port } = server.address() as net.AddressInfo;

      const { received, maxRead, dataEvents } = await receiveAll({
        host: '127.0.0.1',
        port,
        // A PSK handshake has no certificate to check the host against.
        ssl: {
          ...PSK_TLS_OPTIONS,
          pskCallback: () => ({ psk: PSK, identity: 'client' }),
          checkServerIdentity: () => undefined,
        },
      });

      expect(received.equals(payload)).toBe(true);
      expect(maxRead).toBeLessThanOrEqual(1_000);
      expect(dataEvents).toBe(0);
    });
  });
});
