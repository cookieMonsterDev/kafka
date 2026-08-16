import { EventEmitter } from 'node:events';
import net from 'node:net';
import tls from 'node:tls';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { createDefaultSocketFactory } from './socket-factory';

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
});
