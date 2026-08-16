import { EventEmitter } from 'node:events';
import type { Socket } from 'node:net';
import { describe, expect, it, vi } from 'vitest';
import { createSocket } from './socket';

describe('network/createSocket', () => {
  it('calls the socketFactory with host/port/ssl/onConnect and wires up event listeners', () => {
    const fakeSocket = new EventEmitter() as unknown as Socket;
    const onConnect = vi.fn();
    const socketFactory = vi.fn().mockReturnValue(fakeSocket);
    const onData = vi.fn();
    const onEnd = vi.fn();
    const onError = vi.fn();
    const onTimeout = vi.fn();

    const socket = createSocket({
      socketFactory,
      host: 'localhost',
      port: 9092,
      ssl: null,
      onConnect,
      onData,
      onEnd,
      onError,
      onTimeout,
    });

    expect(socket).toBe(fakeSocket);
    expect(socketFactory).toHaveBeenCalledWith({ host: 'localhost', port: 9092, ssl: null, onConnect });

    const chunk = Buffer.from('hello');
    fakeSocket.emit('data', chunk);
    expect(onData).toHaveBeenCalledWith(chunk);

    fakeSocket.emit('end');
    expect(onEnd).toHaveBeenCalledOnce();

    const error = new Error('boom');
    fakeSocket.emit('error', error);
    expect(onError).toHaveBeenCalledWith(error);

    fakeSocket.emit('timeout');
    expect(onTimeout).toHaveBeenCalledOnce();
  });
});
