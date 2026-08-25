import type { Socket } from 'node:net';
import type { SocketFactory, SocketFactoryArgs } from './socket-factory';

export interface CreateSocketOptions {
  socketFactory: SocketFactory;
  host: string;
  port: number;
  ssl: SocketFactoryArgs['ssl'];
  onConnect: () => void;
  onData: (data: Buffer) => void;
  onEnd: () => void;
  onError: (err: Error) => void;
  onTimeout: () => void;
  servername?: string;
}

/** Builds the socket via `socketFactory` and wires up the event handlers `Connection` needs. */
export function createSocket({
  socketFactory,
  host,
  port,
  ssl,
  onConnect,
  onData,
  onEnd,
  onError,
  onTimeout,
  servername,
}: CreateSocketOptions): Socket {
  const socket = socketFactory({ host, port, ssl, onConnect, servername });

  socket.on('data', onData);
  socket.on('end', onEnd);
  socket.on('error', onError);
  socket.on('timeout', onTimeout);

  return socket;
}
