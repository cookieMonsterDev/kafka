import net from 'node:net';
import tls from 'node:tls';

const KEEP_ALIVE_DELAY = 60_000; // ms

export interface SocketFactoryArgs {
  host: string;
  port: number;
  ssl?: tls.ConnectionOptions | null;
  onConnect: () => void;
}

export type SocketFactory = (args: SocketFactoryArgs) => net.Socket;

/**
 * The default `SocketFactory`: a plain TCP connection, or TLS when `ssl` is set. `servername`
 * defaults to `host` (SNI) unless `host` is a bare IP, which TLS rejects as a servername.
 */
export function createDefaultSocketFactory(): SocketFactory {
  return ({ host, port, ssl, onConnect }) => {
    const socket = ssl
      ? tls.connect({ host, port, ...(net.isIP(host) ? {} : { servername: host }), ...ssl }, onConnect)
      : net.connect({ host, port }, onConnect);

    socket.setKeepAlive(true, KEEP_ALIVE_DELAY);
    socket.setNoDelay(true);
    return socket;
  };
}
