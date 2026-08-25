import net from 'node:net';
import tls from 'node:tls';
import { HAPPY_EYEBALLS_DELAY_MS } from './dns-lookup';

const KEEP_ALIVE_DELAY = 60_000; // ms

export interface SocketFactoryArgs {
  host: string;
  port: number;
  ssl?: tls.ConnectionOptions | null;
  onConnect: () => void;
  /**
   * TLS SNI when `host` is an IP (the original broker hostname). Ignored for plain TCP.
   * When `host` is a hostname, SNI defaults to `host` if this is omitted.
   */
  servername?: string;
}

export type SocketFactory = (args: SocketFactoryArgs) => net.Socket;

function tlsServername(host: string, servername: string | undefined): { servername: string } | Record<string, never> {
  const name = servername ?? host;
  if (net.isIP(name)) return {};
  return { servername: name };
}

/**
 * The default `SocketFactory`: a plain TCP connection, or TLS when `ssl` is set. `servername`
 * defaults to `host` (SNI) unless that value is a bare IP, which TLS rejects as a servername.
 * Hostname connects enable Node's RFC 8305 family autodetection as a backstop when the client
 * did not already resolve addresses itself.
 */
export function createDefaultSocketFactory(): SocketFactory {
  return ({ host, port, ssl, onConnect, servername }) => {
    const socket = ssl
      ? tls.connect({ host, port, ...tlsServername(host, servername), ...ssl }, onConnect)
      : net.connect(
          net.isIP(host)
            ? { host, port }
            : { host, port, autoSelectFamily: true, autoSelectFamilyAttemptTimeout: HAPPY_EYEBALLS_DELAY_MS },
          onConnect,
        );

    socket.setKeepAlive(true, KEEP_ALIVE_DELAY);
    socket.setNoDelay(true);
    return socket;
  };
}
