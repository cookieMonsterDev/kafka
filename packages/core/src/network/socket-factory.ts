import net from 'node:net';
import type { ConnectionOptions as TlsConnectionOptions } from 'node:tls';
import { HAPPY_EYEBALLS_DELAY_MS } from './dns-lookup';

const KEEP_ALIVE_DELAY = 60_000; // ms

export interface SocketFactoryArgs {
  host: string;
  port: number;
  ssl?: TlsConnectionOptions | null;
  onConnect: () => void;
  /**
   * TLS SNI when `host` is an IP (the original broker hostname). Ignored for plain TCP.
   * When `host` is a hostname, SNI defaults to `host` if this is omitted.
   */
  servername?: string;
  /**
   * Node's `onread` option: the socket reads into the buffers it supplies instead of emitting
   * `'data'`. The default factory passes it to `net.connect` and `tls.connect`. A factory may ignore
   * it, because the client still handles `'data'`. A factory that uses it must keep Node's contract:
   * call `buffer()` for each read's target, and pass `callback` that same buffer.
   */
  onread?: net.OnReadOpts;
}

/**
 * Creates the socket for one broker connection. The client reuses a written request buffer once
 * that write's callback fires, so a custom socket must call it only after the chunk has been
 * consumed, as `net.Socket` and `tls.TLSSocket` do.
 */
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
 * did not already resolve addresses itself. `onread` is applied after `ssl`, so TLS options can't
 * replace the client's reader.
 */
export function createDefaultSocketFactory(): SocketFactory {
  return ({ host, port, ssl, onConnect, servername, onread }) => {
    const readOptions = onread ? { onread } : {};
    // `node:tls` loads on the first TLS connect, so importing the client does not load it.
    const socket = ssl
      ? process
          .getBuiltinModule('node:tls')
          .connect({ host, port, ...tlsServername(host, servername), ...ssl, ...readOptions }, onConnect)
      : net.connect(
          net.isIP(host)
            ? { host, port, ...readOptions }
            : {
                host,
                port,
                autoSelectFamily: true,
                autoSelectFamilyAttemptTimeout: HAPPY_EYEBALLS_DELAY_MS,
                ...readOptions,
              },
          onConnect,
        );

    socket.setKeepAlive(true, KEEP_ALIVE_DELAY);
    socket.setNoDelay(true);
    return socket;
  };
}
