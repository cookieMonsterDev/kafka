import net from 'node:net';

/** Prisma Studio scans `5555-5600`; ours is shifted to avoid colliding with it on a shared box. */
export const DEFAULT_PORT_RANGE: readonly [number, number] = [5757, 5807];

export class PortUnavailableError extends Error {
  readonly port: number;

  constructor(port: number) {
    super(`port ${String(port)} is already in use`);
    this.port = port;
  }
}

export class NoFreePortError extends Error {
  readonly range: readonly [number, number];

  constructor(range: readonly [number, number]) {
    super(`no free port available in range ${String(range[0])}-${String(range[1])}`);
    this.range = range;
  }
}

function isPortFree(port: number, host: string): Promise<boolean> {
  return new Promise((resolve) => {
    const tester = net.createServer();
    tester.once('error', () => resolve(false));
    tester.once('listening', () => {
      tester.close(() => resolve(true));
    });
    tester.listen(port, host);
  });
}

export interface ResolvePortOptions {
  readonly host: string;
  /** An explicit port the caller asked for. When given, no scanning happens — it's this port or nothing. */
  readonly requestedPort?: number;
  readonly range?: readonly [number, number];
}

/**
 * Resolves the port the server should bind to: the caller's explicit choice if given (checked,
 * not silently swapped for another one), otherwise the first free port in {@link range}.
 *
 * Checking and binding are two separate steps, so a port reported free here can be taken by
 * something else microseconds later — the same inherent race the `get-port` package has.
 * Acceptable for a single local dev server; callers that actually bind should
 * still handle `EADDRINUSE` on `listen()` rather than trust this result unconditionally.
 */
export async function resolvePort(options: ResolvePortOptions): Promise<number> {
  const { host, requestedPort, range = DEFAULT_PORT_RANGE } = options;

  if (requestedPort !== undefined) {
    if (!(await isPortFree(requestedPort, host))) throw new PortUnavailableError(requestedPort);
    return requestedPort;
  }

  for (let port = range[0]; port <= range[1]; port++) {
    if (await isPortFree(port, host)) return port;
  }
  throw new NoFreePortError(range);
}
