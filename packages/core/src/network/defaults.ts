/** Close idle sockets after this many ms (broker `connections.max.idle.ms`). `0` disables. */
export const DEFAULT_CONNECTIONS_MAX_IDLE_MS = 540_000;

/**
 * Cap for exponential growth of the connect/TLS handshake timeout after consecutive failures
 * (broker `socket.connection.setup.timeout.max.ms`).
 */
export const DEFAULT_SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS = 30_000;

/** Initial wait before reconnecting a dropped socket (broker `reconnect.backoff.ms`). */
export const DEFAULT_RECONNECT_BACKOFF_MS = 50;

/** Cap for reconnect backoff (broker `reconnect.backoff.max.ms`). */
export const DEFAULT_RECONNECT_BACKOFF_MAX_MS = 1_000;

export function exponentialBackoffMs(failures: number, initialMs: number, maxMs: number): number {
  if (failures <= 0 || initialMs <= 0) return 0;
  const exp = initialMs * 2 ** (failures - 1);
  return Math.min(maxMs, exp);
}
