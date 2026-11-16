import net, { type AddressInfo } from 'node:net';

/**
 * Asks the OS for a port that is free on `host`, then releases it, so a test can pass it as an
 * explicit port. The port can be taken again before the caller binds it, the same check-then-bind
 * race as `resolvePort`, but far less often than a hard-coded high port, which other test files
 * or outgoing connections (the OS's ephemeral range) may already hold.
 */
export function findFreePort(host = '127.0.0.1'): Promise<number> {
  return new Promise((resolve, reject) => {
    const server = net.createServer();
    server.once('error', reject);
    server.listen(0, host, () => {
      const { port } = server.address() as AddressInfo;
      server.close(() => resolve(port));
    });
  });
}
