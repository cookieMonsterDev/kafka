import net from 'node:net';
import { afterEach, describe, expect, it } from 'vitest';
import { NoFreePortError, PortUnavailableError, resolvePort } from './port';

const HOST = '127.0.0.1';

function listenOn(port: number): Promise<net.Server> {
  return new Promise((resolve, reject) => {
    const server = net.createServer();
    server.once('error', reject);
    server.listen(port, HOST, () => resolve(server));
  });
}

function close(server: net.Server): Promise<void> {
  return new Promise((resolve) => server.close(() => resolve()));
}

describe('resolvePort', () => {
  const busyServers: net.Server[] = [];

  afterEach(async () => {
    await Promise.all(busyServers.splice(0).map(close));
  });

  it('returns the requested port when it is free', async () => {
    // A genuinely free ephemeral port, found by asking the OS for one and releasing it.
    const probe = await listenOn(0);
    const { port } = probe.address() as net.AddressInfo;
    await close(probe);

    await expect(resolvePort({ host: HOST, requestedPort: port })).resolves.toBe(port);
  });

  it('rejects with PortUnavailableError when the requested port is busy', async () => {
    const server = await listenOn(0);
    busyServers.push(server);
    const { port } = server.address() as net.AddressInfo;

    await expect(resolvePort({ host: HOST, requestedPort: port })).rejects.toBeInstanceOf(PortUnavailableError);
  });

  it('scans the range and returns the first free port', async () => {
    const range: [number, number] = [59_000, 59_010];
    const busy = await listenOn(range[0]);
    busyServers.push(busy);

    await expect(resolvePort({ host: HOST, range })).resolves.toBe(range[0] + 1);
  });

  it('throws NoFreePortError when every port in range is busy', async () => {
    const range: [number, number] = [59_020, 59_021];
    for (const port of [range[0], range[1]]) {
      busyServers.push(await listenOn(port));
    }

    await expect(resolvePort({ host: HOST, range })).rejects.toBeInstanceOf(NoFreePortError);
  });
});
