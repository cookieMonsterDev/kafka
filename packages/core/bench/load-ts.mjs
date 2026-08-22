import { createServer } from 'vite';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const pkgRoot = resolve(dirname(fileURLToPath(import.meta.url)), '..');

const server = await createServer({
  configFile: false,
  root: pkgRoot,
  server: { middlewareMode: true, hmr: false },
  appType: 'custom',
  optimizeDeps: { noDiscovery: true },
});

try {
  const mod = await server.ssrLoadModule(resolve(pkgRoot, 'bench/run.ts'));
  await mod.main();
} finally {
  await server.close();
}
