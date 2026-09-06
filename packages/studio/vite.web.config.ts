import { readdirSync, readFileSync, writeFileSync } from 'node:fs';
import { extname, join, resolve } from 'node:path';
import { brotliCompressSync, constants as zlibConstants, gzipSync } from 'node:zlib';
import { defineConfig, type Plugin } from 'vite';
import react from '@vitejs/plugin-react';
import tailwindcss from '@tailwindcss/vite';

const COMPRESSIBLE_EXTENSIONS = new Set(['.js', '.css', '.html', '.svg', '.json']);
// Below this, a `.br`/`.gz` sibling wouldn't shrink the response meaningfully — it would just add
// two extra files to ship and a doomed content-negotiation branch to every request for it.
const MIN_COMPRESSIBLE_BYTES = 1024;

function listFiles(dir: string): string[] {
  return readdirSync(dir, { withFileTypes: true }).flatMap((entry) => {
    const full = join(dir, entry.name);
    return entry.isDirectory() ? listFiles(full) : [full];
  });
}

/**
 * Writes a `.br` and a `.gz` sibling next to every compressible build output over the size
 * threshold. `src/server/static.ts` negotiates the request's `Accept-Encoding` and serves whichever
 * (if either) it accepts, falling back to the identity file otherwise — this plugin only produces
 * the files that negotiation picks from.
 */
function precompressAssets(): Plugin {
  let outDir = '';
  return {
    name: 'studio-precompress-assets',
    apply: 'build',
    configResolved(config) {
      outDir = resolve(config.root, config.build.outDir);
    },
    closeBundle() {
      for (const file of listFiles(outDir)) {
        if (!COMPRESSIBLE_EXTENSIONS.has(extname(file))) continue;
        const contents = readFileSync(file);
        if (contents.byteLength < MIN_COMPRESSIBLE_BYTES) continue;

        writeFileSync(
          `${file}.br`,
          brotliCompressSync(contents, {
            params: {
              [zlibConstants.BROTLI_PARAM_QUALITY]: zlibConstants.BROTLI_MAX_QUALITY,
              [zlibConstants.BROTLI_PARAM_SIZE_HINT]: contents.byteLength,
            },
          }),
        );
        writeFileSync(`${file}.gz`, gzipSync(contents, { level: zlibConstants.Z_BEST_COMPRESSION }));
      }
    },
  };
}

// Builds the browser SPA served by the studio server (see src/server/static.ts). Kept as a
// separate config from vite.config.ts because the server bundle targets Node and externalizes
// core/config, while this one targets browsers and bundles everything it needs.
export default defineConfig({
  root: 'src/web',
  plugins: [react({ compiler: true }), tailwindcss(), precompressAssets()],
  build: {
    outDir: '../../dist/web',
    emptyOutDir: true,
    sourcemap: true,
    target: 'es2022',
  },
});
