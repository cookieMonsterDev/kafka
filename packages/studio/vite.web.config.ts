import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';
import tailwindcss from '@tailwindcss/vite';

// Builds the browser SPA served by the studio server (see src/server/static.ts). Kept as a
// separate config from vite.config.ts because the server bundle targets Node and externalizes
// core/config, while this one targets browsers and bundles everything it needs.
export default defineConfig({
  root: 'src/web',
  plugins: [react(), tailwindcss()],
  build: {
    outDir: '../../dist/web',
    emptyOutDir: true,
    sourcemap: true,
    target: 'es2022',
  },
});
