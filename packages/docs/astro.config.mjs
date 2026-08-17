// @ts-check
import { defineConfig } from 'astro/config';

import react from '@astrojs/react';
import tailwindcss from '@tailwindcss/vite';

// https://docs.astro.build/en/reference/configuration-reference/
export default defineConfig({
  site: 'https://example.com',

  redirects: {
    '/docs/introduction': '/docs/start/introduction',
    '/docs/getting-started': '/docs/start/getting-started',
    '/docs/compatibility': '/docs/reference/compatibility',
    '/docs/public-api': '/docs/reference/public-api',
    '/docs/migration': '/docs/migration/breaking-changes',
  },

  markdown: {
    shikiConfig: {
      themes: { light: 'github-light', dark: 'github-dark' },
    },
  },

  integrations: [react()],

  vite: {
    plugins: [tailwindcss()],
  },
});
