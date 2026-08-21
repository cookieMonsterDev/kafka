// @ts-check
import { defineConfig } from 'astro/config';

import react from '@astrojs/react';
import tailwindcss from '@tailwindcss/vite';

const githubPages = process.env.GITHUB_PAGES === '1';

// https://docs.astro.build/en/reference/configuration-reference/
export default defineConfig({
  site: 'https://cookiemonsterdev.github.io',
  base: githubPages ? '/kafka' : '/',

  redirects: {
    '/docs': '/docs/core/start/introduction',
    '/docs/core': '/docs/core/start/introduction',
    '/docs/introduction': '/docs/core/start/introduction',
    '/docs/getting-started': '/docs/core/start/getting-started',
    '/docs/compatibility': '/docs/core/reference/compatibility',
    '/docs/public-api': '/docs/core/reference/public-api',
    '/docs/migration': '/docs/core/migration/breaking-changes',
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
