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
