// @ts-check
import { defineConfig } from 'astro/config';

import react from '@astrojs/react';
import tailwindcss from '@tailwindcss/vite';
import { chartLanguage, markdownCharts } from './src/lib/rehype-chart.ts';

const githubPages = process.env.GITHUB_PAGES === '1';

// https://docs.astro.build/en/reference/configuration-reference/
export default defineConfig({
  site: 'https://cookiemonsterdev.github.io',
  base: githubPages ? '/kafka' : '/',

  redirects: Object.fromEntries(
    Object.entries({
      '/docs': '/docs/core/start/introduction',
      '/docs/core': '/docs/core/start/introduction',
      '/docs/introduction': '/docs/core/start/introduction',
      '/docs/getting-started': '/docs/core/start/getting-started',
      '/docs/compatibility': '/docs/core/reference/compatibility',
      '/docs/public-api': '/docs/core/reference/public-api',
      '/docs/migration': '/docs/core/migration/breaking-changes',
    }).map(([from, to]) => [from, `${githubPages ? '/kafka' : ''}${to}`]),
  ),

  // Match the dark-only docs palette.
  markdown: {
    shikiConfig: {
      theme: 'github-dark',
    },
    // Chart blocks hold JSON for markdownCharts(), not code to highlight.
    syntaxHighlight: { type: 'shiki', excludeLangs: [chartLanguage] },
  },

  integrations: [react(), markdownCharts()],

  vite: {
    plugins: [tailwindcss()],
  },
});
