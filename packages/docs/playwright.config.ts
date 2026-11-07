import { defineConfig } from '@playwright/test';

export default defineConfig({
  testDir: './test/browser',
  fullyParallel: true,
  forbidOnly: Boolean(process.env.CI),
  retries: process.env.CI ? 1 : 0,
  reporter: 'list',
  use: { baseURL: 'http://127.0.0.1:4329/kafka/', trace: 'retain-on-failure' },
  webServer: {
    command: 'pnpm exec astro preview --host 127.0.0.1 --port 4329 --ignore-lock',
    env: { GITHUB_PAGES: '1' },
    url: 'http://127.0.0.1:4329/kafka/',
    reuseExistingServer: false,
  },
});
