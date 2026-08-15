// @ts-check
import { defineConfig } from 'astro/config'

// https://docs.astro.build/en/reference/configuration-reference/
export default defineConfig({
  site: 'https://example.com',
  markdown: {
    shikiConfig: {
      themes: { light: 'github-light', dark: 'github-dark' },
    },
  },
})
