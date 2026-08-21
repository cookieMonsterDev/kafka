import { defineCollection } from 'astro:content';
import { glob } from 'astro/loaders';
import { z } from 'astro/zod';

// Nested folders under src/content/docs become URL segments:
// core/start/introduction.md → /docs/core/start/introduction/
const docs = defineCollection({
  loader: glob({ pattern: '**/*.md', base: './src/content/docs' }),
  schema: z.object({
    title: z.string(),
    description: z.string(),
    order: z.number().default(999),
    section: z.enum(['start', 'guides', 'reference', 'integrations', 'migration']),
    sidebarLabel: z.string().optional(),
  }),
});

export const collections = { docs };
