import type { APIRoute } from 'astro';
import { getCollection } from 'astro:content';
import { withBase } from '@/lib/base';
import { hrefFor } from '@/lib/docs';

// Hidden entries are excluded from the nav only; they are still real pages worth indexing.
export const GET: APIRoute = async ({ site }) => {
  if (site == null) {
    throw new Error('sitemap.xml needs `site` set in astro.config.mjs');
  }
  const entries = await getCollection('docs');
  const paths = [withBase('/'), ...entries.map(hrefFor).sort()];
  const urls = paths.map((path) => `  <url><loc>${new URL(path, site).href}</loc></url>`).join('\n');
  const body = `<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
${urls}
</urlset>
`;
  return new Response(body, { headers: { 'Content-Type': 'application/xml; charset=utf-8' } });
};
