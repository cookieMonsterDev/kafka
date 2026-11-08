import { getCollection } from 'astro:content';
import type { APIRoute, GetStaticPaths } from 'astro';
import { markdownFor } from '@/lib/publication';

export const getStaticPaths: GetStaticPaths = async () =>
  (await getCollection('docs')).map((entry) => ({ params: { slug: entry.id }, props: { entry } }));

export const GET: APIRoute = ({ props }) =>
  new Response(markdownFor(props.entry), { headers: { 'Content-Type': 'text/markdown; charset=utf-8' } });
