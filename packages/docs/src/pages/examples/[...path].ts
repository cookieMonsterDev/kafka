import type { APIRoute, GetStaticPaths } from 'astro';

const files = import.meta.glob('../../../examples/**/*.{mts,yml,json}', {
  query: '?raw',
  import: 'default',
  eager: true,
}) as Record<string, string>;

export const getStaticPaths: GetStaticPaths = () =>
  Object.entries(files).map(([path, body]) => ({
    params: { path: path.replace('../../../examples/', '') },
    props: { body },
  }));

export const GET: APIRoute = ({ props }) =>
  new Response(props.body, { headers: { 'Content-Type': 'text/plain; charset=utf-8' } });
