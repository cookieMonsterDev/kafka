import type { APIRoute, GetStaticPaths } from 'astro';
import { publishedSkills, skillMarkdown } from '@/lib/publication';

export const getStaticPaths: GetStaticPaths = () =>
  publishedSkills.map((skill) => ({ params: { path: skill.path }, props: { skill } }));

export const GET: APIRoute = ({ props }) =>
  new Response(skillMarkdown(props.skill), { headers: { 'Content-Type': 'text/markdown; charset=utf-8' } });
