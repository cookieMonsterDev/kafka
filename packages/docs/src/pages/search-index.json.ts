import type { APIRoute } from 'astro';
import { getSearchIndex } from '@/lib/search-index';

export const GET: APIRoute = async () => Response.json(await getSearchIndex());
