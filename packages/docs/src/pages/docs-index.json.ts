import { getCollection, render } from 'astro:content';
import type { APIRoute } from 'astro';
import { hrefFor, docsPackageOf, sortDocs } from '@/lib/docs';
import { publication, absoluteUrl, markdownHref, publishedSkills } from '@/lib/publication';

export const GET: APIRoute = async () =>
  Response.json({
    schemaVersion: 1,
    ...publication,
    pages: await Promise.all(
      sortDocs(await getCollection('docs')).map(async (entry) => ({
        id: entry.id,
        package: docsPackageOf(entry.id),
        title: entry.data.title,
        description: entry.data.description,
        url: absoluteUrl(hrefFor(entry)),
        markdown: absoluteUrl(markdownHref(entry)),
        version: publication.packages[docsPackageOf(entry.id)],
        section: entry.data.section,
        headings: (await render(entry)).headings.map(({ text, slug, depth }) => ({ text, slug, depth })),
      })),
    ),
    skills: publishedSkills.map((skill) => ({ path: skill.path, url: absoluteUrl(skill.href) })),
  });
