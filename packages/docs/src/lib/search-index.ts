import { getCollection } from 'astro:content';
import { hrefFor, SECTION_LABELS, sortDocs } from '@/lib/docs';
import { extractHeadings, type SearchDoc } from '@/lib/search';

export async function getSearchIndex(): Promise<SearchDoc[]> {
  const entries = sortDocs(await getCollection('docs'));
  return entries.map((entry) => ({
    href: hrefFor(entry),
    title: entry.data.title,
    description: entry.data.description,
    section: entry.data.section,
    sectionLabel: SECTION_LABELS[entry.data.section],
    headings: extractHeadings(entry.body ?? ''),
  }));
}
