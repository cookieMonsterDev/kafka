import { getCollection, render } from 'astro:content';
import { docsPackageOf, hrefFor, SECTION_LABELS, sortDocs } from '@/lib/docs';
import type { SearchDoc } from '@/lib/search';

export async function getSearchIndex(): Promise<SearchDoc[]> {
  const entries = sortDocs(await getCollection('docs'));
  return Promise.all(
    entries.map(async (entry) => {
      const { headings } = await render(entry);
      return {
        href: hrefFor(entry),
        package: docsPackageOf(entry.id),
        body: entry.body ?? '',
        title: entry.data.title,
        description: entry.data.description,
        section: entry.data.section,
        sectionLabel: SECTION_LABELS[entry.data.section],
        headings: headings
          .filter((heading) => heading.depth === 2 || heading.depth === 3)
          .map(({ text, slug }) => ({ text, slug })),
      };
    }),
  );
}
