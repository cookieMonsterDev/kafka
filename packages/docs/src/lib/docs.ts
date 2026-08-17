import type { CollectionEntry } from 'astro:content';

export const SECTION_ORDER = ['start', 'guides', 'reference', 'integrations', 'migration'] as const;

export type DocsSection = (typeof SECTION_ORDER)[number];

export const SECTION_LABELS: Record<DocsSection, string> = {
  start: 'Start',
  guides: 'Guides',
  reference: 'Reference',
  integrations: 'Integrations',
  migration: 'Migration',
};

export type DocsEntry = CollectionEntry<'docs'>;

export function hrefFor(entry: DocsEntry): string {
  return `/docs/${entry.id}/`;
}

export function navLabel(entry: DocsEntry): string {
  return entry.data.sidebarLabel ?? entry.data.title;
}

export function sortDocs(entries: DocsEntry[]): DocsEntry[] {
  const sectionIndex = new Map(SECTION_ORDER.map((section, index) => [section, index]));
  return [...entries].sort((a, b) => {
    const bySection = (sectionIndex.get(a.data.section) ?? 99) - (sectionIndex.get(b.data.section) ?? 99);
    if (bySection !== 0) {
      return bySection;
    }
    if (a.data.order !== b.data.order) {
      return a.data.order - b.data.order;
    }
    return a.data.title.localeCompare(b.data.title);
  });
}

export function groupDocs(entries: DocsEntry[]) {
  const sorted = sortDocs(entries);
  return SECTION_ORDER.map((section) => ({
    section,
    label: SECTION_LABELS[section],
    entries: sorted.filter((entry) => entry.data.section === section),
  })).filter((group) => group.entries.length > 0);
}

export function neighbors(entries: DocsEntry[], id: string) {
  const sorted = sortDocs(entries);
  const index = sorted.findIndex((entry) => entry.id === id);
  return {
    prev: index > 0 ? sorted[index - 1] : undefined,
    next: index >= 0 && index < sorted.length - 1 ? sorted[index + 1] : undefined,
  };
}

export function isCurrentPath(href: string, pathname: string): boolean {
  const normalize = (path: string) => path.replace(/\/+$/, '') || '/';
  return normalize(pathname) === normalize(href);
}
