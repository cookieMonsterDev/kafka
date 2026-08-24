import type { CollectionEntry } from 'astro:content';
import { withBase } from '@/lib/base';

export const DOCS_PACKAGES = ['core'] as const;

export type DocsPackage = (typeof DOCS_PACKAGES)[number];

export const DEFAULT_DOCS_PACKAGE: DocsPackage = 'core';

/** Display names for the sidebar package switcher. Add a row when a package grows a docs tree. */
export const DOCS_PACKAGE_META: Record<DocsPackage, { label: string; blurb: string }> = {
  core: {
    label: 'Core',
    blurb: 'Node.js client',
  },
};

export function isDocsPackage(value: string): value is DocsPackage {
  return (DOCS_PACKAGES as readonly string[]).includes(value);
}

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

export function pathFor(entry: DocsEntry): string {
  return `/docs/${entry.id}/`;
}

export function hrefFor(entry: DocsEntry): string {
  return withBase(pathFor(entry));
}

export function navLabel(entry: DocsEntry): string {
  return entry.data.sidebarLabel ?? entry.data.title;
}

export function docsPackageOf(id: string): string {
  return id.split('/')[0] ?? DEFAULT_DOCS_PACKAGE;
}

export function inDocsPackage(entry: DocsEntry, pkg: string): boolean {
  return entry.id === pkg || entry.id.startsWith(`${pkg}/`);
}

export function docsPackageFromPathname(pathname: string): DocsPackage {
  const parts = pathname.split('/').filter(Boolean);
  const docsIndex = parts.indexOf('docs');
  const candidate = docsIndex >= 0 ? parts[docsIndex + 1] : undefined;
  return candidate != null && isDocsPackage(candidate) ? candidate : DEFAULT_DOCS_PACKAGE;
}

export function packageHomeHref(entries: DocsEntry[], pkg: DocsPackage): string {
  const first = groupDocs(entries, pkg)[0]?.entries[0];
  return first != null ? hrefFor(first) : withBase(`/docs/${pkg}/`);
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

export function groupDocs(entries: DocsEntry[], pkg: string = DEFAULT_DOCS_PACKAGE) {
  const sorted = sortDocs(entries.filter((entry) => inDocsPackage(entry, pkg)));
  return SECTION_ORDER.map((section) => ({
    section,
    label: SECTION_LABELS[section],
    entries: sorted.filter((entry) => entry.data.section === section),
  })).filter((group) => group.entries.length > 0);
}

export function neighbors(entries: DocsEntry[], id: string) {
  const sorted = sortDocs(entries.filter((entry) => inDocsPackage(entry, docsPackageOf(id))));
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
