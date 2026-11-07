import type { DocsSection } from '@/lib/docs';

export type SearchHeading = {
  text: string;
  slug: string;
};

export type SearchDoc = {
  href: string;
  title: string;
  description: string;
  section: DocsSection;
  sectionLabel: string;
  headings: SearchHeading[];
  package: string;
  body: string;
};

export type SearchHit = {
  href: string;
  title: string;
  description: string;
  section: DocsSection;
  sectionLabel: string;
  heading?: string;
  package: string;
};

export function filterSearch(docs: SearchDoc[], query: string): SearchHit[] {
  const tokens = query
    .trim()
    .toLowerCase()
    .split(/\s+/)
    .filter((token) => token.length > 0);

  if (tokens.length === 0) {
    return docs.map((doc) => toHit(doc));
  }

  const ranked: { hit: SearchHit; score: number }[] = [];
  for (const doc of docs) {
    const scored = scoreDoc(doc, tokens);
    if (scored != null) {
      ranked.push(scored);
    }
  }
  ranked.sort((a, b) => b.score - a.score || a.hit.title.localeCompare(b.hit.title));
  return ranked.map((entry) => entry.hit);
}

function toHit(doc: SearchDoc, heading?: SearchHeading): SearchHit {
  return {
    package: doc.package,
    href: heading == null ? doc.href : `${doc.href}#${heading.slug}`,
    title: doc.title,
    description: doc.description,
    section: doc.section,
    sectionLabel: doc.sectionLabel,
    heading: heading?.text,
  };
}

function scoreDoc(doc: SearchDoc, tokens: string[]): { hit: SearchHit; score: number } | null {
  const title = doc.title.toLowerCase();
  const description = doc.description.toLowerCase();
  const section = doc.sectionLabel.toLowerCase();
  let score = 0;
  let heading: SearchHeading | undefined;

  for (const token of tokens) {
    let tokenScore = 0;
    if (title === token) {
      tokenScore = 100;
    } else if (title.startsWith(token)) {
      tokenScore = 80;
    } else if (title.includes(token)) {
      tokenScore = 60;
    }
    if (description.includes(token)) {
      tokenScore = Math.max(tokenScore, 20);
    }
    if (section.includes(token)) {
      tokenScore = Math.max(tokenScore, 10);
    }
    for (const candidate of doc.headings) {
      if (candidate.text.toLowerCase().includes(token)) {
        tokenScore = Math.max(tokenScore, 40);
        heading ??= candidate;
      }
    }
    if (doc.body.toLowerCase().includes(token)) tokenScore = Math.max(tokenScore, 5);
    if (tokenScore === 0) {
      return null;
    }
    score += tokenScore;
  }

  return { hit: toHit(doc, heading), score };
}
