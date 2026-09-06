/**
 * Assigns each topic one of the five categorical chart colours, so a topic keeps the same accent
 * everywhere it appears (rail list, table, and later the topology board).
 *
 * Derived from the name rather than list position: positions shift when topics are created or
 * deleted, and a dot that changes colour on refresh is worse than no dot at all.
 */
const ACCENTS = ['bg-chart-1', 'bg-chart-2', 'bg-chart-3', 'bg-chart-4', 'bg-chart-5'] as const;

export function topicAccentClass(name: string): string {
  // FNV-1a: short, stable across runs, and good enough spread for five buckets.
  let hash = 0x811c9dc5;
  for (let index = 0; index < name.length; index++) {
    hash ^= name.charCodeAt(index);
    hash = Math.imul(hash, 0x01000193) >>> 0;
  }
  return ACCENTS[hash % ACCENTS.length] ?? ACCENTS[0];
}
