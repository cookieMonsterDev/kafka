import { getCollection } from 'astro:content';
import type { APIRoute } from 'astro';
import { DOCS_PACKAGES, DOCS_PACKAGE_META, docsPackageOf, sortDocs } from '@/lib/docs';
import { absoluteUrl, markdownHref, publication, publishedSkills } from '@/lib/publication';
import { withBase } from '@/lib/base';

export const GET: APIRoute = async () => {
  const entries = sortDocs(await getCollection('docs'));
  const lines = [
    '# Kafka TypeScript tools',
    '',
    '> Core client, admin CLI, generic config loader, and local Studio UI. Node.js 24+ or Bun 1.4+.',
    '',
    `Docs version: ${publication.docsVersion}. Source revision: ${publication.revision}.`,
    'Offsets are bigint; import public APIs from package roots. Core defaults: idempotent false, read_committed isolation, murmur2 partitioning. Check each feature’s broker requirements.',
    'Use the skill matching your installed package version. This index describes the deployed docs; older release source snapshots are linked from the documentation-access guide.',
    '',
    `Machine index: ${absoluteUrl(withBase('/docs-index.json'))}`,
    '',
  ];
  for (const pkg of DOCS_PACKAGES) {
    lines.push(`## ${DOCS_PACKAGE_META[pkg].label} ${publication.packages[pkg]}`, '');
    for (const entry of entries.filter((page) => docsPackageOf(page.id) === pkg)) {
      lines.push(`- [${entry.data.title}](${absoluteUrl(markdownHref(entry))}): ${entry.data.description}`);
    }
    lines.push('');
  }
  lines.push('## Agent skills', '');
  for (const skill of publishedSkills.filter((item) => item.path.endsWith('/SKILL.md')))
    lines.push(`- [${skill.path}](${absoluteUrl(skill.href)})`);
  return new Response(lines.join('\n') + '\n', { headers: { 'Content-Type': 'text/plain; charset=utf-8' } });
};
