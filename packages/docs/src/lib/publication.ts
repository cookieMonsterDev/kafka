import { execFileSync } from 'node:child_process';
import core from '../../../core/package.json';
import cli from '../../../cli/package.json';
import config from '../../../config/package.json';
import studio from '../../../studio/package.json';
import docs from '../../package.json';
import { withBase } from '@/lib/base';
import { hrefFor, docsPackageOf, type DocsEntry } from '@/lib/docs';

export const packageVersions: Record<string, string> = {
  core: core.version,
  cli: cli.version,
  config: config.version,
  studio: studio.version,
};

function sourceRevision(): string {
  try {
    return execFileSync('git', ['rev-parse', 'HEAD'], { encoding: 'utf8' }).trim();
  } catch {
    return 'master';
  }
}

export const publication = { docsVersion: docs.version, packages: packageVersions, revision: sourceRevision() };

export function absoluteUrl(path: string): string {
  return new URL(path, import.meta.env.SITE).href;
}

export function markdownHref(entry: DocsEntry): string {
  return `${hrefFor(entry)}index.md`;
}

/** Resolve against the HTML page before exporting, so /index.md never changes relative-link meaning. */
export function absoluteMarkdownLinks(body: string, htmlUrl: string): string {
  const rewrite = (link: string): string => {
    if (/^(?:https?:|mailto:|tel:|data:)/i.test(link)) return link;
    return new URL(link, htmlUrl).href;
  };
  return body
    .split(/(```[\s\S]*?```|~~~[\s\S]*?~~~)/g)
    .map((part, index) => {
      if (index % 2 === 1) return part;
      return part
        .replace(
          /(!?\[[^\]]*\]\()([^\s)]+)([^)]*\))/g,
          (_match, prefix: string, link: string, suffix: string) => `${prefix}${rewrite(link)}${suffix}`,
        )
        .replace(/^(\[[^\]]+\]:\s*)(\S+)/gm, (_match, prefix: string, link: string) => `${prefix}${rewrite(link)}`)
        .replace(
          /(\b(?:src|href)=")([^"]+)(")/g,
          (_match, prefix: string, link: string, suffix: string) => `${prefix}${rewrite(link)}${suffix}`,
        );
    })
    .join('');
}

/**
 * Replaces each ```chart block (ApexCharts options as JSON, see rehype-chart.ts) with one line, so the
 * Markdown export stays readable; pages put the chart's data in a table after the block.
 */
export function summarizeChartBlocks(body: string): string {
  return body.replace(/^```chart\n([\s\S]*?)\n```$/gm, (_block, json: string) => {
    let title = 'chart';
    try {
      const options = JSON.parse(json) as { description?: unknown; title?: { text?: unknown } };
      const text = typeof options.description === 'string' ? options.description : options.title?.text;
      if (typeof text === 'string') title = text;
    } catch {
      // The site build rejects invalid chart JSON; keep the generic line here.
    }
    return `[Chart: ${title}]`;
  });
}

export function markdownFor(entry: DocsEntry): string {
  const url = absoluteUrl(hrefFor(entry));
  const pkg = docsPackageOf(entry.id);
  const body = summarizeChartBlocks(entry.body ?? '');
  return `# ${entry.data.title}\n\n${entry.data.description}\n\nPackage: @cookiemonsterdev/kafka-${pkg} ${packageVersions[pkg] ?? ''}\nCanonical: ${url}\nSource revision: ${publication.revision}\n\n${absoluteMarkdownLinks(body, url)}\n`;
}

const skillFiles = import.meta.glob('../../../*/skills/**/*.md', {
  query: '?raw',
  import: 'default',
  eager: true,
}) as Record<string, string>;

export const publishedSkills = Object.entries(skillFiles).map(([path, body]) => {
  const relative = path.replace('../../../', '');
  return { path: relative, body, href: withBase(`/agent-skills/${relative}`) };
});

export function skillMarkdown(skill: (typeof publishedSkills)[number]): string {
  const directory = skill.path.slice(0, skill.path.lastIndexOf('/') + 1);
  return skill.body.replace(
    /(\[[^\]]*\]\()([^\s)]+)([^)]*\))/g,
    (_match, prefix: string, link: string, suffix: string) => {
      if (/^(?:https?:|#)/.test(link)) return `${prefix}${link}${suffix}`;
      return `${prefix}${absoluteUrl(withBase(`/agent-skills/${directory}${link}`))}${suffix}`;
    },
  );
}
