import { createHash } from 'node:crypto';
import type { AstroIntegration } from 'astro';

/**
 * Fenced code blocks in this language hold an ApexCharts options object as strict JSON instead of code.
 * `astro.config.mjs` excludes it from syntax highlighting, so the block reaches the plugin as raw text.
 */
export const chartLanguage = 'chart';

interface HastElement {
  type: 'element';
  tagName: string;
  properties?: Record<string, unknown>;
  children?: unknown[];
  data?: { lang?: string };
}

/** The part of Sätteri's hast visitor context this plugin uses. */
interface ChartVisitorContext {
  readonly fileURL: URL | undefined;
  textContent(node: HastElement): string;
}

function isElement(node: unknown, tagName: string): node is HastElement {
  if (typeof node !== 'object' || node === null) return false;
  const candidate = node as Partial<HastElement>;
  return candidate.type === 'element' && candidate.tagName === tagName;
}

function codeLanguage(code: HastElement): string | undefined {
  if (code.data?.lang) return code.data.lang;
  const classNames = code.properties?.className;
  if (!Array.isArray(classNames)) return undefined;
  const languageClass = classNames.find(
    (className): className is string => typeof className === 'string' && className.startsWith('language-'),
  );
  return languageClass?.slice('language-'.length);
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function chartTitle(options: Record<string, unknown>): string | undefined {
  const title = options.title;
  if (!isRecord(title) || typeof title.text !== 'string' || title.text.trim() === '') return undefined;
  return title.text;
}

/**
 * Turns a ```chart block into `<div class="chart-container" data-chart="…">` for the client script in
 * `src/components/markdown-charts.astro`. The block's optional top-level `description` is not an
 * ApexCharts option: it becomes the container's `aria-label`, which otherwise is the required `title.text`.
 * The chart id is a hash of the options, so builds are reproducible. Invalid JSON or a missing title
 * fails the build.
 */
export const chartBlocksPlugin = {
  name: 'chart-blocks',
  element: {
    filter: ['pre'],
    visit(node: HastElement, ctx: ChartVisitorContext): HastElement | undefined {
      const code = node.children?.find((child) => isElement(child, 'code'));
      if (code === undefined || codeLanguage(code) !== chartLanguage) return undefined;
      const location = ctx.fileURL?.pathname ?? 'a Markdown file';
      let parsed: unknown;
      try {
        parsed = JSON.parse(ctx.textContent(code));
      } catch (error) {
        throw new Error(`${location}: a \`${chartLanguage}\` block is not valid JSON: ${(error as Error).message}`);
      }
      if (!isRecord(parsed)) throw new Error(`${location}: a \`${chartLanguage}\` block must hold a JSON object`);
      const { description, ...options } = parsed;
      const title = chartTitle(options);
      if (title === undefined)
        throw new Error(`${location}: a \`${chartLanguage}\` block needs a non-empty title.text`);
      const id = `chart-${createHash('sha256').update(JSON.stringify(options)).digest('hex').slice(0, 12)}`;
      const chart = isRecord(options.chart) ? options.chart : {};
      const label = typeof description === 'string' && description !== '' ? description : title;
      const height = typeof chart.height === 'number' ? chart.height : undefined;
      return {
        type: 'element',
        tagName: 'div',
        properties: {
          className: ['chart-container'],
          id,
          role: 'img',
          ariaLabel: label,
          dataChart: JSON.stringify({ ...options, chart: { ...chart, id } }),
          ...(height === undefined ? {} : { style: `min-height: ${height}px` }),
        },
        children: [],
      };
    },
  },
};

/**
 * Registers {@link chartBlocksPlugin} with Astro's default Sätteri Markdown processor. Sätteri takes hast
 * plugins through `satteri({ hastPlugins })`; adding to the default processor's options from an
 * integration avoids a direct dependency on `@astrojs/markdown-satteri`.
 */
export function markdownCharts(): AstroIntegration {
  return {
    name: 'markdown-charts',
    hooks: {
      'astro:config:setup': ({ config }) => {
        const { processor } = config.markdown;
        const { hastPlugins } = processor.options as { hastPlugins?: unknown };
        if (processor.name !== 'satteri' || !Array.isArray(hastPlugins)) {
          throw new Error(`markdown-charts supports the Sätteri Markdown processor, not "${processor.name}"`);
        }
        hastPlugins.push(chartBlocksPlugin);
      },
    },
  };
}
