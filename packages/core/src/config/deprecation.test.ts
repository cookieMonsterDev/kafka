import { readFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { describe, expect, it } from 'vitest';

const INDEX_PATH = join(dirname(fileURLToPath(import.meta.url)), 'index.ts');
const source = readFileSync(INDEX_PATH, 'utf8');
const lines = source.split('\n');

/**
 * D21's two lists, kept here so a future addition to either list in `index.ts` without a matching
 * update here fails the suite instead of silently drifting. `home` is `'kafka-config'` for the
 * generic machinery re-exported from `@cookiemonsterdev/kafka-config` (deprecated, removed in core
 * 3.0.0), `'core'` for core's own Kafka-typed facade (not deprecated).
 */
const D21_SYMBOLS: { name: string; home: 'kafka-config' | 'core' }[] = [
  { name: 'defineConfig', home: 'core' },
  { name: 'loadKafkaConfig', home: 'core' },
  { name: 'CANDIDATE_EXTENSIONS', home: 'kafka-config' },
  { name: 'discoverConfigFile', home: 'kafka-config' },
  { name: 'loadConfigFileSync', home: 'kafka-config' },
  { name: 'loadConfigFileAsync', home: 'kafka-config' },
  { name: 'mergeConfigLayers', home: 'kafka-config' },
  { name: 'KafkaConfigError', home: 'kafka-config' },
  { name: 'KafkaConfigRequiresAsyncError', home: 'kafka-config' },
  { name: 'defaultOnConfigDiagnostic', home: 'kafka-config' },
];

/** True when an `export` line at `lineIndex` is immediately preceded by a `@deprecated` JSDoc comment. */
function isPrecededByDeprecatedTag(lineIndex: number): boolean {
  for (let i = lineIndex - 1; i >= 0; i--) {
    const line = lines[i]?.trim();
    if (line === undefined || line === '') continue;
    return line.startsWith('/**') && line.includes('@deprecated');
  }
  return false;
}

function findExportLineIndex(symbolName: string): number {
  const index = lines.findIndex((line) => line.startsWith('export') && new RegExp(`\\b${symbolName}\\b`).test(line));
  if (index === -1) {
    throw new Error(`index.ts has no export statement for "${symbolName}" — update D21_SYMBOLS or index.ts`);
  }
  return index;
}

describe('kafka-core/config re-export deprecation (D21)', () => {
  it.each(D21_SYMBOLS)('$name ($home) is $home === "kafka-config" ? deprecated : not deprecated', ({ name, home }) => {
    const lineIndex = findExportLineIndex(name);
    const deprecated = isPrecededByDeprecatedTag(lineIndex);

    expect(deprecated).toBe(home === 'kafka-config');
  });

  it('every export sourced from @cookiemonsterdev/kafka-config is tagged @deprecated', () => {
    lines.forEach((line, lineIndex) => {
      if (!line.startsWith('export') || !line.includes("'@cookiemonsterdev/kafka-config'")) return;

      expect(isPrecededByDeprecatedTag(lineIndex)).toBe(true);
    });
  });

  it("every deprecated symbol names core 3.0.0 as the removal release, so the two can't silently disagree", () => {
    const deprecationLines = lines.filter((line) => line.includes('@deprecated'));

    expect(deprecationLines.length).toBeGreaterThan(0);
    for (const line of deprecationLines) {
      expect(line).toContain('core 3.0.0');
    }
  });
});
