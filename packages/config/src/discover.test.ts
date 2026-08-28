import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, isAbsolute, join, relative } from 'node:path';
import { fileURLToPath } from 'node:url';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { discoverConfigFile } from './discover';

const FIXTURES = join(dirname(fileURLToPath(import.meta.url)), '../test/fixtures/discover');

describe('discoverConfigFile — static fixtures', () => {
  it.each([
    ['ts', 'kafka.config.ts'],
    ['mts', 'kafka.config.mts'],
    ['cts', 'kafka.config.cts'],
    ['js', 'kafka.config.js'],
    ['mjs', 'kafka.config.mjs'],
    ['cjs', 'kafka.config.cjs'],
    ['json', 'kafka.config.json'],
  ])('finds %s in the ladder', (dir, filename) => {
    const cwd = join(FIXTURES, 'ladder', dir);

    expect(discoverConfigFile({ cwd, searchParents: false })).toBe(join(cwd, filename));
  });

  it('finds .config/kafka.ts only when no kafka.config.* exists at that level', () => {
    const cwd = join(FIXTURES, 'nested-only');

    expect(discoverConfigFile({ cwd, searchParents: false })).toBe(join(cwd, '.config', 'kafka.ts'));
  });

  it('prefers a top-level kafka.config.* over .config/kafka.* in the same directory', () => {
    const cwd = join(FIXTURES, 'nested-precedence');

    expect(discoverConfigFile({ cwd, searchParents: false })).toBe(join(cwd, 'kafka.config.js'));
  });

  it('picks the first top-level candidate by ladder order and warns naming both', () => {
    const cwd = join(FIXTURES, 'multiple-candidates');
    const onDiagnostic = vi.fn();

    const result = discoverConfigFile({ cwd, searchParents: false, onDiagnostic });

    expect(result).toBe(join(cwd, 'kafka.config.ts'));
    expect(onDiagnostic).toHaveBeenCalledExactlyOnceWith(
      expect.objectContaining({
        code: 'config.multiple-candidates',
        level: 'warn',
        path: join(cwd, 'kafka.config.ts'),
        candidates: [join(cwd, 'kafka.config.ts'), join(cwd, 'kafka.config.js')],
      }),
    );
  });

  it('picks the first nested candidate by ladder order and warns naming both', () => {
    const cwd = join(FIXTURES, 'multiple-candidates-nested');
    const onDiagnostic = vi.fn();

    const result = discoverConfigFile({ cwd, searchParents: false, onDiagnostic });

    expect(result).toBe(join(cwd, '.config', 'kafka.ts'));
    expect(onDiagnostic).toHaveBeenCalledExactlyOnceWith(
      expect.objectContaining({
        code: 'config.multiple-candidates',
        candidates: [join(cwd, '.config', 'kafka.ts'), join(cwd, '.config', 'kafka.js')],
      }),
    );
  });

  it('returns null when searchParents is false and the starting dir has no candidate', () => {
    // "discover" itself has no kafka.config.*; only its children do — discovery never looks down.
    const cwd = join(FIXTURES);

    expect(discoverConfigFile({ cwd, searchParents: false })).toBeNull();
  });

  it('never calls process.chdir', () => {
    const spy = vi.spyOn(process, 'chdir');

    discoverConfigFile({ cwd: join(FIXTURES, 'ladder', 'ts'), searchParents: false });

    expect(spy).not.toHaveBeenCalled();
    spy.mockRestore();
  });

  it('resolves a relative cwd to an absolute path (pathToFileURL/memoisation downstream require one)', () => {
    const absolute = join(FIXTURES, 'ladder', 'ts');
    const relativeCwd = relative(process.cwd(), absolute);
    // Guard the fixture itself: a relative path that already escaped to absolute (e.g. a
    // different drive on Windows) would make this test vacuously pass.
    expect(isAbsolute(relativeCwd)).toBe(false);

    const result = discoverConfigFile({ cwd: relativeCwd, searchParents: false });

    expect(result).toBe(join(absolute, 'kafka.config.ts'));
    expect(result).not.toBeNull();
    expect(isAbsolute(result as string)).toBe(true);
  });
});

describe('discoverConfigFile — dynamic trees', () => {
  const tempDirs: string[] = [];

  function makeTempDir(): string {
    const dir = mkdtempSync(join(tmpdir(), 'kafka-config-discover-'));
    tempDirs.push(dir);
    return dir;
  }

  afterEach(() => {
    for (const dir of tempDirs.splice(0)) {
      rmSync(dir, { recursive: true, force: true });
    }
  });

  it('searches upward from a nested directory and finds an ancestor config', () => {
    const root = makeTempDir();
    const nested = join(root, 'apps', 'api');
    mkdirSync(nested, { recursive: true });
    writeFileSync(join(root, 'kafka.config.ts'), 'export default { client: { brokers: ["root:9092"] } };\n');

    expect(discoverConfigFile({ cwd: nested })).toBe(join(root, 'kafka.config.ts'));
  });

  it('stops the upward search at a .git boundary, inclusive of that directory', () => {
    const root = makeTempDir();
    const boundary = join(root, 'workspace');
    const nested = join(boundary, 'apps', 'api');
    mkdirSync(join(boundary, '.git'), { recursive: true });
    mkdirSync(nested, { recursive: true });
    // A config above the .git boundary must never be found.
    writeFileSync(join(root, 'kafka.config.ts'), 'export default { client: { brokers: ["above:9092"] } };\n');

    expect(discoverConfigFile({ cwd: nested })).toBeNull();
  });

  it('finds a config that lives directly in the .git boundary directory itself', () => {
    const root = makeTempDir();
    const boundary = join(root, 'workspace');
    const nested = join(boundary, 'apps', 'api');
    mkdirSync(join(boundary, '.git'), { recursive: true });
    mkdirSync(nested, { recursive: true });
    writeFileSync(join(boundary, 'kafka.config.ts'), 'export default { client: { brokers: ["boundary:9092"] } };\n');

    expect(discoverConfigFile({ cwd: nested })).toBe(join(boundary, 'kafka.config.ts'));
  });

  it('stops the upward search at a pnpm-workspace.yaml boundary', () => {
    const root = makeTempDir();
    const boundary = join(root, 'workspace');
    const nested = join(boundary, 'apps', 'api');
    mkdirSync(nested, { recursive: true });
    writeFileSync(join(boundary, 'pnpm-workspace.yaml'), 'packages:\n  - "packages/*"\n');
    writeFileSync(join(root, 'kafka.config.ts'), 'export default { client: { brokers: ["above:9092"] } };\n');

    expect(discoverConfigFile({ cwd: nested })).toBeNull();
  });

  it('stops the upward search at a package.json with a workspaces field', () => {
    const root = makeTempDir();
    const boundary = join(root, 'workspace');
    const nested = join(boundary, 'apps', 'api');
    mkdirSync(nested, { recursive: true });
    writeFileSync(join(boundary, 'package.json'), JSON.stringify({ workspaces: ['packages/*'] }));
    writeFileSync(join(root, 'kafka.config.ts'), 'export default { client: { brokers: ["above:9092"] } };\n');

    expect(discoverConfigFile({ cwd: nested })).toBeNull();
  });

  it('does not treat a plain package.json (no workspaces field) as a boundary', () => {
    const root = makeTempDir();
    const middle = join(root, 'middle');
    const nested = join(middle, 'apps', 'api');
    mkdirSync(nested, { recursive: true });
    writeFileSync(join(middle, 'package.json'), JSON.stringify({ name: 'not-a-workspace-root' }));
    writeFileSync(join(root, 'kafka.config.ts'), 'export default { client: { brokers: ["root:9092"] } };\n');

    expect(discoverConfigFile({ cwd: nested })).toBe(join(root, 'kafka.config.ts'));
  });

  it('respects searchParents: false even when an ancestor has a config', () => {
    const root = makeTempDir();
    const nested = join(root, 'apps', 'api');
    mkdirSync(nested, { recursive: true });
    writeFileSync(join(root, 'kafka.config.ts'), 'export default { client: { brokers: ["root:9092"] } };\n');

    expect(discoverConfigFile({ cwd: nested, searchParents: false })).toBeNull();
  });

  it('returns null when nothing is found before the filesystem root', () => {
    const root = makeTempDir();

    expect(discoverConfigFile({ cwd: root })).toBeNull();
  });

  it('name: a different consumer discovers its own <name>.config.* ladder', () => {
    const root = makeTempDir();
    writeFileSync(join(root, 'studio.config.ts'), 'export default {};\n');
    // A kafka.config.* in the same directory must not interfere with a "studio" search.
    writeFileSync(join(root, 'kafka.config.ts'), 'export default {};\n');

    expect(discoverConfigFile({ cwd: root, name: 'studio', searchParents: false })).toBe(
      join(root, 'studio.config.ts'),
    );
  });

  it('name defaults to "kafka" when omitted', () => {
    const root = makeTempDir();
    writeFileSync(join(root, 'kafka.config.ts'), 'export default {};\n');

    expect(discoverConfigFile({ cwd: root, searchParents: false })).toBe(join(root, 'kafka.config.ts'));
  });
});
