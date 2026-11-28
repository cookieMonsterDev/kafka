import { spawnSync } from 'node:child_process';
import { copyFileSync, mkdirSync, mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';

// Lints with the repo's own .oxlintrc.json, so an edit that loosens or drops the guard fails here.
// Override globs are relative to the config file, so it is copied into a temp tree that mirrors
// the repo. Type-aware linting is off there: the guard does not need it, and oxlint cannot find
// oxlint-tsgolint from a directory outside the repo.
const rootConfigPath = fileURLToPath(new URL('../../../.oxlintrc.json', import.meta.url));
const oxlintPath = fileURLToPath(new URL('../node_modules/.bin/oxlint', import.meta.url));

const commandFile = 'packages/cli/src/commands/topic/list.ts';
const runtimeFile = 'packages/cli/src/runtime.ts';
const entryPointFile = 'packages/cli/src/bin.ts';

interface OxlintReport {
  diagnostics: { code: string; filename: string }[];
}

let fixtureRoot: string;
let report: OxlintReport;

beforeAll(() => {
  fixtureRoot = mkdtempSync(join(tmpdir(), 'cli-process-guard-'));
  copyFileSync(rootConfigPath, join(fixtureRoot, 'repo.oxlintrc.json'));
  writeFileSync(
    join(fixtureRoot, '.oxlintrc.json'),
    JSON.stringify({ extends: ['./repo.oxlintrc.json'], options: { typeAware: false } }),
  );
  for (const file of [commandFile, runtimeFile, entryPointFile]) {
    mkdirSync(join(fixtureRoot, dirname(file)), { recursive: true });
    writeFileSync(join(fixtureRoot, file), 'export const x = process.env.FOO;\n');
  }

  const result = spawnSync(oxlintPath, ['--format', 'json', commandFile, runtimeFile, entryPointFile], {
    cwd: fixtureRoot,
    encoding: 'utf8',
  });
  if (result.error !== undefined) {
    throw result.error;
  }
  if (!result.stdout.startsWith('{')) {
    throw new Error(`oxlint did not print a JSON report:\n${result.stdout}${result.stderr}`);
  }
  report = JSON.parse(result.stdout) as OxlintReport;
});

afterAll(() => {
  rmSync(fixtureRoot, { recursive: true, force: true });
});

function ruleCodesFor(filename: string): string[] {
  return report.diagnostics
    .filter((diagnostic) => diagnostic.filename === filename)
    .map((diagnostic) => diagnostic.code);
}

describe('the cli src process guard', () => {
  it('rejects a bare `process` reference in an ordinary command file', () => {
    expect(ruleCodesFor(commandFile)).toContain('eslint(no-restricted-globals)');
  });

  it('allows `process` in the runtime port', () => {
    expect(ruleCodesFor(runtimeFile)).not.toContain('eslint(no-restricted-globals)');
  });

  it('allows `process` in the process entry point', () => {
    expect(ruleCodesFor(entryPointFile)).not.toContain('eslint(no-restricted-globals)');
  });
});
