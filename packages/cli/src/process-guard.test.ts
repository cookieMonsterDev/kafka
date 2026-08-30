import { Linter } from 'eslint';
import { beforeAll, describe, expect, it } from 'vitest';

// `bin.ts` and `runtime.ts` don't exist yet (they land with the walking skeleton), but the guard
// that exempts them must already be wired up correctly — this fixture exercises the real block
// from the repo's own eslint.config.js, not a re-typed copy of it, so a future edit that loosens
// or drops the guard fails this test rather than only failing an eventual manual review.
let processGuardBlock: Linter.Config;

beforeAll(async () => {
  // @ts-expect-error -- eslint.config.js is a plain JS module with no declaration file
  const rootEslintConfig = (await import('../../../eslint.config.js')).default as Linter.Config[];
  const found = rootEslintConfig.find((block) => block.rules != null && 'no-restricted-globals' in block.rules);
  if (found === undefined) {
    throw new Error('eslint.config.js has no no-restricted-globals block for the cli package');
  }
  processGuardBlock = found;
});

const linter = new Linter({ configType: 'flat' });
const code = 'export const x = process.env.FOO;\n';

function ruleIdsFor(filename: string): (string | null)[] {
  return linter.verify(code, [processGuardBlock], filename).map((message) => message.ruleId);
}

describe('the cli src process guard', () => {
  it('rejects a bare `process` reference in an ordinary command file', () => {
    expect(ruleIdsFor('packages/cli/src/commands/topic/list.ts')).toContain('no-restricted-globals');
  });

  it('allows `process` in the runtime port', () => {
    expect(ruleIdsFor('packages/cli/src/runtime.ts')).not.toContain('no-restricted-globals');
  });

  it('allows `process` in the process entry point', () => {
    expect(ruleIdsFor('packages/cli/src/bin.ts')).not.toContain('no-restricted-globals');
  });
});
