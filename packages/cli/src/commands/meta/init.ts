import { existsSync, readFileSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import { CliUsageError } from '../../args/coerce';
import type { CommandSpec } from '../../args/define';
import { EXIT_CODES } from '../../errors/exit-codes';
import { stringifyJsonSafe } from '../../output/json';

// A bare object, not `defineConfig({...})`: this file has to load standalone, for a user who
// only installed the CLI and never added `@cookiemonsterdev/kafka-core` to their own project.
// `defineConfig` is documented as the preferred form for a project that already depends on core
// directly (it adds compile-time section typing and a freeze); D6 accepts a bare object too, and
// that's what every consumer here — the CLI, and core's own loader — accepts identically.
const TEMPLATE = `export default {
  client: {
    brokers: ['localhost:9092'],
  },
};
`;

function hasTypeScriptDependency(packageJsonPath: string): boolean {
  let packageJson: unknown;
  try {
    packageJson = JSON.parse(readFileSync(packageJsonPath, 'utf8'));
  } catch {
    return false;
  }
  if (typeof packageJson !== 'object' || packageJson === null) return false;
  const { dependencies, devDependencies } = packageJson as Record<string, unknown>;
  const has = (deps: unknown): boolean => typeof deps === 'object' && deps !== null && 'typescript' in deps;
  return has(dependencies) || has(devDependencies);
}

/** TypeScript when a `tsconfig.json` exists, or `typescript` is a dependency of the nearest `package.json`. */
function detectTypeScript(cwd: string): boolean {
  if (existsSync(join(cwd, 'tsconfig.json'))) return true;
  const packageJsonPath = join(cwd, 'package.json');
  return existsSync(packageJsonPath) && hasTypeScriptDependency(packageJsonPath);
}

export const initCommand: CommandSpec = {
  path: ['init'],
  summary: 'Scaffold a kafka.config file in the current directory',
  flags: [
    { name: 'ts', type: 'boolean', brief: 'scaffold kafka.config.ts (default when TypeScript is detected)' },
    { name: 'js', type: 'boolean', brief: 'scaffold kafka.config.mjs instead' },
    { name: 'force', type: 'boolean', brief: 'overwrite the file if it already exists' },
  ],
  examples: ['init', 'init --js', 'init --force'],
  exitCodes: [EXIT_CODES.ok, EXIT_CODES.usage],
  async run({ flags, runtime, output }) {
    if (flags.ts === true && flags.js === true) {
      throw new CliUsageError('--ts and --js are mutually exclusive');
    }

    const useTs = flags.ts === true || (flags.js !== true && detectTypeScript(runtime.cwd));
    const filename = useTs ? 'kafka.config.ts' : 'kafka.config.mjs';
    const target = join(runtime.cwd, filename);

    if (existsSync(target) && flags.force !== true) {
      throw new CliUsageError(`"${filename}" already exists in ${runtime.cwd} — pass --force to overwrite it`);
    }

    writeFileSync(target, TEMPLATE);

    output.write({
      human: () => `wrote ${filename}`,
      json: () => stringifyJsonSafe({ path: target, format: useTs ? 'ts' : 'mjs' }),
    });
    return EXIT_CODES.ok;
  },
};
