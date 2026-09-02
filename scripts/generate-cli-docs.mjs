#!/usr/bin/env node
// Regenerates the docs site's CLI command reference from the CLI's own command registry, so the
// ~60-command surface never drifts from what `kafka --help` actually shows. Run it directly
// (`node scripts/generate-cli-docs.mjs`) after adding or changing a CLI command; `--check` verifies
// the committed file instead of overwriting it. Lives here, not under `packages/cli/`, because it
// reads one package's source and writes into another's — a repo-wide concern, like every other
// script in this directory.
import { createRequire } from 'node:module';
import { dirname, join } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { readFileSync, writeFileSync } from 'node:fs';
import * as prettier from 'prettier';

const ROOT = dirname(dirname(fileURLToPath(import.meta.url)));
const CLI_ROOT = join(ROOT, 'packages', 'cli');
const COMMANDS_INDEX_PATH = join(CLI_ROOT, 'src', 'commands', 'index.ts');
const REGISTRY_PATH = join(CLI_ROOT, 'src', 'registry.ts');
const OUTPUT_PATH = join(ROOT, 'packages', 'docs', 'src', 'content', 'docs', 'cli', 'reference', 'commands.md');

/**
 * `commands/index.ts` (like every file in the CLI package) imports its neighbors without a file
 * extension, which Node's native TypeScript support cannot resolve on its own — see the plan's
 * own "Extensionless relative import" finding. `@cookiemonsterdev/kafka-config`'s transform hooks
 * rescue exactly that case for `require()`, which is why this loads the registry synchronously
 * instead of via `import()` (`registerHooks` only intercepts `require()`).
 *
 * `require` is rooted at the CLI package, not this script's own location, purely so the bare
 * specifier `@cookiemonsterdev/kafka-config` resolves through the CLI's own `node_modules` (where
 * it's a real dependency) rather than the repo root's (where it isn't) — every other `require()`
 * call here passes an absolute path, which resolves the same way regardless of that base.
 */
function loadCommandRegistry() {
  const require = createRequire(join(CLI_ROOT, 'package.json'));
  const { installConfigTransformHooks } = require('@cookiemonsterdev/kafka-config');
  installConfigTransformHooks();
  const { ALL_COMMANDS } = require(COMMANDS_INDEX_PATH);
  const { createRegistry, commandGroups } = require(REGISTRY_PATH);
  const registry = createRegistry(ALL_COMMANDS);
  const groups = commandGroups(registry);
  return { commands: ALL_COMMANDS.filter((command) => command.hidden !== true), groups };
}

function flagUsage(flag) {
  const alias = flag.alias !== undefined ? `-${flag.alias}, ` : '';
  if (flag.type === 'boolean') return `${alias}--${flag.name}`;
  if (flag.keyValue === true)
    return `${alias}--${flag.name} <key=value>${flag.multiple === true ? ' (repeatable)' : ''}`;
  if (flag.type === 'enum') return `${alias}--${flag.name} <${(flag.values ?? []).join('|')}>`;
  return `${alias}--${flag.name} <${flag.type}>${flag.multiple === true ? ' (repeatable)' : ''}`;
}

function commandLine(command) {
  const positionals = (command.positionals ?? [])
    .map((p) => (p.variadic === true ? `<${p.name}...>` : `<${p.name}>`))
    .join(' ');
  return ['kafka', command.path.join(' '), positionals].filter((part) => part.length > 0).join(' ');
}

function renderCommand(command) {
  const lines = [`### ${command.path.join(' ')}`, ''];
  lines.push('```sh', commandLine(command), '```', '');
  lines.push(command.summary + (command.unstable === true ? ' _(unstable)_' : ''), '');

  if (command.flags !== undefined && command.flags.length > 0) {
    lines.push('| Flag | Description |', '| --- | --- |');
    for (const flag of command.flags) {
      lines.push(`| \`${flagUsage(flag)}\` | ${flag.brief} |`);
    }
    lines.push('');
  }

  if (command.examples !== undefined && command.examples.length > 0) {
    lines.push('Examples:', '', '```sh', ...command.examples.map((example) => `kafka ${example}`), '```', '');
  }

  return lines.join('\n');
}

/** Every top-level segment that is a group (has children) vs. a standalone leaf command. */
function topLevelSections(commands, groups) {
  const groupNames = new Set();
  const standalone = [];
  for (const command of commands) {
    const [first] = command.path;
    if (first === undefined) continue;
    if (groups.has(first)) groupNames.add(first);
    else standalone.push(command);
  }
  return {
    groupNames: [...groupNames].sort(),
    standalone: standalone.sort((a, b) => a.path[0].localeCompare(b.path[0])),
  };
}

function generateCommandsMarkdown(commands, groups) {
  const sorted = [...commands].sort((a, b) => a.path.join(' ').localeCompare(b.path.join(' ')));
  const { groupNames, standalone } = topLevelSections(sorted, groups);

  const lines = [
    '---',
    'title: Command reference',
    'description: Every kafka command, generated from the command registry',
    'order: 1',
    'section: reference',
    '---',
    '',
    "_Generated from the CLI's own command registry by `scripts/generate-cli-docs.mjs` — do not",
    'edit by hand; run that script again after changing a command._',
    '',
  ];

  if (standalone.length > 0) {
    lines.push('## General', '');
    for (const command of standalone) lines.push(renderCommand(command));
  }

  for (const group of groupNames) {
    lines.push(`## ${group}`, '');
    const children = sorted.filter((command) => command.path[0] === group);
    for (const command of children) lines.push(renderCommand(command));
  }

  return `${lines.join('\n').trimEnd()}\n`;
}

/**
 * Every markdown file in this repo is reformatted by Prettier on commit (see `.prettierrc.json`
 * and the root `lint-staged` config) — writing raw, unformatted markdown here would pass on a
 * clean checkout and then go stale the moment anyone committed it, since the commit hook's own
 * reformatting would no longer match "a fresh generation" byte for byte.
 */
async function formatMarkdown(markdown) {
  const config = (await prettier.resolveConfig(OUTPUT_PATH)) ?? {};
  return prettier.format(markdown, { ...config, filepath: OUTPUT_PATH });
}

async function main() {
  const check = process.argv.includes('--check');
  const { commands, groups } = loadCommandRegistry();
  const fresh = await formatMarkdown(generateCommandsMarkdown(commands, groups));

  if (check) {
    const committed = readFileSync(OUTPUT_PATH, 'utf8');
    if (committed !== fresh) {
      process.stderr.write(`${OUTPUT_PATH} is stale — run "node scripts/generate-cli-docs.mjs" to regenerate it\n`);
      process.exitCode = 1;
      return;
    }
    process.stdout.write('commands.md is up to date\n');
    return;
  }

  writeFileSync(OUTPUT_PATH, fresh);
  process.stdout.write(`wrote ${OUTPUT_PATH}\n`);
}

if (import.meta.url === pathToFileURL(process.argv[1] ?? '').href) {
  await main();
}
