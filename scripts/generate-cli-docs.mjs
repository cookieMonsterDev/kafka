#!/usr/bin/env node
// Regenerates the docs site's CLI command reference from the CLI's own command registry, so the
// ~60-command surface never drifts from what `kafka --help` actually shows: one page per command
// under `cli/reference/commands/`, plus an index page linking to all of them. Run it directly
// (`node scripts/generate-cli-docs.mjs`) after adding or changing a CLI command; `--check` verifies
// the committed files instead of overwriting them. Lives here, not under `packages/cli/`, because
// it reads one package's source and writes into another's — a repo-wide concern, like every other
// script in this directory.
import { createRequire } from 'node:module';
import { dirname, join } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { mkdirSync, readdirSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import * as prettier from 'prettier';

const ROOT = dirname(dirname(fileURLToPath(import.meta.url)));
const CLI_ROOT = join(ROOT, 'packages', 'cli');
const COMMANDS_INDEX_PATH = join(CLI_ROOT, 'src', 'commands', 'index.ts');
const REGISTRY_PATH = join(CLI_ROOT, 'src', 'registry.ts');
const REFERENCE_DIR = join(ROOT, 'packages', 'docs', 'src', 'content', 'docs', 'cli', 'reference');
const INDEX_PATH = join(REFERENCE_DIR, 'commands.md');
const COMMANDS_DIR = join(REFERENCE_DIR, 'commands');

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

/** Relative to `commands/`: a standalone command is `<name>.md`; a grouped one is `<group>/<rest-of-path>.md`. */
function commandRelPath(command, groupNames) {
  const [first, ...rest] = command.path;
  if (!groupNames.includes(first)) return `${first}.md`;
  return `${first}/${rest.join('-')}.md`;
}

function renderCommandBody(command) {
  const lines = [];
  lines.push('```sh', commandLine(command), '```', '');
  lines.push(command.summary + (command.unstable === true ? ' _(unstable)_' : ''), '');

  if (command.flags !== undefined && command.flags.length > 0) {
    lines.push('## Flags', '', '| Flag | Description |', '| --- | --- |');
    for (const flag of command.flags) {
      lines.push(`| \`${flagUsage(flag)}\` | ${flag.brief} |`);
    }
    lines.push('');
  }

  if (command.examples !== undefined && command.examples.length > 0) {
    lines.push('## Examples', '', '```sh', ...command.examples.map((example) => `kafka ${example}`), '```', '');
  }

  return lines.join('\n');
}

/** A JSON string literal is also a valid YAML double-quoted scalar, so this escapes colons, quotes, and backslashes correctly regardless of what a command's summary happens to contain. */
function yamlString(value) {
  return JSON.stringify(value);
}

function commandFrontmatter(command, order) {
  return [
    '---',
    `title: ${yamlString(command.path.join(' '))}`,
    `description: ${yamlString(command.summary)}`,
    `order: ${String(order)}`,
    'section: reference',
    'hidden: true',
    '---',
    '',
    "_Generated from the CLI's own command registry by `scripts/generate-cli-docs.mjs` — do not",
    'edit by hand; run that script again after changing a command._',
    '',
  ].join('\n');
}

function generateCommandFiles(commands, groups) {
  const sorted = [...commands].sort((a, b) => a.path.join(' ').localeCompare(b.path.join(' ')));
  const { groupNames, standalone } = topLevelSections(sorted, groups);

  const ordered = [
    ...standalone,
    ...groupNames.flatMap((group) => sorted.filter((command) => command.path[0] === group)),
  ];

  const files = new Map();
  ordered.forEach((command, index) => {
    const relPath = commandRelPath(command, groupNames);
    const markdown = `${commandFrontmatter(command, 10 + index)}${renderCommandBody(command)}`.trimEnd() + '\n';
    files.set(relPath, markdown);
  });
  return { files, groupNames, standalone };
}

function generateIndexMarkdown(commands, groupNames, standalone) {
  const sorted = [...commands].sort((a, b) => a.path.join(' ').localeCompare(b.path.join(' ')));

  const lines = [
    '---',
    'title: Command reference',
    'description: Every kafka command, one page each, generated from the command registry',
    'order: 1',
    'section: reference',
    '---',
    '',
    "_Generated from the CLI's own command registry by `scripts/generate-cli-docs.mjs` — do not",
    'edit by hand; run that script again after changing a command._',
    '',
  ];

  function commandLink(command) {
    const relPath = commandRelPath(command, groupNames).replace(/\.md$/, '');
    // Relative to this index page's own URL, which already ends in `.../commands/` (its file is
    // `commands.md`, a sibling of the `commands/` directory these links point into).
    return `- [\`${command.path.join(' ')}\`](./${relPath}/) — ${command.summary}`;
  }

  if (standalone.length > 0) {
    lines.push('## General', '', ...standalone.map(commandLink), '');
  }

  for (const group of groupNames) {
    lines.push(`## ${group}`, '');
    const children = sorted.filter((command) => command.path[0] === group);
    lines.push(...children.map(commandLink), '');
  }

  return `${lines.join('\n').trimEnd()}\n`;
}

/**
 * Every markdown file in this repo is reformatted by Prettier on commit (see `.prettierrc.json`
 * and the root `lint-staged` config) — writing raw, unformatted markdown here would pass on a
 * clean checkout and then go stale the moment anyone committed it, since the commit hook's own
 * reformatting would no longer match "a fresh generation" byte for byte.
 */
async function formatMarkdown(markdown, filepath) {
  const config = (await prettier.resolveConfig(filepath)) ?? {};
  return prettier.format(markdown, { ...config, filepath });
}

/** Every `.md` file under `dir`, keyed by its path relative to `dir` (POSIX separators). */
function walkMarkdownFiles(dir) {
  const found = new Map();
  let entries;
  try {
    entries = readdirSync(dir, { withFileTypes: true });
  } catch {
    return found;
  }
  for (const entry of entries) {
    const full = join(dir, entry.name);
    if (entry.isDirectory()) {
      for (const [relPath, path] of walkMarkdownFiles(full)) found.set(`${entry.name}/${relPath}`, path);
    } else if (entry.name.endsWith('.md')) {
      found.set(entry.name, full);
    }
  }
  return found;
}

async function main() {
  const check = process.argv.includes('--check');
  const { commands, groups } = loadCommandRegistry();
  const { files, groupNames, standalone } = generateCommandFiles(commands, groups);

  const fresh = new Map();
  fresh.set(INDEX_PATH, await formatMarkdown(generateIndexMarkdown(commands, groupNames, standalone), INDEX_PATH));
  for (const [relPath, markdown] of files) {
    const path = join(COMMANDS_DIR, relPath);
    fresh.set(path, await formatMarkdown(markdown, path));
  }

  if (check) {
    const problems = [];
    const existing = walkMarkdownFiles(COMMANDS_DIR);
    const expectedRelPaths = new Set(files.keys());

    for (const relPath of existing.keys()) {
      if (!expectedRelPaths.has(relPath)) problems.push(`stale file: commands/${relPath}`);
    }
    for (const [path, content] of fresh) {
      let committed;
      try {
        committed = readFileSync(path, 'utf8');
      } catch {
        problems.push(`missing: ${path}`);
        continue;
      }
      if (committed !== content) problems.push(`out of date: ${path}`);
    }

    if (problems.length > 0) {
      process.stderr.write(
        `CLI command docs are stale — run "node scripts/generate-cli-docs.mjs" to regenerate them\n${problems.map((p) => `  - ${p}`).join('\n')}\n`,
      );
      process.exitCode = 1;
      return;
    }
    process.stdout.write('CLI command docs are up to date\n');
    return;
  }

  rmSync(COMMANDS_DIR, { recursive: true, force: true });
  for (const path of fresh.keys()) mkdirSync(dirname(path), { recursive: true });
  for (const [path, content] of fresh) writeFileSync(path, content);
  process.stdout.write(`wrote ${fresh.size} files under ${COMMANDS_DIR} and ${INDEX_PATH}\n`);
}

if (import.meta.url === pathToFileURL(process.argv[1] ?? '').href) {
  await main();
}
