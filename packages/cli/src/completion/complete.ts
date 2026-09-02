import type { CommandSpec } from '../args/define';

/**
 * Global flags every command accepts (see `registry.ts`'s `RESERVED_FLAG_NAMES`), spelled out as
 * the literal tokens a shell would insert — long form plus the handful with a short alias.
 * `--format`'s own values are handled separately, in {@link enumValuesForFlag}.
 */
const GLOBAL_FLAG_TOKENS: readonly string[] = [
  '--help',
  '-h',
  '--version',
  '--json',
  '--format',
  '-q',
  '--quiet',
  '-v',
  '--verbose',
  '--color',
  '--no-color',
  '--config-file',
  '--profile',
];

const FORMAT_VALUES: readonly string[] = ['human', 'json'];

function flagNameFromToken(token: string): string {
  return token.replace(/^--?/, '');
}

/** Every path segment immediately after `prefix` reachable in `commands` — a group or a verb. */
function nextSegments(prefix: readonly string[], commands: readonly CommandSpec[]): string[] {
  const prefixKey = prefix.join(' ');
  const seen = new Set<string>();
  for (const command of commands) {
    const key = command.path.join(' ');
    if (prefix.length > 0 && key !== prefixKey && !key.startsWith(`${prefixKey} `)) continue;
    const nextSegment = command.path[prefix.length];
    if (nextSegment !== undefined) seen.add(nextSegment);
  }
  return [...seen];
}

/** The values a just-typed flag's own value should complete to, if it's a closed enum. */
function enumValuesForFlag(token: string, matchedLeaf: CommandSpec | undefined): readonly string[] | undefined {
  const name = flagNameFromToken(token);
  if (name === 'format') return FORMAT_VALUES;
  const flag = matchedLeaf?.flags?.find((candidate) => candidate.name === name || candidate.alias === name);
  return flag?.type === 'enum' ? (flag.values ?? []) : undefined;
}

/** Every flag token worth suggesting for `matchedLeaf`, given what `priorWords` already used. */
function flagCandidates(matchedLeaf: CommandSpec | undefined, priorWords: readonly string[]): string[] {
  const used = new Set(priorWords.filter((word) => word.startsWith('-')).map(flagNameFromToken));
  const out = new Set<string>(GLOBAL_FLAG_TOKENS);
  for (const flag of matchedLeaf?.flags ?? []) {
    if (used.has(flag.name) && flag.multiple !== true) continue;
    out.add(`--${flag.name}`);
    if (flag.negatable === true) out.add(`--no-${flag.name}`);
    if (flag.alias !== undefined) out.add(`-${flag.alias}`);
  }
  return [...out];
}

/** The leading run of non-flag tokens — the command path a user has typed so far, if any. */
function leadingPath(words: readonly string[]): string[] {
  const path: string[] = [];
  for (const word of words) {
    if (word.startsWith('-')) break;
    path.push(word);
  }
  return path;
}

/**
 * Completes one shell word given every word typed so far (see `commands/meta/complete.ts` for how
 * a shell hands these in). Pure and synchronous: it only ever reads `commands`' own declared
 * shape — never a config file, an environment variable, or a broker — so a completion request
 * can't hang, fail, or leak a config value into a shell's completion menu.
 *
 * `words` is everything after `kafka` on the command line, including the (possibly empty) word
 * currently under the cursor as its last element.
 */
export function getCompletions(commands: readonly CommandSpec[], words: readonly string[]): string[] {
  const visible = commands.filter((command) => command.hidden !== true);
  const current = words.length > 0 ? (words[words.length - 1] ?? '') : '';
  const priorWords = words.slice(0, -1);
  const path = leadingPath(priorWords);
  const previous = priorWords[priorWords.length - 1];
  const matchedLeaf = visible.find((command) => command.path.join(' ') === path.join(' '));

  if (previous !== undefined && previous.startsWith('-')) {
    const values = enumValuesForFlag(previous, matchedLeaf);
    if (values !== undefined) {
      return values.filter((value) => value.startsWith(current)).sort();
    }
  }

  if (current.startsWith('-')) {
    return flagCandidates(matchedLeaf, priorWords)
      .filter((flag) => flag.startsWith(current))
      .sort();
  }

  return nextSegments(path, visible)
    .filter((segment) => segment.startsWith(current))
    .sort();
}
