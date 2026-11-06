export interface ConsoleCommand {
  readonly path: string;
  readonly description: string;
  readonly resource?: 'topic' | 'group';
  readonly usage: string;
}

/** The supported inspection subset of kafka-cli syntax. No shell or arbitrary Admin dispatch. */
export const CONSOLE_COMMANDS: readonly ConsoleCommand[] = [
  { path: 'help', description: 'Show available commands and keyboard shortcuts', usage: 'help' },
  { path: 'cluster info', description: 'Inspect the cluster connection', usage: 'cluster info' },
  { path: 'topic list', description: 'List topics and partition counts', usage: 'topic list' },
  {
    path: 'topic describe',
    description: 'Inspect partitions, offsets and configuration',
    resource: 'topic',
    usage: 'topic describe <topic>',
  },
  {
    path: 'topic offsets',
    description: 'Show earliest and latest partition offsets',
    resource: 'topic',
    usage: 'topic offsets <topic>',
  },
  {
    path: 'config describe',
    description: 'Read topic configuration',
    resource: 'topic',
    usage: 'config describe --type topic <topic>',
  },
  { path: 'group list', description: 'List consumer groups', usage: 'group list' },
  {
    path: 'group describe',
    description: 'Inspect group state, members and lag',
    resource: 'group',
    usage: 'group describe <group>',
  },
  {
    path: 'group offsets',
    description: 'Read committed offsets and partition lag',
    resource: 'group',
    usage: 'group offsets <group>',
  },
  { path: 'acl list', description: 'List access-control entries', usage: 'acl list' },
  { path: 'quota describe', description: 'Inspect client quotas', usage: 'quota describe' },
  { path: 'txn list', description: 'List transactions', usage: 'txn list' },
];

interface Token {
  readonly value: string;
  readonly start: number;
  readonly end: number;
}

function tokenize(line: string, strict: boolean): Token[] {
  const tokens: Token[] = [];
  let value = '';
  let start = -1;
  let quote: string | undefined;
  let escaped = false;
  for (let index = 0; index < line.length; index++) {
    const char = line[index] ?? '';
    if (start < 0 && /\s/.test(char)) continue;
    if (start < 0) start = index;
    if (escaped) {
      value += char;
      escaped = false;
    } else if (char === '\\' && quote !== "'") {
      escaped = true;
    } else if (quote !== undefined) {
      if (char === quote) quote = undefined;
      else value += char;
    } else if (char === '"' || char === "'") {
      quote = char;
    } else if (/\s/.test(char)) {
      tokens.push({ value, start, end: index });
      value = '';
      start = -1;
    } else {
      value += char;
    }
  }
  if (strict && (quote !== undefined || escaped))
    throw new Error('Close the quoted value or finish the escape before running.');
  if (start >= 0) tokens.push({ value, start, end: line.length });
  return tokens.map((token) => {
    const raw = line.slice(token.start, token.end);
    if (raw.startsWith('"') && raw.endsWith('"')) {
      try {
        const decoded: unknown = JSON.parse(raw);
        if (typeof decoded === 'string') return { ...token, value: decoded };
      } catch {
        // Non-JSON shell-style quotes still use the tokenizer's escape rules.
      }
    }
    return token;
  });
}

export interface ParsedConsoleCommand {
  readonly command: ConsoleCommand;
  readonly resource: string | undefined;
  readonly help: boolean;
}

export function parseConsoleCommand(line: string): ParsedConsoleCommand {
  const words = tokenize(line, true).map((token) => token.value);
  if (words[0] === 'kafka') words.shift();
  const delimiter = words.indexOf('--');
  const options = delimiter < 0 ? words : words.slice(0, delimiter);
  const literals = delimiter < 0 ? [] : words.slice(delimiter + 1);
  const help = options.includes('--help');
  const args = options.filter((word) => word !== '--help');
  const command = CONSOLE_COMMANDS.find(
    (candidate) => candidate.path === args.slice(0, candidate.path.split(' ').length).join(' '),
  );
  if (command === undefined) throw new Error('Unknown command. Run help to see the supported read-only commands.');
  const rest = args.slice(command.path.split(' ').length);
  if (help) return { command, resource: undefined, help: true };
  if (command.path === 'config describe') {
    const typeIndex = rest.indexOf('--type');
    if (typeIndex < 0 || rest[typeIndex + 1] !== 'topic') throw new Error(`Usage: ${command.usage}`);
    rest.splice(typeIndex, 2);
  }
  if (rest.some((word) => word.startsWith('--'))) {
    throw new Error(`Usage: ${command.usage}. Only the documented arguments are supported.`);
  }
  rest.push(...literals);
  if (rest.length !== (command.resource === undefined ? 0 : 1) || rest.some((word) => word.length === 0)) {
    throw new Error(`Usage: ${command.usage}. Only the documented arguments are supported.`);
  }
  return { command, resource: rest[0], help: false };
}

export interface ConsoleResources {
  readonly topics: readonly string[];
  readonly groups: readonly string[];
}

export interface ConsoleSuggestion {
  readonly label: string;
  readonly description: string;
  readonly value: string;
  readonly start: number;
  readonly end: number;
}

function quoteResource(value: string): string {
  return /^[a-zA-Z0-9._-]+$/.test(value) ? value : JSON.stringify(value);
}

/** Complete the word at the caret, retaining arguments to its right when accepting a suggestion. */
export function suggestConsoleCommands(line: string, cursor: number, resources: ConsoleResources): ConsoleSuggestion[] {
  const prefix = line.slice(0, cursor);
  const tokens = tokenize(prefix, false);
  const last = tokens.at(-1);
  const editing = last !== undefined && last.end === cursor;
  const current = editing ? last.value : '';
  const prior = (editing ? tokens.slice(0, -1) : tokens).map((token) => token.value);
  if (prior[0] === 'kafka') prior.shift();
  const start = editing ? last.start : cursor;
  const end = tokenize(line, false).find((token) => token.start === start)?.end ?? cursor;
  const candidates = new Map<string, { description: string; value: string }>();
  for (const command of CONSOLE_COMMANDS) {
    const path = command.path.split(' ');
    if (prior.every((word, index) => word === path[index])) {
      const next = path[prior.length];
      if (next !== undefined) candidates.set(next, { description: command.description, value: next });
    }
  }
  const command = CONSOLE_COMMANDS.find(
    (candidate) => prior.slice(0, candidate.path.split(' ').length).join(' ') === candidate.path,
  );
  if (command !== undefined) {
    const args = prior.slice(command.path.split(' ').length);
    let positional = args;
    if (command.path === 'config describe') {
      const typeIndex = args.indexOf('--type');
      if (args.at(-1) === '--type') candidates.set('topic', { description: 'Topic configuration', value: 'topic' });
      if (typeIndex < 0) candidates.set('--type', { description: 'Resource type (topic)', value: '--type' });
      positional = args.filter((_word, index) => index !== typeIndex && index !== typeIndex + 1);
      if (typeIndex < 0) positional = args;
    }
    positional = positional.filter((word) => word !== '--');
    if (command.resource !== undefined && positional.length === 0 && args.at(-1) !== '--type') {
      const names = command.resource === 'topic' ? resources.topics : resources.groups;
      for (const name of names)
        candidates.set(name, {
          description: command.resource === 'topic' ? 'Topic' : 'Consumer group',
          value: name.startsWith('--') && !args.includes('--') ? `-- ${quoteResource(name)}` : quoteResource(name),
        });
    }
    if (
      current.startsWith('-') &&
      !prior.includes('--help') &&
      !args.includes('--') &&
      args.at(-1) !== '--type' &&
      !candidates.has('--help')
    ) {
      candidates.set('--help', { description: 'Show command usage', value: '--help' });
    }
  }
  return [...candidates]
    .filter(([label]) => label.startsWith(current))
    .sort(([left], [right]) => left.localeCompare(right))
    .slice(0, 30)
    .map(([label, candidate]) => ({ label, ...candidate, start, end }));
}

export function acceptConsoleSuggestion(line: string, suggestion: ConsoleSuggestion): { line: string; cursor: number } {
  const suffix = line.slice(suggestion.end);
  const inserted = suggestion.value + (/^\s/.test(suffix) ? '' : ' ');
  return { line: line.slice(0, suggestion.start) + inserted + suffix, cursor: suggestion.start + inserted.length };
}
