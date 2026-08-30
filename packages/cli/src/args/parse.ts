import { parseArgs } from 'node:util';
import type { FlagSpec, PositionalSpec } from './define';
import { CliUsageError, coerceEnum, coerceKeyValueRecord, coerceNumber } from './coerce';

export interface ParsedCommandArgs {
  readonly flags: Record<string, unknown>;
  readonly positionals: string[];
}

/** Node's `parseArgs` has no built-in `--no-x` negation, so it's handled as a pre-pass. */
function extractNegations(
  argv: readonly string[],
  flags: readonly FlagSpec[],
): { rest: string[]; negated: Set<string> } {
  const negatable = new Set(flags.filter((flag) => flag.negatable === true).map((flag) => flag.name));
  const negated = new Set<string>();
  const rest: string[] = [];
  let sawTerminator = false;

  for (const token of argv) {
    if (sawTerminator) {
      rest.push(token);
      continue;
    }
    if (token === '--') {
      sawTerminator = true;
      rest.push(token);
      continue;
    }
    if (token.startsWith('--no-')) {
      const name = token.slice('--no-'.length);
      if (negatable.has(name)) {
        negated.add(name);
        continue;
      }
    }
    rest.push(token);
  }

  return { rest, negated };
}

function toParseArgsOptions(
  flags: readonly FlagSpec[],
): Record<string, { type: 'string' | 'boolean'; short?: string; multiple?: boolean }> {
  const options: Record<string, { type: 'string' | 'boolean'; short?: string; multiple?: boolean }> = {};
  for (const flag of flags) {
    options[flag.name] = {
      type: flag.type === 'boolean' ? 'boolean' : 'string',
      ...(flag.alias !== undefined ? { short: flag.alias } : {}),
      ...(flag.multiple === true ? { multiple: true } : {}),
    };
  }
  return options;
}

function coerceValue(flag: FlagSpec, raw: unknown): unknown {
  if (flag.type === 'boolean') return raw;

  if (flag.keyValue === true) {
    const values = Array.isArray(raw) ? (raw as string[]) : raw === undefined ? [] : [raw as string];
    return coerceKeyValueRecord(values, flag.name);
  }

  if (flag.multiple === true) {
    const values = Array.isArray(raw) ? (raw as string[]) : [];
    return values.map((value) => coerceScalar(flag, value));
  }

  if (raw === undefined) return undefined;
  return coerceScalar(flag, raw as string);
}

function coerceScalar(flag: FlagSpec, raw: string): unknown {
  if (flag.type === 'number') return coerceNumber(raw, flag.name);
  if (flag.type === 'enum') return coerceEnum(raw, flag.name, flag.values ?? []);
  return raw;
}

/**
 * Parses one command's argv slice (everything after the command path) against its declared
 * flags and positionals. Throws {@link CliUsageError} for anything a user got wrong — an unknown
 * flag, a bad enum value, a malformed number — never anything else.
 */
export function parseCommandArgs(
  argv: readonly string[],
  flags: readonly FlagSpec[],
  positionals: readonly PositionalSpec[] = [],
): ParsedCommandArgs {
  const { rest, negated } = extractNegations(argv, flags);

  let raw: ReturnType<typeof parseArgs>;
  try {
    raw = parseArgs({
      args: rest,
      options: toParseArgsOptions(flags),
      allowPositionals: true,
      strict: true,
    });
  } catch (error) {
    throw new CliUsageError(error instanceof Error ? error.message : String(error));
  }

  const result: Record<string, unknown> = {};
  for (const flag of flags) {
    if (flag.type === 'boolean' && negated.has(flag.name)) {
      result[flag.name] = false;
      continue;
    }
    const value = coerceValue(flag, raw.values[flag.name]);
    if (value !== undefined) result[flag.name] = value;
  }

  const positionalValues = raw.positionals;
  if (positionals.length > 0) {
    const variadicIndex = positionals.findIndex((spec) => spec.variadic === true);
    const fixedCount = variadicIndex === -1 ? positionals.length : variadicIndex;
    if (positionalValues.length < fixedCount) {
      const missing = positionals[positionalValues.length];
      throw new CliUsageError(`missing required argument <${missing?.name ?? 'argument'}>`);
    }
    if (variadicIndex === -1 && positionalValues.length > fixedCount) {
      throw new CliUsageError(`unexpected argument "${positionalValues[fixedCount] ?? ''}"`);
    }
  }

  return { flags: result, positionals: positionalValues };
}
