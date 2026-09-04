/** Raised for any user-facing argument mistake — always maps to the usage exit code. */
export class CliUsageError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'CliUsageError';
  }
}

export function coerceNumber(raw: string, flagName: string): number {
  const value = Number(raw);
  if (raw.trim() === '' || Number.isNaN(value)) {
    throw new CliUsageError(`--${flagName} expects a number, got "${raw}"`);
  }
  return value;
}

export function coerceBigInt(raw: string, flagName: string): bigint {
  try {
    return BigInt(raw);
  } catch {
    throw new CliUsageError(`--${flagName} expects an integer, got "${raw}"`);
  }
}

export function coerceEnum<T extends string>(raw: string, flagName: string, values: readonly T[]): T {
  if ((values as readonly string[]).includes(raw)) return raw as T;
  throw new CliUsageError(`--${flagName} must be one of: ${values.join(', ')} (got "${raw}")`);
}

/** Splits one `key=value` entry, as used by repeated `--config k=v` flags. */
export function splitKeyValue(raw: string, flagName: string): [key: string, value: string] {
  const index = raw.indexOf('=');
  if (index <= 0) {
    throw new CliUsageError(`--${flagName} expects "key=value", got "${raw}"`);
  }
  return [raw.slice(0, index), raw.slice(index + 1)];
}

/** Decodes every `key=value` entry from a repeated flag into a plain record. */
export function coerceKeyValueRecord(raw: readonly string[], flagName: string): Record<string, string> {
  const record: Record<string, string> = {};
  for (const entry of raw) {
    const [key, value] = splitKeyValue(entry, flagName);
    record[key] = value;
  }
  return record;
}
