import type { KafkaPrincipal } from '@cookiemonsterdev/kafka-core';
import { CliUsageError } from '../../args/coerce';

/** Parses one `PrincipalType:name` flag value, e.g. `User:alice`. */
export function parsePrincipalFlag(raw: string, flagName: string): KafkaPrincipal {
  const separatorIndex = raw.indexOf(':');
  if (separatorIndex <= 0 || separatorIndex === raw.length - 1) {
    throw new CliUsageError(`--${flagName} must be "PrincipalType:name", got "${raw}"`);
  }
  return { principalType: raw.slice(0, separatorIndex), name: raw.slice(separatorIndex + 1) };
}

/** Parses every value of a repeatable `PrincipalType:name` flag. */
export function parsePrincipalFlags(
  raw: readonly string[] | undefined,
  flagName: string,
): KafkaPrincipal[] | undefined {
  if (raw === undefined) return undefined;
  return raw.map((entry) => parsePrincipalFlag(entry, flagName));
}
