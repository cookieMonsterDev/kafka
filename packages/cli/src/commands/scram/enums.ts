import { CliUsageError } from '../../args/coerce';
import { SCRAM_MECHANISMS } from '../../output/codes';

/**
 * Builds a name/code resolver for one enum table. Kept as an independent copy rather than a
 * shared export, matching how `acl/enums.ts` and `cluster/enums.ts` each already carry their own.
 * `UNKNOWN` is a real code but never a selectable one, since naming it would only ever be a
 * mistake, never a legitimate mechanism.
 */
function createResolver(flagName: string, table: Readonly<Record<string, number>>): (raw: string) => number {
  const selectable = Object.entries(table).filter(([name]) => name !== 'UNKNOWN');
  const nameToCode: ReadonlyMap<string, number> = new Map(
    selectable.map(([name, code]) => [name.toLowerCase().replaceAll('_', '-'), code]),
  );
  const validCodes: ReadonlySet<number> = new Set(selectable.map(([, code]) => code));

  return function resolve(raw: string): number {
    const byName = nameToCode.get(raw.toLowerCase());
    if (byName !== undefined) return byName;

    const asNumber = Number(raw);
    if (Number.isInteger(asNumber) && validCodes.has(asNumber)) return asNumber;

    throw new CliUsageError(
      `--${flagName} must be one of: ${[...nameToCode.keys()].join(', ')} (or the matching numeric code), got "${raw}"`,
    );
  };
}

/** `--mechanism` — case-insensitive name (`scram-sha-256`, `scram-sha-512`) or raw numeric code. */
export const resolveScramMechanism = createResolver('mechanism', SCRAM_MECHANISMS);
