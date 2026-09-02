import type { FeatureUpdateUpgradeType } from '@cookiemonsterdev/kafka-core';
import { CliUsageError } from '../../args/coerce';
import { FEATURE_UPDATE_UPGRADE_TYPES } from '../../output/codes';

/**
 * Builds a name/code resolver for one numeric enum table, mirroring `acl/enums.ts`'s
 * `createResolver` — kept as an independent copy rather than a shared export, matching how
 * `config/resource-type.ts` and `acl/enums.ts` each already carry their own.
 */
function createResolver<T extends number>(
  flagName: string,
  table: Readonly<Record<string, number>>,
): (raw: string) => T {
  const nameToCode: ReadonlyMap<string, number> = new Map(
    Object.entries(table).map(([name, code]) => [name.toLowerCase().replaceAll('_', '-'), code]),
  );
  const validCodes: ReadonlySet<number> = new Set(Object.values(table));

  return function resolve(raw: string): T {
    const byName = nameToCode.get(raw.toLowerCase());
    if (byName !== undefined) return byName as T;

    // `Number('')` (and `Number('   ')`) is `0`, not `NaN` — guard explicitly so an empty or
    // blank value never silently resolves to whichever valid code happens to be `0` (e.g.
    // `ELECTION_TYPES.PREFERRED`) instead of raising a usage error.
    if (raw.trim() !== '') {
      const asNumber = Number(raw);
      if (Number.isInteger(asNumber) && validCodes.has(asNumber)) return asNumber as T;
    }

    throw new CliUsageError(
      `--${flagName} must be one of: ${[...nameToCode.keys()].join(', ')} (or the matching numeric code), got "${raw}"`,
    );
  };
}

/** `--upgrade-type` — case-insensitive name (`upgrade`, `safe-downgrade`, `unsafe-downgrade`) or raw numeric code. */
export const resolveFeatureUpgradeType: (raw: string) => FeatureUpdateUpgradeType =
  createResolver<FeatureUpdateUpgradeType>('upgrade-type', FEATURE_UPDATE_UPGRADE_TYPES);

/**
 * `election_type` (0 preferred, 1 unclean) has no name table exported anywhere in core — verified
 * against `packages/core/src/protocol/requests/elect-leaders/v1/request.ts`, which hand-documents
 * the wire values directly rather than through a shared enum module like `AclOperationType` or
 * `ConfigResourceType`. So, unlike every other table in this file (and in `output/codes.ts`),
 * this one has no "deep-equals core's real export" test backing it — it is checked here against
 * the protocol doc comment instead, and would need updating by hand if core's wire format ever
 * changed name for these two values.
 */
const ELECTION_TYPES = Object.freeze({ PREFERRED: 0, UNCLEAN: 1 });

/** `--election-type` — `preferred` or `unclean` (or the matching numeric code: `0`/`1`). */
export const resolveElectionType: (raw: string) => number = createResolver('election-type', ELECTION_TYPES);
