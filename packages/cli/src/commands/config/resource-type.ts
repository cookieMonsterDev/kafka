import type { Admin } from '@cookiemonsterdev/kafka-core';
import { CliUsageError } from '../../args/coerce';
import { CONFIG_RESOURCE_TYPES } from '../../output/codes';

/**
 * The `type` field on a config resource query/entry is a closed numeric union, not a plain
 * `number` — derived from `Admin` itself so this stays in lockstep with core without importing a
 * type core doesn't actually export from its package root.
 */
export type ConfigResourceType = Parameters<Admin['describeConfigs']>[0]['resources'][number]['type'];

const SELECTABLE_TYPES = Object.entries(CONFIG_RESOURCE_TYPES).filter(([name]) => name !== 'UNKNOWN');

const NAME_TO_CODE: ReadonlyMap<string, number> = new Map(
  SELECTABLE_TYPES.map(([name, code]) => [name.toLowerCase().replaceAll('_', '-'), code]),
);
const VALID_CODES: ReadonlySet<number> = new Set(SELECTABLE_TYPES.map(([, code]) => code));

/**
 * `--type` accepts a case-insensitive resource-type name (`topic`, `broker-logger`, …, matching
 * the flag's own kebab-case convention) or the broker's raw numeric code. An unrecognized value is
 * a usage error naming every valid choice, never a silent fallback.
 */
export function resolveConfigResourceType(raw: string): ConfigResourceType {
  const byName = NAME_TO_CODE.get(raw.toLowerCase());
  if (byName !== undefined) return byName as ConfigResourceType;

  const asNumber = Number(raw);
  if (Number.isInteger(asNumber) && VALID_CODES.has(asNumber)) return asNumber as ConfigResourceType;

  throw new CliUsageError(
    `--type must be one of: ${[...NAME_TO_CODE.keys()].join(', ')} (or the matching numeric code), got "${raw}"`,
  );
}
