import type { Admin } from '@cookiemonsterdev/kafka-core';
import { CliUsageError } from '../../args/coerce';
import {
  ACL_OPERATION_TYPES,
  ACL_PERMISSION_TYPES,
  ACL_RESOURCE_TYPES,
  RESOURCE_PATTERN_TYPES,
} from '../../output/codes';

/**
 * Every ACL field is a closed numeric union, not a plain `number` — derived from `Admin` itself
 * so this stays in lockstep with core without importing a type core doesn't export from its
 * package root, matching `ConfigResourceType` in `../config/resource-type.ts`.
 */
type AclEntryFields = Parameters<Admin['createAcls']>[0]['acl'][number];
export type AclResourceType = AclEntryFields['resourceType'];
export type AclOperationType = AclEntryFields['operation'];
export type AclPermissionType = AclEntryFields['permissionType'];
export type AclResourcePatternType = AclEntryFields['resourcePatternType'];

/**
 * Builds a name/code resolver for one ACL enum table. `UNKNOWN` is a real code but never a
 * selectable one — matching `resolveConfigResourceType` — since naming it would only ever be a
 * mistake, never a legitimate filter or creation value.
 */
function createResolver<T extends number>(
  flagName: string,
  table: Readonly<Record<string, number>>,
): (raw: string) => T {
  const selectable = Object.entries(table).filter(([name]) => name !== 'UNKNOWN');
  const nameToCode: ReadonlyMap<string, number> = new Map(
    selectable.map(([name, code]) => [name.toLowerCase().replaceAll('_', '-'), code]),
  );
  const validCodes: ReadonlySet<number> = new Set(selectable.map(([, code]) => code));

  return function resolve(raw: string): T {
    const byName = nameToCode.get(raw.toLowerCase());
    if (byName !== undefined) return byName as T;

    const asNumber = Number(raw);
    if (Number.isInteger(asNumber) && validCodes.has(asNumber)) return asNumber as T;

    throw new CliUsageError(
      `--${flagName} must be one of: ${[...nameToCode.keys()].join(', ')} (or the matching numeric code), got "${raw}"`,
    );
  };
}

/** `--resource-type` — case-insensitive name (`topic`, `transactional-id`, …) or raw numeric code. */
export const resolveAclResourceType: (raw: string) => AclResourceType = createResolver<AclResourceType>(
  'resource-type',
  ACL_RESOURCE_TYPES,
);

/** `--pattern-type` — case-insensitive name (`literal`, `prefixed`, …) or raw numeric code. */
export const resolveAclPatternType: (raw: string) => AclResourcePatternType = createResolver<AclResourcePatternType>(
  'pattern-type',
  RESOURCE_PATTERN_TYPES,
);

/** `--operation` — case-insensitive name (`read`, `describe-configs`, …) or raw numeric code. */
export const resolveAclOperationType: (raw: string) => AclOperationType = createResolver<AclOperationType>(
  'operation',
  ACL_OPERATION_TYPES,
);

/** `--permission-type` — case-insensitive name (`allow`, `deny`, …) or raw numeric code. */
export const resolveAclPermissionType: (raw: string) => AclPermissionType = createResolver<AclPermissionType>(
  'permission-type',
  ACL_PERMISSION_TYPES,
);
