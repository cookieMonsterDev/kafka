import type { Admin } from '@cookiemonsterdev/kafka-core';
import { CliUsageError } from '../../args/coerce';

/**
 * Derived from `Admin` itself, matching `AclResourceType` in `../acl/enums.ts` — core does not
 * export `AlterClientQuotasEntity` from its package root, so this stays in lockstep without a
 * static value import.
 */
type AlterClientQuotasEntry = Parameters<Admin['alterClientQuotas']>[0]['entries'][number];
type AlterClientQuotasEntity = AlterClientQuotasEntry['entity'][number];

export interface QuotaComponent {
  entityType: string;
  matchType: number;
  match: string | null;
}

/**
 * KIP-546's `QuotaFilterComponent` match types — not a named export in core, since they are a
 * filter concept with no corresponding value in a quota entity itself (unlike, say, an ACL
 * operation code, which core's wire types actually carry).
 */
const MATCH_TYPES = Object.freeze({ EXACT: 0, DEFAULT: 1, ANY_SPECIFIED_NAME: 2 });

/**
 * Builds an alter-time entity from `--entity type=name` (repeatable, `key=value`). An empty name
 * (`--entity user=`) targets that entity type's cluster-default rather than one specific name.
 */
export function toAlterQuotaEntity(entries: Readonly<Record<string, string>> | undefined): AlterClientQuotasEntity[] {
  if (entries === undefined || Object.keys(entries).length === 0) {
    throw new CliUsageError('requires at least one --entity type=name');
  }
  return Object.entries(entries).map(([entityType, entityName]) => ({
    entityType,
    entityName: entityName === '' ? null : entityName,
  }));
}

/**
 * Builds describe-time filter components from `--entity type=name` (exact, or default when
 * `name` is empty) and `--entity-any type` (any specified name for that type).
 */
export function toDescribeQuotaComponents(
  entries: Readonly<Record<string, string>> | undefined,
  anyEntityTypes: readonly string[] | undefined,
): QuotaComponent[] {
  const components: QuotaComponent[] = [];
  if (entries !== undefined) {
    for (const [entityType, entityName] of Object.entries(entries)) {
      components.push(
        entityName === ''
          ? { entityType, matchType: MATCH_TYPES.DEFAULT, match: null }
          : { entityType, matchType: MATCH_TYPES.EXACT, match: entityName },
      );
    }
  }
  if (anyEntityTypes !== undefined) {
    for (const entityType of anyEntityTypes) {
      components.push({ entityType, matchType: MATCH_TYPES.ANY_SPECIFIED_NAME, match: null });
    }
  }
  return components;
}
