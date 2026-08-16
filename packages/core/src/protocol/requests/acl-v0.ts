import { KafkaNonRetriableError } from '../../errors';
import { RESOURCE_PATTERN_TYPES } from '../enums/resource-pattern-types';

/**
 * ACL APIs v0 have no `resourcePatternType` on the wire (literal names only).
 * Prefixed patterns require v1 / Kafka 2.0+.
 */
export function assertNoPrefixedAclOnV0(resourcePatternType: number | undefined): void {
  if (resourcePatternType === RESOURCE_PATTERN_TYPES.PREFIXED) {
    throw new KafkaNonRetriableError(
      'Prefixed ACL resource patterns require ACL APIs v1 (Kafka 2.0+); this broker negotiated v0',
    );
  }
}
