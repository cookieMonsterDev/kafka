/** Read-only cluster inspection surfaces (phase 9): ACLs, client quotas, and in-flight transactions. */

export interface AclEntryView {
  readonly resourceType: string;
  readonly resourceName: string;
  readonly resourcePatternType: string;
  readonly principal: string;
  readonly host: string;
  readonly operation: string;
  readonly permissionType: string;
}

export interface AclListResponse {
  readonly acls: readonly AclEntryView[];
}

export interface QuotaEntityComponent {
  readonly entityType: string;
  readonly entityName: string | null;
}

export interface QuotaValue {
  readonly key: string;
  readonly value: number;
}

export interface QuotaEntryView {
  readonly entity: readonly QuotaEntityComponent[];
  readonly values: readonly QuotaValue[];
}

export interface QuotaListResponse {
  readonly quotas: readonly QuotaEntryView[];
}

export interface TransactionListEntry {
  readonly transactionalId: string;
  /** `bigint` on the server; serialized as a decimal string over the wire, matching every offset. */
  readonly producerId: string;
  readonly transactionState: string;
}

export interface TransactionListResponse {
  readonly transactions: readonly TransactionListEntry[];
}
