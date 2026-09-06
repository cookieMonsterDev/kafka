import {
  AclOperationTypes,
  AclPermissionTypes,
  AclResourceTypes,
  ResourcePatternTypes,
} from '@cookiemonsterdev/kafka-core';
import type { AclListResponse, QuotaListResponse, TransactionListResponse } from '../../shared/contracts/settings';
import { sendJson } from '../create-server';
import type { AdminPool } from '../kafka/admin-pool';
import { isSecurityDisabledError } from '../kafka/protocol-error';
import type { Router } from '../router';

export interface SettingsRouteContext {
  readonly pool: AdminPool;
  getActiveProfile(): string | null;
}

/** Reverses one of core's frozen numeric-code tables (`{ NAME: code }`) back to the name for a given code. */
function describeCode(table: Readonly<Record<string, number>>, code: number): string {
  for (const [name, value] of Object.entries(table)) {
    if (value === code) return name;
  }
  return `UNKNOWN(${String(code)})`;
}

/** Read-only inspection views: every ACL, every client quota override, and every transaction the cluster currently knows about. */
export function registerSettingsRoutes(router: Router, context: SettingsRouteContext): void {
  router.get('/api/acls', async (_req, res) => {
    const admin = await context.pool.get(context.getActiveProfile());

    let resources: Awaited<ReturnType<typeof admin.describeAcls>>['resources'];
    try {
      ({ resources } = await admin.describeAcls({
        resourceType: AclResourceTypes.ANY,
        resourcePatternType: ResourcePatternTypes.ANY,
        operation: AclOperationTypes.ANY,
        permissionType: AclPermissionTypes.ANY,
      }));
    } catch (error) {
      // No `authorizer.class.name` configured — the common case for a non-production cluster,
      // not a studio-side failure. "No ACLs" is the accurate answer, not a 500.
      if (!isSecurityDisabledError(error)) throw error;
      resources = [];
    }

    const response: AclListResponse = {
      acls: resources.flatMap((resource) =>
        resource.acls.map((acl) => ({
          resourceType: describeCode(AclResourceTypes, resource.resourceType),
          resourceName: resource.resourceName,
          resourcePatternType: describeCode(ResourcePatternTypes, resource.resourcePatternType),
          principal: acl.principal,
          host: acl.host,
          operation: describeCode(AclOperationTypes, acl.operation),
          permissionType: describeCode(AclPermissionTypes, acl.permissionType),
        })),
      ),
    };
    sendJson(res, 200, response);
  });

  router.get('/api/quotas', async (_req, res) => {
    const admin = await context.pool.get(context.getActiveProfile());
    const { entries } = await admin.describeClientQuotas();

    const response: QuotaListResponse = {
      quotas: entries.map((entry) => ({
        entity: entry.entity.map((component) => ({
          entityType: component.entityType,
          entityName: component.entityName,
        })),
        values: entry.values.map((value) => ({ key: value.key, value: value.value })),
      })),
    };
    sendJson(res, 200, response);
  });

  router.get('/api/transactions', async (_req, res) => {
    const admin = await context.pool.get(context.getActiveProfile());
    const { transactionStates } = await admin.listTransactions();

    const response: TransactionListResponse = {
      transactions: transactionStates.map((transaction) => ({
        transactionalId: transaction.transactionalId,
        producerId: transaction.producerId.toString(),
        transactionState: transaction.transactionState,
      })),
    };
    sendJson(res, 200, response);
  });
}
