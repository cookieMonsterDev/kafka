import type { Admin } from '@cookiemonsterdev/kafka-core';
import { parseBrokersFlag } from '../../admin/parse-brokers';
import { CliUsageError } from '../../args/coerce';
import type { CommandSpec } from '../../args/define';
import { EXIT_CODES, exitForBatchResults } from '../../errors/exit-codes';
import { confirmDestructive } from '../../interaction/confirm';
import { stringifyJsonSafe } from '../../output/json';
import { renderTable } from '../../output/table';
import { mapWithConcurrency } from '../../concurrency';
import {
  resolveAclOperationType,
  resolveAclPatternType,
  resolveAclPermissionType,
  resolveAclResourceType,
  type AclOperationType,
  type AclPermissionType,
  type AclResourcePatternType,
  type AclResourceType,
} from './enums';

const CONCURRENCY = 8;

interface AclFilter {
  readonly principal: string;
  readonly host: string | undefined;
  readonly operation: AclOperationType;
  readonly permissionType: AclPermissionType;
  readonly resourceType: AclResourceType;
  readonly resourceName: string | undefined;
  readonly resourcePatternType: AclResourcePatternType;
}

interface AclResult {
  readonly principal: string;
  readonly ok: boolean;
  readonly matched: number;
  readonly detail?: string;
}

async function removeOne(admin: Admin, filter: AclFilter): Promise<AclResult> {
  const { filterResponses } = await admin.deleteAcls({ filters: [filter] });
  return { principal: filter.principal, ok: true, matched: filterResponses[0]?.matchingAcls.length ?? 0 };
}

export const aclRemoveCommand: CommandSpec = {
  path: ['acl', 'remove'],
  summary: 'Delete every ACL matching a filter, per principal',
  flags: [
    { name: 'brokers', type: 'string', brief: 'comma-separated broker list, e.g. localhost:9092' },
    { name: 'resource-type', type: 'string', brief: 'limit to this resource type (default: any)' },
    { name: 'resource-name', type: 'string', brief: 'limit to this exact resource name' },
    { name: 'pattern-type', type: 'string', brief: 'limit to this resource pattern type (default: any)' },
    { name: 'host', type: 'string', brief: 'limit to this host' },
    { name: 'operation', type: 'string', brief: 'limit to this operation (default: any)' },
    { name: 'permission-type', type: 'string', brief: 'limit to this permission type (default: any)' },
    { name: 'yes', type: 'boolean', brief: 'confirm the deletion without an interactive prompt' },
  ],
  positionals: [{ name: 'principals', variadic: true, brief: 'principals whose matching ACLs are removed' }],
  examples: ['acl remove User:alice --resource-type topic --resource-name orders --brokers localhost:9092 --yes'],
  exitCodes: [
    EXIT_CODES.ok,
    EXIT_CODES.operationFailed,
    EXIT_CODES.usage,
    EXIT_CODES.partialBatch,
    EXIT_CODES.abortedOrUnconfirmed,
  ],
  async run({ flags, positionals, runtime, output, config }) {
    if (positionals.length === 0) {
      throw new CliUsageError('acl remove requires at least one principal');
    }

    const resourceType = resolveAclResourceType((flags['resource-type'] as string | undefined) ?? 'any');
    const resourcePatternType = resolveAclPatternType((flags['pattern-type'] as string | undefined) ?? 'any');
    const operation = resolveAclOperationType((flags.operation as string | undefined) ?? 'any');
    const permissionType = resolveAclPermissionType((flags['permission-type'] as string | undefined) ?? 'any');
    const resourceName = flags['resource-name'] as string | undefined;
    const host = flags.host as string | undefined;

    const brokers = parseBrokersFlag(flags.brokers);
    const yes = flags.yes === true;

    await confirmDestructive({
      runtime,
      yes,
      message: `Delete ACLs for principal${positionals.length > 1 ? 's' : ''} ${positionals.join(', ')}?`,
      confirmDestructive: config.cli.confirmDestructive,
    });

    const filters: AclFilter[] = positionals.map((principal) => ({
      principal,
      host,
      operation,
      permissionType,
      resourceType,
      resourceName,
      resourcePatternType,
    }));

    const admin = await runtime.openAdmin({ brokers, env: runtime.env, config });
    try {
      let results: AclResult[];

      if (filters.length === 1) {
        results = [await removeOne(admin, filters[0]!)];
      } else {
        results = await mapWithConcurrency(filters, CONCURRENCY, async (filter) => {
          try {
            return await removeOne(admin, filter);
          } catch (error) {
            return {
              principal: filter.principal,
              ok: false,
              matched: 0,
              detail: error instanceof Error ? error.message : String(error),
            };
          }
        });
      }

      output.write({
        human: () =>
          renderTable(
            ['PRINCIPAL', 'STATUS'],
            results.map((r) => [r.principal, r.ok ? `removed (${r.matched})` : (r.detail ?? 'failed')]),
          ),
        json: () => stringifyJsonSafe({ results }),
      });

      return exitForBatchResults(results, (r) => r.ok);
    } finally {
      await admin.disconnect();
    }
  },
};
