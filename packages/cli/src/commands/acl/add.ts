import type { Admin } from '@cookiemonsterdev/kafka-core';
import { parseBrokersFlag } from '../../admin/parse-brokers';
import { CliUsageError } from '../../args/coerce';
import type { CommandSpec } from '../../args/define';
import { EXIT_CODES } from '../../errors/exit-codes';
import { stringifyJsonSafe } from '../../output/json';
import { renderTable } from '../../output/table';
import { mapWithConcurrency } from '../topic/concurrency';
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

interface AclEntry {
  readonly principal: string;
  readonly host: string;
  readonly operation: AclOperationType;
  readonly permissionType: AclPermissionType;
  readonly resourceType: AclResourceType;
  readonly resourceName: string;
  readonly resourcePatternType: AclResourcePatternType;
}

interface AclResult {
  readonly principal: string;
  readonly operation: string;
  readonly ok: boolean;
  readonly detail?: string;
}

async function addOne(admin: Admin, entry: AclEntry): Promise<AclResult> {
  await admin.createAcls({ acl: [entry] });
  return { principal: entry.principal, operation: entry.operation.toString(), ok: true };
}

export const aclAddCommand: CommandSpec = {
  path: ['acl', 'add'],
  summary: 'Create one or more ACLs, granting or denying an operation',
  flags: [
    { name: 'brokers', type: 'string', brief: 'comma-separated broker list, e.g. localhost:9092' },
    {
      name: 'resource-type',
      type: 'string',
      brief: 'resource type: topic, group, cluster, transactional-id, delegation-token',
    },
    { name: 'resource-name', type: 'string', brief: 'exact name of the resource' },
    { name: 'pattern-type', type: 'string', brief: 'resource pattern type: literal or prefixed (default: literal)' },
    { name: 'operation', type: 'string', multiple: true, brief: 'operation to grant (repeatable), e.g. read, write' },
    { name: 'host', type: 'string', brief: 'host the principal connects from (default: *)' },
    { name: 'permission-type', type: 'string', brief: 'allow or deny (default: allow)' },
    { name: 'dry-run', type: 'boolean', brief: 'print what would be created without creating anything' },
  ],
  positionals: [{ name: 'principals', variadic: true, brief: 'principals to grant, e.g. User:alice' }],
  examples: [
    'acl add User:alice --resource-type topic --resource-name orders --operation read --operation write',
    'acl add User:alice User:bob --resource-type group --resource-name my-group --operation read --dry-run',
  ],
  exitCodes: [EXIT_CODES.ok, EXIT_CODES.operationFailed, EXIT_CODES.usage, EXIT_CODES.partialBatch],
  async run({ flags, positionals, runtime, output, config }) {
    if (positionals.length === 0) {
      throw new CliUsageError('acl add requires at least one principal');
    }

    const resourceTypeFlag = flags['resource-type'] as string | undefined;
    if (resourceTypeFlag === undefined) {
      throw new CliUsageError('acl add requires --resource-type');
    }
    const resourceType = resolveAclResourceType(resourceTypeFlag);

    const resourceName = flags['resource-name'] as string | undefined;
    if (resourceName === undefined) {
      throw new CliUsageError('acl add requires --resource-name');
    }

    const operationFlags = (flags.operation as string[] | undefined) ?? [];
    if (operationFlags.length === 0) {
      throw new CliUsageError('acl add requires at least one --operation');
    }
    const operations = operationFlags.map(resolveAclOperationType);

    const resourcePatternType = resolveAclPatternType((flags['pattern-type'] as string | undefined) ?? 'literal');
    const permissionType = resolveAclPermissionType((flags['permission-type'] as string | undefined) ?? 'allow');
    const host = (flags.host as string | undefined) ?? '*';
    const dryRun = flags['dry-run'] === true;

    const entries: AclEntry[] = positionals.flatMap((principal) =>
      operations.map((operation) => ({
        principal,
        host,
        operation,
        permissionType,
        resourceType,
        resourceName,
        resourcePatternType,
      })),
    );

    if (dryRun) {
      output.write({
        human: () =>
          renderTable(
            ['PRINCIPAL', 'OPERATION', 'STATUS'],
            entries.map((entry) => [entry.principal, entry.operation.toString(), 'validated']),
          ),
        json: () => stringifyJsonSafe({ entries }),
      });
      return EXIT_CODES.ok;
    }

    const brokers = parseBrokersFlag(flags.brokers);
    const admin = await runtime.openAdmin({ brokers, env: runtime.env, config });
    try {
      let results: AclResult[];

      if (entries.length === 1) {
        results = [await addOne(admin, entries[0]!)];
      } else {
        results = await mapWithConcurrency(entries, CONCURRENCY, async (entry) => {
          try {
            return await addOne(admin, entry);
          } catch (error) {
            return {
              principal: entry.principal,
              operation: entry.operation.toString(),
              ok: false,
              detail: error instanceof Error ? error.message : String(error),
            };
          }
        });
      }

      output.write({
        human: () =>
          renderTable(
            ['PRINCIPAL', 'OPERATION', 'STATUS'],
            results.map((r) => [r.principal, r.operation, r.ok ? 'ok' : (r.detail ?? 'failed')]),
          ),
        json: () => stringifyJsonSafe({ results }),
      });

      const okCount = results.filter((r) => r.ok).length;
      if (okCount === results.length) return EXIT_CODES.ok;
      if (okCount === 0) return EXIT_CODES.operationFailed;
      return EXIT_CODES.partialBatch;
    } finally {
      await admin.disconnect();
    }
  },
};
