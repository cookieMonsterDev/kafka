import { parseBrokersFlag } from '../../admin/parse-brokers';
import type { CommandSpec } from '../../args/define';
import { EXIT_CODES } from '../../errors/exit-codes';
import {
  ACL_OPERATION_TYPES,
  ACL_PERMISSION_TYPES,
  ACL_RESOURCE_TYPES,
  describeCode,
  formatCode,
  RESOURCE_PATTERN_TYPES,
} from '../../output/codes';
import { stringifyJsonSafe } from '../../output/json';
import { renderTable } from '../../output/table';
import {
  resolveAclOperationType,
  resolveAclPatternType,
  resolveAclPermissionType,
  resolveAclResourceType,
} from './enums';

export const aclListCommand: CommandSpec = {
  path: ['acl', 'list'],
  summary: 'List ACLs matching a filter, or every ACL by default',
  flags: [
    { name: 'brokers', type: 'string', brief: 'comma-separated broker list, e.g. localhost:9092' },
    { name: 'resource-type', type: 'string', brief: 'limit to this resource type (default: any)' },
    { name: 'resource-name', type: 'string', brief: 'limit to this exact resource name' },
    { name: 'pattern-type', type: 'string', brief: 'limit to this resource pattern type (default: any)' },
    { name: 'principal', type: 'string', brief: 'limit to this principal, e.g. User:alice' },
    { name: 'host', type: 'string', brief: 'limit to this host' },
    { name: 'operation', type: 'string', brief: 'limit to this operation (default: any)' },
    { name: 'permission-type', type: 'string', brief: 'limit to this permission type (default: any)' },
  ],
  examples: ['acl list --brokers localhost:9092', 'acl list --resource-type topic --resource-name orders'],
  exitCodes: [EXIT_CODES.ok, EXIT_CODES.operationFailed, EXIT_CODES.usage],
  async run({ flags, runtime, output, config }) {
    const brokers = parseBrokersFlag(flags.brokers);

    const resourceType = resolveAclResourceType((flags['resource-type'] as string | undefined) ?? 'any');
    const resourcePatternType = resolveAclPatternType((flags['pattern-type'] as string | undefined) ?? 'any');
    const operation = resolveAclOperationType((flags.operation as string | undefined) ?? 'any');
    const permissionType = resolveAclPermissionType((flags['permission-type'] as string | undefined) ?? 'any');

    const admin = await runtime.openAdmin({ brokers, env: runtime.env, config });
    try {
      const { resources } = await admin.describeAcls({
        resourceType,
        resourceName: flags['resource-name'] as string | undefined,
        resourcePatternType,
        principal: flags.principal as string | undefined,
        host: flags.host as string | undefined,
        operation,
        permissionType,
      });

      const rows = resources.flatMap((resource) =>
        resource.acls.map((acl) => ({
          resourceType: formatCode(describeCode(ACL_RESOURCE_TYPES, resource.resourceType)),
          resourceName: resource.resourceName,
          resourcePatternType: formatCode(describeCode(RESOURCE_PATTERN_TYPES, resource.resourcePatternType)),
          principal: acl.principal,
          host: acl.host,
          operation: formatCode(describeCode(ACL_OPERATION_TYPES, acl.operation)),
          permissionType: formatCode(describeCode(ACL_PERMISSION_TYPES, acl.permissionType)),
        })),
      );

      output.write({
        human: () =>
          rows.length === 0
            ? '(no matching ACLs)'
            : renderTable(
                ['RESOURCE_TYPE', 'RESOURCE_NAME', 'PATTERN_TYPE', 'PRINCIPAL', 'HOST', 'OPERATION', 'PERMISSION'],
                rows.map((row) => [
                  row.resourceType,
                  row.resourceName,
                  row.resourcePatternType,
                  row.principal,
                  row.host,
                  row.operation,
                  row.permissionType,
                ]),
              ),
        json: () => stringifyJsonSafe({ resources }),
      });

      return EXIT_CODES.ok;
    } finally {
      await admin.disconnect();
    }
  },
};
