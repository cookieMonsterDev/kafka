import { KafkaNonRetriableError } from '../errors';
import { ACL_OPERATION_TYPES } from '../protocol/enums/acl-operation-types';
import { ACL_PERMISSION_TYPES } from '../protocol/enums/acl-permission-types';
import { ACL_RESOURCE_TYPES } from '../protocol/enums/acl-resource-types';
import { RESOURCE_PATTERN_TYPES } from '../protocol/enums/resource-pattern-types';
import type { DescribeAclsResponseV1Body } from '../protocol/requests/describe-acls/v1/response';
import type { DeleteAclsResponseV1Body } from '../protocol/requests/delete-acls/v1/response';
import { retrier } from '../retry/index';
import type { AdminContext } from './helpers';
import { protocolType, formatUnknown } from './helpers';
import type { AclEntry, AclFilter } from './types';

export interface AclsApi {
  createAcls: (options: { acl: AclEntry[] }) => Promise<boolean>;
  describeAcls: (options: AclFilter) => Promise<{ resources: DescribeAclsResponseV1Body['resources'] }>;
  deleteAcls: (options: { filters: AclFilter[] }) => Promise<{
    filterResponses: DeleteAclsResponseV1Body['filterResponses'];
  }>;
}

const VALID_OPERATION_TYPES = Object.values(ACL_OPERATION_TYPES);
const VALID_PERMISSION_TYPES = Object.values(ACL_PERMISSION_TYPES);
const VALID_RESOURCE_TYPES = Object.values(ACL_RESOURCE_TYPES);
const VALID_RESOURCE_PATTERN_TYPES = Object.values(RESOURCE_PATTERN_TYPES);

function optionalStringOrUndefined(value: unknown): boolean {
  return typeof value === 'string' || typeof value === 'undefined';
}

export function createAclsApi({ cluster, logger, retry }: AdminContext): AclsApi {
  const createAcls = async ({ acl }: { acl: AclEntry[] }): Promise<boolean> => {
    if (!acl || !Array.isArray(acl)) {
      throw new KafkaNonRetriableError(`Invalid ACL array ${formatUnknown(acl)}`);
    }
    if (acl.length === 0) {
      throw new KafkaNonRetriableError('Empty ACL array');
    }

    if (acl.some(({ principal }) => typeof principal !== 'string')) {
      throw new KafkaNonRetriableError('Invalid ACL array, the principals have to be a valid string');
    }

    if (acl.some(({ host }) => typeof host !== 'string')) {
      throw new KafkaNonRetriableError('Invalid ACL array, the hosts have to be a valid string');
    }

    if (acl.some(({ resourceName }) => typeof resourceName !== 'string')) {
      throw new KafkaNonRetriableError('Invalid ACL array, the resourceNames have to be a valid string');
    }

    const invalidOperation = acl.find((entry) => !VALID_OPERATION_TYPES.includes(entry.operation));
    if (invalidOperation) {
      throw new KafkaNonRetriableError(
        `Invalid operation type ${invalidOperation.operation}: ${JSON.stringify(invalidOperation)}`,
      );
    }

    const invalidPattern = acl.find((entry) => !VALID_RESOURCE_PATTERN_TYPES.includes(entry.resourcePatternType));
    if (invalidPattern) {
      throw new KafkaNonRetriableError(
        `Invalid resource pattern type ${invalidPattern.resourcePatternType}: ${JSON.stringify(invalidPattern)}`,
      );
    }

    const invalidPermission = acl.find((entry) => !VALID_PERMISSION_TYPES.includes(entry.permissionType));
    if (invalidPermission) {
      throw new KafkaNonRetriableError(
        `Invalid permission type ${invalidPermission.permissionType}: ${JSON.stringify(invalidPermission)}`,
      );
    }

    const invalidResource = acl.find((entry) => !VALID_RESOURCE_TYPES.includes(entry.resourceType));
    if (invalidResource) {
      throw new KafkaNonRetriableError(
        `Invalid resource type ${invalidResource.resourceType}: ${JSON.stringify(invalidResource)}`,
      );
    }

    return retrier(retry)(async (bail, retryCount, retryTime) => {
      try {
        await cluster.refreshMetadata();
        const broker = await cluster.findControllerBroker();
        await broker.createAcls({ creations: acl });
        return true;
      } catch (error) {
        if (protocolType(error) === 'NOT_CONTROLLER') {
          logger.warn('Could not create ACL', {
            error: error instanceof Error ? error.message : String(error),
            retryCount,
            retryTime,
          });
          throw error;
        }

        bail(error as Error);
        return false;
      }
    });
  };

  const describeAcls = async ({
    resourceType,
    resourceName,
    resourcePatternType,
    principal,
    host,
    operation,
    permissionType,
  }: AclFilter): Promise<{ resources: DescribeAclsResponseV1Body['resources'] }> => {
    if (!optionalStringOrUndefined(principal)) {
      throw new KafkaNonRetriableError('Invalid principal, the principal have to be a valid string');
    }

    if (!optionalStringOrUndefined(host)) {
      throw new KafkaNonRetriableError('Invalid host, the host have to be a valid string');
    }

    if (!optionalStringOrUndefined(resourceName)) {
      throw new KafkaNonRetriableError('Invalid resourceName, the resourceName have to be a valid string');
    }

    if (!VALID_OPERATION_TYPES.includes(operation)) {
      throw new KafkaNonRetriableError(`Invalid operation type ${operation}`);
    }

    if (!VALID_RESOURCE_PATTERN_TYPES.includes(resourcePatternType)) {
      throw new KafkaNonRetriableError(`Invalid resource pattern filter type ${resourcePatternType}`);
    }

    if (!VALID_PERMISSION_TYPES.includes(permissionType)) {
      throw new KafkaNonRetriableError(`Invalid permission type ${permissionType}`);
    }

    if (!VALID_RESOURCE_TYPES.includes(resourceType)) {
      throw new KafkaNonRetriableError(`Invalid resource type ${resourceType}`);
    }

    return retrier(retry)(async (bail, retryCount, retryTime) => {
      try {
        await cluster.refreshMetadata();
        const broker = await cluster.findControllerBroker();
        const { resources } = await broker.describeAcls({
          resourceType,
          resourceName: resourceName ?? null,
          resourcePatternType,
          principal: principal ?? null,
          host: host ?? null,
          operation,
          permissionType,
        });
        return { resources };
      } catch (error) {
        if (protocolType(error) === 'NOT_CONTROLLER') {
          logger.warn('Could not describe ACL', {
            error: error instanceof Error ? error.message : String(error),
            retryCount,
            retryTime,
          });
          throw error;
        }

        bail(error as Error);
        return { resources: [] };
      }
    });
  };

  const deleteAcls = async ({
    filters,
  }: {
    filters: AclFilter[];
  }): Promise<{ filterResponses: DeleteAclsResponseV1Body['filterResponses'] }> => {
    if (!filters || !Array.isArray(filters)) {
      throw new KafkaNonRetriableError(`Invalid ACL Filter array ${formatUnknown(filters)}`);
    }

    if (filters.length === 0) {
      throw new KafkaNonRetriableError('Empty ACL Filter array');
    }

    if (filters.some(({ principal }) => !optionalStringOrUndefined(principal))) {
      throw new KafkaNonRetriableError('Invalid ACL Filter array, the principals have to be a valid string');
    }

    if (filters.some(({ host }) => !optionalStringOrUndefined(host))) {
      throw new KafkaNonRetriableError('Invalid ACL Filter array, the hosts have to be a valid string');
    }

    if (filters.some(({ resourceName }) => !optionalStringOrUndefined(resourceName))) {
      throw new KafkaNonRetriableError('Invalid ACL Filter array, the resourceNames have to be a valid string');
    }

    const invalidOperation = filters.find((entry) => !VALID_OPERATION_TYPES.includes(entry.operation));
    if (invalidOperation) {
      throw new KafkaNonRetriableError(
        `Invalid operation type ${invalidOperation.operation}: ${JSON.stringify(invalidOperation)}`,
      );
    }

    const invalidPattern = filters.find((entry) => !VALID_RESOURCE_PATTERN_TYPES.includes(entry.resourcePatternType));
    if (invalidPattern) {
      throw new KafkaNonRetriableError(
        `Invalid resource pattern type ${invalidPattern.resourcePatternType}: ${JSON.stringify(invalidPattern)}`,
      );
    }

    const invalidPermission = filters.find((entry) => !VALID_PERMISSION_TYPES.includes(entry.permissionType));
    if (invalidPermission) {
      throw new KafkaNonRetriableError(
        `Invalid permission type ${invalidPermission.permissionType}: ${JSON.stringify(invalidPermission)}`,
      );
    }

    const invalidResource = filters.find((entry) => !VALID_RESOURCE_TYPES.includes(entry.resourceType));
    if (invalidResource) {
      throw new KafkaNonRetriableError(
        `Invalid resource type ${invalidResource.resourceType}: ${JSON.stringify(invalidResource)}`,
      );
    }

    return retrier(retry)(async (bail, retryCount, retryTime) => {
      try {
        await cluster.refreshMetadata();
        const broker = await cluster.findControllerBroker();
        const { filterResponses } = await broker.deleteAcls({
          filters: filters.map((filter) => ({
            resourceType: filter.resourceType,
            resourceName: filter.resourceName ?? null,
            resourcePatternType: filter.resourcePatternType,
            principal: filter.principal ?? null,
            host: filter.host ?? null,
            operation: filter.operation,
            permissionType: filter.permissionType,
          })),
        });
        return { filterResponses };
      } catch (error) {
        if (protocolType(error) === 'NOT_CONTROLLER') {
          logger.warn('Could not delete ACL', {
            error: error instanceof Error ? error.message : String(error),
            retryCount,
            retryTime,
          });
          throw error;
        }

        bail(error as Error);
        return { filterResponses: [] };
      }
    });
  };

  return { createAcls, describeAcls, deleteAcls };
}
