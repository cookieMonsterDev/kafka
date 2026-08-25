import { describe, expect, it, vi } from 'vitest';
import type { Cluster } from '../cluster/index';
import { KafkaNonRetriableError } from '../errors';
import { createLogger, LOG_LEVELS } from '../loggers/index';
import { ACL_OPERATION_TYPES } from '../protocol/enums/acl-operation-types';
import type { AclOperationType } from '../protocol/enums/acl-operation-types';
import { ACL_PERMISSION_TYPES } from '../protocol/enums/acl-permission-types';
import type { AclPermissionType } from '../protocol/enums/acl-permission-types';
import { ACL_RESOURCE_TYPES } from '../protocol/enums/acl-resource-types';
import type { AclResourceType } from '../protocol/enums/acl-resource-types';
import { RESOURCE_PATTERN_TYPES } from '../protocol/enums/resource-pattern-types';
import type { ResourcePatternType } from '../protocol/enums/resource-pattern-types';
import { createAclsApi } from './acls';
import type { AclEntry, AclFilter } from './types';

const INVALID_OPERATION = 999 as unknown as AclOperationType;
const INVALID_PATTERN = 999 as unknown as ResourcePatternType;
const INVALID_PERMISSION = 999 as unknown as AclPermissionType;
const INVALID_RESOURCE = 999 as unknown as AclResourceType;

const silentLogger = createLogger({ level: LOG_LEVELS.NOTHING, logCreator: () => () => {} });

const validAcl: AclEntry = {
  principal: 'User:alice',
  host: '*',
  operation: ACL_OPERATION_TYPES.READ,
  permissionType: ACL_PERMISSION_TYPES.ALLOW,
  resourceType: ACL_RESOURCE_TYPES.TOPIC,
  resourceName: 'orders',
  resourcePatternType: RESOURCE_PATTERN_TYPES.LITERAL,
};

const validFilter: AclFilter = {
  resourceType: ACL_RESOURCE_TYPES.TOPIC,
  resourcePatternType: RESOURCE_PATTERN_TYPES.LITERAL,
  operation: ACL_OPERATION_TYPES.READ,
  permissionType: ACL_PERMISSION_TYPES.ALLOW,
};

function fakeCluster(broker: Record<string, ReturnType<typeof vi.fn>> = {}) {
  return {
    refreshMetadata: vi.fn().mockResolvedValue(undefined),
    findControllerBroker: vi.fn().mockResolvedValue(broker),
    broker,
  };
}

function makeApi(cluster: ReturnType<typeof fakeCluster>, retry?: { retries?: number }) {
  return createAclsApi({
    cluster: cluster as unknown as Cluster,
    logger: silentLogger.namespace('Admin'),
    rootLogger: silentLogger,
    retry,
  });
}

describe('admin/acls', () => {
  describe('createAcls', () => {
    it('rejects a non-array acl', async () => {
      const cluster = fakeCluster();
      const api = makeApi(cluster);
      await expect(api.createAcls({ acl: undefined as unknown as AclEntry[] })).rejects.toThrow(KafkaNonRetriableError);
      expect(cluster.findControllerBroker).not.toHaveBeenCalled();
    });

    it('rejects an empty acl array', async () => {
      const api = makeApi(fakeCluster());
      await expect(api.createAcls({ acl: [] })).rejects.toThrow('Empty ACL array');
    });

    it('rejects an invalid principal', async () => {
      const api = makeApi(fakeCluster());
      await expect(api.createAcls({ acl: [{ ...validAcl, principal: 123 as unknown as string }] })).rejects.toThrow(
        'principals have to be a valid string',
      );
    });

    it('rejects an invalid host', async () => {
      const api = makeApi(fakeCluster());
      await expect(api.createAcls({ acl: [{ ...validAcl, host: 123 as unknown as string }] })).rejects.toThrow(
        'hosts have to be a valid string',
      );
    });

    it('rejects an invalid resourceName', async () => {
      const api = makeApi(fakeCluster());
      await expect(api.createAcls({ acl: [{ ...validAcl, resourceName: 123 as unknown as string }] })).rejects.toThrow(
        'resourceNames have to be a valid string',
      );
    });

    it('rejects an invalid operation type', async () => {
      const api = makeApi(fakeCluster());
      await expect(api.createAcls({ acl: [{ ...validAcl, operation: INVALID_OPERATION }] })).rejects.toThrow(
        'Invalid operation type',
      );
    });

    it('rejects an invalid resource pattern type', async () => {
      const api = makeApi(fakeCluster());
      await expect(api.createAcls({ acl: [{ ...validAcl, resourcePatternType: INVALID_PATTERN }] })).rejects.toThrow(
        'Invalid resource pattern type',
      );
    });

    it('rejects an invalid permission type', async () => {
      const api = makeApi(fakeCluster());
      await expect(api.createAcls({ acl: [{ ...validAcl, permissionType: INVALID_PERMISSION }] })).rejects.toThrow(
        'Invalid permission type',
      );
    });

    it('rejects an invalid resource type', async () => {
      const api = makeApi(fakeCluster());
      await expect(api.createAcls({ acl: [{ ...validAcl, resourceType: INVALID_RESOURCE }] })).rejects.toThrow(
        'Invalid resource type',
      );
    });

    it('creates ACLs through the active controller', async () => {
      const cluster = fakeCluster({ createAcls: vi.fn().mockResolvedValue(undefined) });
      const api = makeApi(cluster);

      await expect(api.createAcls({ acl: [validAcl] })).resolves.toBe(true);

      expect(cluster.refreshMetadata).toHaveBeenCalled();
      expect(cluster.findControllerBroker).toHaveBeenCalled();
      expect(cluster.broker.createAcls).toHaveBeenCalledWith({ creations: [validAcl] });
    });

    it('bails and resolves to false when the broker call fails for a non-retriable reason', async () => {
      const cluster = fakeCluster({
        createAcls: vi.fn().mockRejectedValue(new Error('boom')),
      });
      const api = makeApi(cluster);

      await expect(api.createAcls({ acl: [validAcl] })).rejects.toThrow('boom');
    });

    it('retries and eventually fails when the controller keeps returning NOT_CONTROLLER', async () => {
      const notController = Object.assign(new Error('not controller'), { type: 'NOT_CONTROLLER' });
      const cluster = fakeCluster({
        createAcls: vi.fn().mockRejectedValue(notController),
      });
      const api = makeApi(cluster, { retries: 0 });

      await expect(api.createAcls({ acl: [validAcl] })).rejects.toThrow();
      expect(cluster.broker.createAcls).toHaveBeenCalledTimes(1);
    });
  });

  describe('describeAcls', () => {
    it('rejects an invalid principal', async () => {
      const api = makeApi(fakeCluster());
      await expect(api.describeAcls({ ...validFilter, principal: 123 as unknown as string })).rejects.toThrow(
        'principal have to be a valid string',
      );
    });

    it('rejects an invalid host', async () => {
      const api = makeApi(fakeCluster());
      await expect(api.describeAcls({ ...validFilter, host: 123 as unknown as string })).rejects.toThrow(
        'host have to be a valid string',
      );
    });

    it('rejects an invalid resourceName', async () => {
      const api = makeApi(fakeCluster());
      await expect(api.describeAcls({ ...validFilter, resourceName: 123 as unknown as string })).rejects.toThrow(
        'resourceName have to be a valid string',
      );
    });

    it('rejects an invalid operation type', async () => {
      const api = makeApi(fakeCluster());
      await expect(api.describeAcls({ ...validFilter, operation: INVALID_OPERATION })).rejects.toThrow(
        'Invalid operation type',
      );
    });

    it('rejects an invalid resource pattern filter type', async () => {
      const api = makeApi(fakeCluster());
      await expect(api.describeAcls({ ...validFilter, resourcePatternType: INVALID_PATTERN })).rejects.toThrow(
        'Invalid resource pattern filter type',
      );
    });

    it('rejects an invalid permission type', async () => {
      const api = makeApi(fakeCluster());
      await expect(api.describeAcls({ ...validFilter, permissionType: INVALID_PERMISSION })).rejects.toThrow(
        'Invalid permission type',
      );
    });

    it('rejects an invalid resource type', async () => {
      const api = makeApi(fakeCluster());
      await expect(api.describeAcls({ ...validFilter, resourceType: INVALID_RESOURCE })).rejects.toThrow(
        'Invalid resource type',
      );
    });

    it('describes ACLs, defaulting optional filter fields to null', async () => {
      const cluster = fakeCluster({
        describeAcls: vi.fn().mockResolvedValue({ resources: [{ resourceName: 'orders' }] }),
      });
      const api = makeApi(cluster);

      await expect(api.describeAcls(validFilter)).resolves.toEqual({ resources: [{ resourceName: 'orders' }] });

      expect(cluster.broker.describeAcls).toHaveBeenCalledWith({
        resourceType: validFilter.resourceType,
        resourceName: null,
        resourcePatternType: validFilter.resourcePatternType,
        principal: null,
        host: null,
        operation: validFilter.operation,
        permissionType: validFilter.permissionType,
      });
    });

    it('forwards provided optional filter fields as-is', async () => {
      const cluster = fakeCluster({
        describeAcls: vi.fn().mockResolvedValue({ resources: [] }),
      });
      const api = makeApi(cluster);

      await api.describeAcls({ ...validFilter, principal: 'User:bob', host: '10.0.0.1', resourceName: 'orders' });

      expect(cluster.broker.describeAcls).toHaveBeenCalledWith(
        expect.objectContaining({ principal: 'User:bob', host: '10.0.0.1', resourceName: 'orders' }),
      );
    });

    it('bails and resolves to no resources when the broker call fails for a non-retriable reason', async () => {
      const cluster = fakeCluster({
        describeAcls: vi.fn().mockRejectedValue(new Error('boom')),
      });
      const api = makeApi(cluster);

      await expect(api.describeAcls(validFilter)).rejects.toThrow('boom');
    });

    it('retries when the controller returns NOT_CONTROLLER', async () => {
      const notController = Object.assign(new Error('not controller'), { type: 'NOT_CONTROLLER' });
      const cluster = fakeCluster({
        describeAcls: vi.fn().mockRejectedValue(notController),
      });
      const api = makeApi(cluster, { retries: 0 });

      await expect(api.describeAcls(validFilter)).rejects.toThrow();
      expect(cluster.broker.describeAcls).toHaveBeenCalledTimes(1);
    });
  });

  describe('deleteAcls', () => {
    it('rejects a non-array filters', async () => {
      const api = makeApi(fakeCluster());
      await expect(api.deleteAcls({ filters: undefined as unknown as AclFilter[] })).rejects.toThrow(
        KafkaNonRetriableError,
      );
    });

    it('rejects an empty filters array', async () => {
      const api = makeApi(fakeCluster());
      await expect(api.deleteAcls({ filters: [] })).rejects.toThrow('Empty ACL Filter array');
    });

    it('rejects an invalid principal', async () => {
      const api = makeApi(fakeCluster());
      await expect(
        api.deleteAcls({ filters: [{ ...validFilter, principal: 123 as unknown as string }] }),
      ).rejects.toThrow('principals have to be a valid string');
    });

    it('rejects an invalid host', async () => {
      const api = makeApi(fakeCluster());
      await expect(api.deleteAcls({ filters: [{ ...validFilter, host: 123 as unknown as string }] })).rejects.toThrow(
        'hosts have to be a valid string',
      );
    });

    it('rejects an invalid resourceName', async () => {
      const api = makeApi(fakeCluster());
      await expect(
        api.deleteAcls({ filters: [{ ...validFilter, resourceName: 123 as unknown as string }] }),
      ).rejects.toThrow('resourceNames have to be a valid string');
    });

    it('rejects an invalid operation type', async () => {
      const api = makeApi(fakeCluster());
      await expect(api.deleteAcls({ filters: [{ ...validFilter, operation: INVALID_OPERATION }] })).rejects.toThrow(
        'Invalid operation type',
      );
    });

    it('rejects an invalid resource pattern type', async () => {
      const api = makeApi(fakeCluster());
      await expect(
        api.deleteAcls({ filters: [{ ...validFilter, resourcePatternType: INVALID_PATTERN }] }),
      ).rejects.toThrow('Invalid resource pattern type');
    });

    it('rejects an invalid permission type', async () => {
      const api = makeApi(fakeCluster());
      await expect(
        api.deleteAcls({ filters: [{ ...validFilter, permissionType: INVALID_PERMISSION }] }),
      ).rejects.toThrow('Invalid permission type');
    });

    it('rejects an invalid resource type', async () => {
      const api = makeApi(fakeCluster());
      await expect(api.deleteAcls({ filters: [{ ...validFilter, resourceType: INVALID_RESOURCE }] })).rejects.toThrow(
        'Invalid resource type',
      );
    });

    it('deletes ACLs, defaulting optional filter fields to null', async () => {
      const cluster = fakeCluster({
        deleteAcls: vi.fn().mockResolvedValue({ filterResponses: [{ errorCode: 0 }] }),
      });
      const api = makeApi(cluster);

      await expect(api.deleteAcls({ filters: [validFilter] })).resolves.toEqual({
        filterResponses: [{ errorCode: 0 }],
      });

      expect(cluster.broker.deleteAcls).toHaveBeenCalledWith({
        filters: [
          {
            resourceType: validFilter.resourceType,
            resourceName: null,
            resourcePatternType: validFilter.resourcePatternType,
            principal: null,
            host: null,
            operation: validFilter.operation,
            permissionType: validFilter.permissionType,
          },
        ],
      });
    });

    it('forwards provided optional filter fields as-is across multiple filters', async () => {
      const cluster = fakeCluster({
        deleteAcls: vi.fn().mockResolvedValue({ filterResponses: [] }),
      });
      const api = makeApi(cluster);

      await api.deleteAcls({
        filters: [{ ...validFilter, principal: 'User:bob', host: '10.0.0.1', resourceName: 'orders' }, validFilter],
      });

      expect(cluster.broker.deleteAcls).toHaveBeenCalledWith({
        filters: [
          expect.objectContaining({ principal: 'User:bob', host: '10.0.0.1', resourceName: 'orders' }),
          expect.objectContaining({ principal: null, host: null, resourceName: null }),
        ],
      });
    });

    it('bails and resolves to no filter responses when the broker call fails for a non-retriable reason', async () => {
      const cluster = fakeCluster({
        deleteAcls: vi.fn().mockRejectedValue(new Error('boom')),
      });
      const api = makeApi(cluster);

      await expect(api.deleteAcls({ filters: [validFilter] })).rejects.toThrow('boom');
    });

    it('retries when the controller returns NOT_CONTROLLER', async () => {
      const notController = Object.assign(new Error('not controller'), { type: 'NOT_CONTROLLER' });
      const cluster = fakeCluster({
        deleteAcls: vi.fn().mockRejectedValue(notController),
      });
      const api = makeApi(cluster, { retries: 0 });

      await expect(api.deleteAcls({ filters: [validFilter] })).rejects.toThrow();
      expect(cluster.broker.deleteAcls).toHaveBeenCalledTimes(1);
    });
  });
});
