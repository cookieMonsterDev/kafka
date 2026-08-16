import { afterEach, describe, expect } from 'vitest';
import { createAdmin } from '../../../src/admin/index';
import { ACL_OPERATION_TYPES } from '../../../src/protocol/enums/acl-operation-types';
import { ACL_PERMISSION_TYPES } from '../../../src/protocol/enums/acl-permission-types';
import { ACL_RESOURCE_TYPES } from '../../../src/protocol/enums/acl-resource-types';
import { RESOURCE_PATTERN_TYPES } from '../../../src/protocol/enums/resource-pattern-types';
import {
  createCluster,
  newLogger,
  saslBrokers,
  saslEntries,
  secureRandom,
  testIfKafkaAtLeast_0_11,
  waitFor,
} from '../../helpers/index';

describe('admin.acls', () => {
  let admin: ReturnType<typeof createAdmin> | undefined;

  afterEach(async () => {
    await admin?.disconnect();
  });

  testIfKafkaAtLeast_0_11('creates, describes, and deletes topic ACLs', async () => {
    const topicName = `test-topic-${secureRandom()}`;
    admin = createAdmin({
      cluster: createCluster(saslEntries[0]!.opts(), saslBrokers()),
      logger: newLogger(),
    });
    await admin.connect();
    await admin.createTopics({
      waitForLeaders: true,
      topics: [{ topic: topicName, numPartitions: 1, replicationFactor: 1 }],
    });

    const acl = {
      resourceType: ACL_RESOURCE_TYPES.TOPIC,
      resourceName: topicName,
      resourcePatternType: RESOURCE_PATTERN_TYPES.LITERAL,
      principal: 'User:bob',
      host: '*',
      operation: ACL_OPERATION_TYPES.ALL,
      permissionType: ACL_PERMISSION_TYPES.DENY,
    };

    await expect(admin.createAcls({ acl: [acl] })).resolves.toBe(true);

    const described = await waitFor(async () => {
      const result = await admin!.describeAcls({
        resourceType: ACL_RESOURCE_TYPES.TOPIC,
        resourceName: topicName,
        resourcePatternType: RESOURCE_PATTERN_TYPES.LITERAL,
        principal: 'User:bob',
        host: '*',
        operation: ACL_OPERATION_TYPES.ALL,
        permissionType: ACL_PERMISSION_TYPES.DENY,
      });
      return result.resources.length > 0 ? result : false;
    });
    expect(described.resources.length).toBeGreaterThan(0);

    await admin.deleteAcls({
      filters: [
        {
          resourceType: ACL_RESOURCE_TYPES.TOPIC,
          resourceName: topicName,
          resourcePatternType: RESOURCE_PATTERN_TYPES.LITERAL,
          principal: 'User:bob',
          host: '*',
          operation: ACL_OPERATION_TYPES.ANY,
          permissionType: ACL_PERMISSION_TYPES.ANY,
        },
      ],
    });
  });
});
