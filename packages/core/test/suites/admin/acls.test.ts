import { afterEach, describe, expect, it } from 'vitest';
import { createAdmin } from '../../../src/admin/index.js';
import { createProducer } from '../../../src/producer/index.js';
import { ACL_OPERATION_TYPES } from '../../../src/protocol/enums/acl-operation-types.js';
import { ACL_PERMISSION_TYPES } from '../../../src/protocol/enums/acl-permission-types.js';
import { ACL_RESOURCE_TYPES } from '../../../src/protocol/enums/acl-resource-types.js';
import { RESOURCE_PATTERN_TYPES } from '../../../src/protocol/enums/resource-pattern-types.js';
import {
  createCluster,
  newLogger,
  saslBrokers,
  saslConnectionOpts,
  secureRandom,
  waitFor,
} from '../../helpers/index.js';

function saslClients() {
  const opts = { ...saslConnectionOpts(), metadataMaxAge: 50 };
  const admin = createAdmin({ cluster: createCluster(opts, saslBrokers()), logger: newLogger() });
  const producer = createProducer({ cluster: createCluster(opts, saslBrokers()), logger: newLogger() });
  return { admin, producer };
}

describe('admin.acls', () => {
  let admin: ReturnType<typeof createAdmin> | undefined;
  let producer: ReturnType<typeof createProducer> | undefined;

  afterEach(async () => {
    await producer?.disconnect();
    await admin?.disconnect();
  });

  it('creates, describes, and deletes topic ACLs', async () => {
    const topicName = `test-topic-${secureRandom()}`;
    ({ admin, producer } = saslClients());
    await admin.connect();
    await admin.createTopics({ waitForLeaders: true, topics: [{ topic: topicName }] });

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
