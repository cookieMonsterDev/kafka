import { afterEach, describe, expect } from 'vitest';
import { createAdmin } from '../../../src/admin/index';
import { SCRAM_MECHANISMS } from '../../../src/protocol/enums/scram-mechanisms';
import { createCluster, newLogger, secureRandom, testIfKafkaAtLeast_3_0, waitFor } from '../../helpers/index';

describe('admin.scram', () => {
  let admin: ReturnType<typeof createAdmin> | undefined;
  let userName: string;

  afterEach(async () => {
    if (admin && userName) {
      await admin
        .alterUserScramCredentials({
          deletions: [{ name: userName, mechanism: SCRAM_MECHANISMS.SCRAM_SHA_256 }],
        })
        .catch(() => undefined);
    }
    await admin?.disconnect();
  });

  testIfKafkaAtLeast_3_0('creates, describes, and deletes a SCRAM user', async () => {
    userName = `scram-user-${secureRandom()}`;
    admin = createAdmin({ cluster: createCluster(), logger: newLogger() });
    await admin.connect();

    const upserted = await admin.alterUserScramCredentials({
      upsertions: [{ name: userName, mechanism: SCRAM_MECHANISMS.SCRAM_SHA_256, password: 'test-password-256' }],
    });
    expect(upserted.results).toEqual(
      expect.arrayContaining([expect.objectContaining({ user: userName, errorCode: 0 })]),
    );

    const described = await waitFor(async () => {
      try {
        const result = await admin!.describeUserScramCredentials({ users: [userName] });
        return result.results.find((entry) => entry.user === userName && entry.errorCode === 0) ?? false;
      } catch (error) {
        if ((error as { type?: string }).type === 'RESOURCE_NOT_FOUND') return false;
        throw error;
      }
    });
    expect(described).toEqual(
      expect.objectContaining({
        user: userName,
        errorCode: 0,
        credentialInfos: expect.arrayContaining([
          expect.objectContaining({ mechanism: SCRAM_MECHANISMS.SCRAM_SHA_256, iterations: 4096 }),
        ]),
      }),
    );

    const deleted = await admin.alterUserScramCredentials({
      deletions: [{ name: userName, mechanism: SCRAM_MECHANISMS.SCRAM_SHA_256 }],
    });
    expect(deleted.results).toEqual(
      expect.arrayContaining([expect.objectContaining({ user: userName, errorCode: 0 })]),
    );
    userName = '';
  });
});
