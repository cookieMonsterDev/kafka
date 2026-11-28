import { describe, expect, it } from 'vitest';
import { Decoder } from '../../decoder';
import { COORDINATOR_TYPES } from '../../enums/coordinator-types';
import { FindCoordinator, type FindCoordinatorOptions } from './index';
import { requestSchema as requestSchemaV3 } from './v3/request';
import { requestSchema as requestSchemaV4 } from './v4/request';

async function encoded(version: number, options: FindCoordinatorOptions): Promise<Buffer> {
  const { request } = FindCoordinator.protocol({ version })(options);
  return (await request.encode()).buffer;
}

async function parsed(version: number, options: FindCoordinatorOptions, body: unknown): Promise<unknown> {
  const { response } = FindCoordinator.protocol({ version })(options);
  return FindCoordinator.parseResponse?.(body, options, (versionBody) => response.parse(versionBody));
}

describe('protocol/requests/find-coordinator', () => {
  it('implements versions 0 through 5', () => {
    expect(FindCoordinator.versions).toEqual([0, 1, 2, 3, 4, 5]);
  });

  it('sends the coordinator type with every version from 1', async () => {
    const v3 = await encoded(3, { coordinatorKeys: ['g'], coordinatorType: COORDINATOR_TYPES.GROUP });
    expect(requestSchemaV3.read(new Decoder(v3))).toEqual({
      coordinatorKey: 'g',
      coordinatorType: COORDINATOR_TYPES.GROUP,
    });

    const v4 = await encoded(4, { coordinatorKeys: ['g', 'h'], coordinatorType: COORDINATOR_TYPES.GROUP });
    expect(requestSchemaV4.read(new Decoder(v4))).toEqual({
      coordinatorType: COORDINATOR_TYPES.GROUP,
      coordinatorKeys: ['g', 'h'],
    });
  });

  it('sends exactly one key before v4', () => {
    for (const version of [0, 1, 2, 3]) {
      expect(() =>
        FindCoordinator.protocol({ version })({
          coordinatorKeys: ['a', 'b'],
          coordinatorType: COORDINATOR_TYPES.GROUP,
        }),
      ).toThrow('FindCoordinator before v4 looks up exactly one key');
      expect(() =>
        FindCoordinator.protocol({ version })({ coordinatorKeys: [], coordinatorType: COORDINATOR_TYPES.GROUP }),
      ).toThrow('FindCoordinator before v4 looks up exactly one key');
    }
  });

  it('answers with the requested key before v4', async () => {
    const body = {
      throttleTime: 0,
      clientSideThrottleTime: 0,
      errorCode: 0,
      errorMessage: null,
      coordinator: { nodeId: 2, host: 'broker-2', port: 9092 },
    };

    await expect(
      parsed(3, { coordinatorKeys: ['tx'], coordinatorType: COORDINATOR_TYPES.TRANSACTION }, body),
    ).resolves.toEqual({
      coordinators: [{ key: 'tx', nodeId: 2, host: 'broker-2', port: 9092 }],
    });
  });

  it('answers with every key from v4, without the per-key error fields', async () => {
    const body = {
      throttleTime: 0,
      clientSideThrottleTime: 0,
      coordinators: [
        { key: 'a', nodeId: 1, host: 'broker-1', port: 9092, errorCode: 0, errorMessage: null },
        { key: 'b', nodeId: 2, host: 'broker-2', port: 9092, errorCode: 0, errorMessage: null },
      ],
    };

    await expect(
      parsed(4, { coordinatorKeys: ['a', 'b'], coordinatorType: COORDINATOR_TYPES.GROUP }, body),
    ).resolves.toEqual({
      coordinators: [
        { key: 'a', nodeId: 1, host: 'broker-1', port: 9092 },
        { key: 'b', nodeId: 2, host: 'broker-2', port: 9092 },
      ],
    });
  });
});
