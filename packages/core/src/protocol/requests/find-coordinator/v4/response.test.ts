import { describe, expect, it } from 'vitest';
import { Encoder } from '../../../encoder';
import { findCoordinatorResponseV4 } from './response';

interface WireCoordinator {
  key: string;
  nodeId: number;
  errorCode?: number;
  errorMessage?: string | null;
}

function encodeV4Response(options: { throttleTime: number; coordinators: WireCoordinator[] }): Buffer {
  const encoder = new Encoder().writeInt32(options.throttleTime).writeUVarInt(options.coordinators.length + 1);
  for (const { key, nodeId, errorCode = 0, errorMessage = null } of options.coordinators) {
    encoder
      .writeUVarIntString(key)
      .writeInt32(nodeId)
      .writeUVarIntString(`broker-${nodeId}`)
      .writeInt32(9092)
      .writeInt16(errorCode)
      .writeUVarIntString(errorMessage)
      .writeUVarInt(0);
  }
  return encoder.writeUVarInt(0).buffer;
}

describe('protocol/requests/find-coordinator/v4/response', () => {
  it('decodes one coordinator per key and remaps throttleTime', async () => {
    const data = await findCoordinatorResponseV4.decode(
      encodeV4Response({
        throttleTime: 8,
        coordinators: [
          { key: 'group-a', nodeId: 1 },
          { key: 'group-b', nodeId: 2 },
        ],
      }),
    );

    expect(data).toEqual({
      throttleTime: 0,
      clientSideThrottleTime: 8,
      coordinators: [
        { key: 'group-a', nodeId: 1, host: 'broker-1', port: 9092, errorCode: 0, errorMessage: null },
        { key: 'group-b', nodeId: 2, host: 'broker-2', port: 9092, errorCode: 0, errorMessage: null },
      ],
    });
    await expect(findCoordinatorResponseV4.parse(data)).resolves.toBe(data);
  });

  it('fails the lookup when any key has an error', async () => {
    const data = await findCoordinatorResponseV4.decode(
      encodeV4Response({
        throttleTime: 0,
        coordinators: [
          { key: 'group-a', nodeId: 1 },
          { key: 'group-b', nodeId: -1, errorCode: 15, errorMessage: 'not available' },
        ],
      }),
    );

    await expect(findCoordinatorResponseV4.parse(data)).rejects.toMatchObject({
      type: 'GROUP_COORDINATOR_NOT_AVAILABLE',
    });
  });
});
