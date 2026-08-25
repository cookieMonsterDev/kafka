import { describe, expect, it } from 'vitest';
import { Encoder } from '../../../encoder';
import { assignReplicasToDirsResponseV0, responseSchema } from './response';

describe('protocol/requests/assign-replicas-to-dirs/v0/response', () => {
  const ok = {
    throttleTime: 4,
    errorCode: 0,
    directories: [
      {
        id: Buffer.alloc(16, 3),
        topics: [
          {
            topicId: Buffer.alloc(16, 4),
            partitions: [{ partitionIndex: 0, errorCode: 0 }],
          },
        ],
      },
    ],
  };

  it('round-trips a flexible v0 response and remaps throttleTime', async () => {
    const encoder = new Encoder();
    responseSchema.write(encoder, ok);
    const data = await assignReplicasToDirsResponseV0.decode(encoder.buffer);
    expect(data).toEqual({
      ...ok,
      throttleTime: 0,
      clientSideThrottleTime: 4,
    });
    await expect(assignReplicasToDirsResponseV0.parse(data)).resolves.toEqual(data);
  });

  it('throws on a top-level error code', async () => {
    await expect(
      assignReplicasToDirsResponseV0.parse({
        throttleTime: 0,
        clientSideThrottleTime: 0,
        errorCode: 41,
        directories: [],
      }),
    ).rejects.toMatchObject({ type: 'NOT_CONTROLLER' });
  });

  it('throws on a nested partition error code', async () => {
    await expect(
      assignReplicasToDirsResponseV0.parse({
        throttleTime: 0,
        clientSideThrottleTime: 0,
        errorCode: 0,
        directories: [
          {
            id: Buffer.alloc(16, 3),
            topics: [
              {
                topicId: Buffer.alloc(16, 4),
                partitions: [{ partitionIndex: 0, errorCode: 3 }],
              },
            ],
          },
        ],
      }),
    ).rejects.toMatchObject({ type: 'UNKNOWN_TOPIC_OR_PARTITION' });
  });
});
