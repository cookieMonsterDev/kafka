import { describe, expect, it } from 'vitest';
import { KafkaProtocolError } from '../../../../errors';
import { Encoder } from '../../../encoder';
import { alterReplicaLogDirsResponseV2, responseSchema } from './response';

describe('protocol/requests/alter-replica-log-dirs/v2/response', () => {
  it('round-trips a flexible v2 response', async () => {
    const value = {
      throttleTime: 0,
      results: [
        {
          topic: 'orders',
          partitions: [
            { partition: 0, errorCode: 0 },
            { partition: 1, errorCode: 0 },
          ],
        },
      ],
    };

    const encoder = new Encoder();
    responseSchema.write(encoder, value);
    const data = await alterReplicaLogDirsResponseV2.decode(encoder.buffer);
    expect(data).toEqual(value);
    await expect(alterReplicaLogDirsResponseV2.parse(data)).resolves.toEqual(data);
  });

  it('throws the first partition error from parse', async () => {
    const value = {
      throttleTime: 0,
      results: [{ topic: 'orders', partitions: [{ partition: 0, errorCode: 5 }] }],
    };

    const encoder = new Encoder();
    responseSchema.write(encoder, value);
    const data = await alterReplicaLogDirsResponseV2.decode(encoder.buffer);
    await expect(alterReplicaLogDirsResponseV2.parse(data)).rejects.toBeInstanceOf(KafkaProtocolError);
  });
});
