import { describe, expect, it } from 'vitest';
import { Encoder } from '../../../encoder';
import { responseSchema } from '../v0/response';
import { alterReplicaLogDirsResponseV1 } from './response';

describe('protocol/requests/alter-replica-log-dirs/v1/response', () => {
  it('decodes the v0 wire format, remapping throttleTime to clientSideThrottleTime', async () => {
    const wire = {
      throttleTime: 50,
      results: [{ topic: 'orders', partitions: [{ partition: 0, errorCode: 0 }] }],
    };

    const encoder = new Encoder();
    responseSchema.write(encoder, wire);
    const data = await alterReplicaLogDirsResponseV1.decode(encoder.buffer);
    expect(data.throttleTime).toBe(0);
    expect(data.clientSideThrottleTime).toBe(50);
    expect(data.results).toHaveLength(1);
    await expect(alterReplicaLogDirsResponseV1.parse(data)).resolves.toEqual(data);
  });
});
