import { describe, expect, it } from 'vitest';
import { Decoder } from '../../../decoder';
import { requestSchema } from '../v0/request';
import { alterReplicaLogDirsRequestV1 } from './request';

describe('protocol/requests/alter-replica-log-dirs/v1/request', () => {
  it('round-trips a v1 request (wire format identical to v0)', async () => {
    const value = {
      dirs: [{ path: '/var/kafka/data-1', topics: [{ topic: 'orders', partitions: [0, 1] }] }],
    };

    const encoder = await alterReplicaLogDirsRequestV1(value).encode();
    expect(alterReplicaLogDirsRequestV1(value).apiVersion).toBe(1);
    expect(requestSchema.read(new Decoder(encoder.buffer))).toEqual(value);
  });
});
