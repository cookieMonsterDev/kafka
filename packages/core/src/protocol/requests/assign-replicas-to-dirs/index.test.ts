import { describe, expect, it } from 'vitest';
import { Decoder } from '../../decoder';
import { AssignReplicasToDirs } from './index';
import { requestSchema } from './v0/request';

describe('protocol/requests/assign-replicas-to-dirs', () => {
  it('registers version 0', () => {
    expect(AssignReplicasToDirs.versions).toEqual([0]);
  });

  it('defaults brokerEpoch to -1n', async () => {
    const { request } = AssignReplicasToDirs.protocol({ version: 0 })({
      brokerId: 1,
      directories: [],
    });
    expect(request.apiVersion).toBe(0);
    const encoded = await request.encode();
    expect(requestSchema.read(new Decoder(encoded.buffer))).toEqual({
      brokerId: 1,
      brokerEpoch: -1n,
      directories: [],
    });
  });
});
